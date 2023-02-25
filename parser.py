import csv
import gzip
import struct
from datetime import datetime

def nanos(time_string):
    temp = struct.pack('>2s6s','\x00\x00'.encode('ASCII'),time_string)
    return struct.unpack('>Q', temp)[0]

#System Event Message
def system_event(msg):
    msg_list = ['S']
    msg_list += list(struct.unpack('>HH6sc',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Stock Directory
def stock_directory(msg):
    msg_list = ['R']
    msg_list += list(struct.unpack('>HH6s8sccIcc2scccccIc',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Stock Trading Action
def stock_trading_action(msg):
    msg_list = ['H']
    msg_list += list(struct.unpack('>HH6s8scc4s',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Reg SHO Restriction
def reg_sho_restriction(msg):
    msg_list = ['Y']
    msg_list += list(struct.unpack('>HH6s8sc',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Market Participant Position
def market_participant_position(msg):
    msg_list = ['L']
    msg_list += list(struct.unpack('>HH6s4s8sccc',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Market Wide Circuit Breaker (MWCB)
# MWCB Decline
def mwcb_decline(msg):
    msg_list = ['V']
    msg_list += list(struct.unpack('>HH6sQQQ',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

def mwcb_status(msg):
    msg_list = ['W']
    msg_list += list(struct.unpack('>HH6sc',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#IPO Quoting Period Update
def ipo_quoting_period(msg):
    msg_list = ['K']
    msg_list += list(struct.unpack('>HH6sIcQ',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Limit Up/Down Auction collar
def limit_auction_collar(msg):
    msg_list = ['J']
    msg_list += list(struct.unpack('>HH6s8sQQQ',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Operational Halt
def operational_halt(msg):
    msg_list = ['h']
    msg_list += list(struct.unpack('>HH6s8scc',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Add Order, No MPID
def add_order_no_mpid(msg):
    msg_list = ['A']
    msg_list += list(struct.unpack('>HH6sQcI8sI',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Add Order, with MPID
def add_order_with_mpid(msg):
    msg_list = ['F']
    msg_list += list(struct.unpack('>HH6sQcI8sI4s',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Order Executed, no price
def order_executed(msg):
    msg_list = ['E']
    msg_list += list(struct.unpack('>HH6sQIQ',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Order Executed, with price
def order_executed_with_price(msg):
    msg_list = ['C']
    msg_list += list(struct.unpack('>HH6sQIQcI', msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Order Cancel Message
def order_cancel(msg):
    msg_list = ['X']
    msg_list += list(struct.unpack('>HH6sQI',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Order Delete Message
def order_delete(msg):
    msg_list = ['D']
    msg_list += list(struct.unpack('>HH6sQ',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Order Replace Message
def order_replace(msg):
    msg_list = ['U']
    msg_list += list(struct.unpack('>HH6sQQII',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Trade Message (non-cross)
def trade_non_cross(msg):
    msg_list = ['P']
    msg_list += list(struct.unpack('>HH6sQcI8sIQ',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Trade Message (cross)
def trade_cross(msg):
    msg_list = ['Q']
    msg_list += list(struct.unpack('>HH6sQ8sIQc',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Broken Trade
def broken_trade(msg):
    msg_list = ['B']
    msg_list += list(struct.unpack('>HH6sQ',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Net Order Imbalance Indicator (NOII)
def noii(msg):
    msg_list = ['i']
    msg_list += list(struct.unpack('>HH6sQQc8sIIIcc',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list

#Retail Price Improvement Indicator (RPII)
def rpii(msg):
    msg_list = ['N']
    msg_list += list(struct.unpack('>HH6s8sc',msg))
    msg_list[3] = nanos(msg_list[3])
    msg_list = [i.decode('ASCII') if type(i) is bytes else i for i in msg_list]
    return msg_list


bin_data = gzip.open('c:\\Users\\Hurle\\Documents\\NASDAQ\\07302019.NASDAQ_ITCH50.gz', 'rb')
msg_header = bin_data.read(1)
writer = csv.writer(open('parsed.csv', 'w', newline=''))
while msg_header and datetime.now() < datetime(2023, 2, 24, 22, 21, 30):

    if msg_header == 'S'.encode('ASCII'):
        message = bin_data.read(11)
        message = system_event(message)
        writer.writerow(message)

    elif msg_header == 'R'.encode('ASCII'):
        message = bin_data.read(38)
        message = stock_directory(message)
        writer.writerow(message)

    elif msg_header == 'H'.encode('ASCII'):
        message = bin_data.read(24)
        message = stock_trading_action(message)
        writer.writerow(message)

    elif msg_header == 'Y'.encode('ASCII'):
        message = bin_data.read(19)
        message = reg_sho_restriction(message)
        writer.writerow(message)

    elif msg_header == 'L'.encode('ASCII'):
        message = bin_data.read(25)
        message = market_participant_position(message)
        writer.writerow(message)
    elif msg_header == 'V'.encode('ASCII'):
        message = bin_data.read(34)
        message = mwcb_decline(message)
        writer.writerow(message)

    elif msg_header == 'W'.encode('ASCII'):
        message = bin_data.read(11)
        message = mwcb_status(message)
        writer.writerow(message)

    elif msg_header == 'K'.encode('ASCII'):
        message = bin_data.read(27)
        message = ipo_quoting_period(message)
        writer.writerow(message)

    elif msg_header == 'J'.encode('ASCII'):
        message = bin_data.read(34)
        message = limit_auction_collar(message)
        writer.writerow(message)

    elif msg_header == 'h'.encode('ASCII'):
        message = bin_data.read(20)
        message = operational_halt(message)
        writer.writerow(message)

    elif msg_header == 'A'.encode('ASCII'):
        message = bin_data.read(35)
        message = add_order_no_mpid(message)
        writer.writerow(message)

    elif msg_header == 'F'.encode('ASCII'):
        message = bin_data.read(39)
        message = add_order_with_mpid(message)
        writer.writerow(message)

    elif msg_header == 'E'.encode('ASCII'):
        message = bin_data.read(30)
        message = order_executed(message)
        writer.writerow(message)

    elif msg_header == 'C'.encode('ASCII'):
        message = bin_data.read(35)
        message = order_executed_with_price(message)
        writer.writerow(message)

    elif msg_header == 'X'.encode('ASCII'):
        message = bin_data.read(22)
        message = order_cancel(message)
        writer.writerow(message)

    elif msg_header == 'D'.encode('ASCII'):
        message = bin_data.read(18)
        message = order_delete(message)
        writer.writerow(message)

    elif msg_header == 'U'.encode('ASCII'):
        message = bin_data.read(34)
        message = order_replace(message)
        writer.writerow(message)

    elif msg_header == 'P'.encode('ASCII'):
        message = bin_data.read(43)
        message = trade_non_cross(message)
        writer.writerow(message)

    elif msg_header == 'Q'.encode('ASCII'):
        message = bin_data.read(39)
        message = trade_cross(message)
        writer.writerow(message)

    elif msg_header == 'B'.encode('ASCII'):
        message = bin_data.read(18)
        message = broken_trade(message)
        writer.writerow(message)

    elif msg_header == 'I'.encode('ASCII'):
        message = bin_data.read(49)
        message = noii(message)
        writer.writerow(message)

    elif msg_header == 'N'.encode('ASCII'):
        message = bin_data.read(19)
        message = rpii(message)
        writer.writerow(message)

    msg_header = bin_data.read(1)

bin_data.close()