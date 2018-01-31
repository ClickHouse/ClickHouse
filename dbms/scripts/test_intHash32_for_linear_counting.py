#!/usr/bin/python3
import sys
import math
import statistics as stat

start = int(sys.argv[1])
end = int(sys.argv[2])

#Copied from dbms/src/Common/HashTable/Hash.h
def intHash32(key, salt = 0):
    key ^= salt;

    key = (~key) + (key << 18);
    key = key ^ ((key >> 31) | (key << 33));
    key = key * 21;
    key = key ^ ((key >> 11) | (key << 53));
    key = key + (key << 6);
    key = key ^ ((key >> 22) | (key << 42));

    return key & 0xffffffff

#Number of buckets for precision p = 12, m = 2^p
m = 4096
n = start
c = 0
m1 = {}
m2 = {}
l1 = []
l2 = []
while n <= end:
    c += 1

    h = intHash32(n)
    #Extract left most 12 bits
    x1 = (h >> 20) & 0xfff
    m1[x1] = 1
    z1 = m - len(m1)
    #Linear counting formula
    u1 = int(m * math.log(float(m) / float(z1)))
    e1 = abs(100*float(u1 - c)/float(c))
    l1.append(e1)
    print("%d %d %d %f" % (n, c, u1, e1))

    #Extract right most 12 bits
    x2 = h & 0xfff
    m2[x2] = 1
    z2 = m - len(m2)
    u2 = int(m * math.log(float(m) / float(z2)))
    e2 = abs(100*float(u2 - c)/float(c))
    l2.append(e2)
    print("%d %d %d %f" % (n, c, u2, e2))

    n += 1

print("Left 12 bits error: min=%f max=%f avg=%f median=%f median_low=%f median_high=%f" % (min(l1), max(l1), stat.mean(l1), stat.median(l1), stat.median_low(l1), stat.median_high(l1)))
print("Right 12 bits error: min=%f max=%f avg=%f median=%f median_low=%f median_high=%f" % (min(l2), max(l2), stat.mean(l2), stat.median(l2), stat.median_low(l2), stat.median_high(l2)))
