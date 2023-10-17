#!/usr/bin/python
# coding=UTF-8

# Displays a list of active parts - parts that are not overlapped by any other part.
# Usage: `ls /var/lib/clickhouse/data/merge/visits | active_parts.py`

import sys
import re

parts = {}
for s in sys.stdin.read().split():
    m = re.match(
        "^([0-9]{6})[0-9]{2}_([0-9]{6})[0-9]{2}_([0-9]+)_([0-9]+)_([0-9]+)$", s
    )
    if m == None:
        continue
    m1 = m.group(1)
    m2 = m.group(2)
    i1 = int(m.group(3))
    i2 = int(m.group(4))
    l = int(m.group(5))
    if m1 != m2:
        raise Exception("not in single month: " + s)
    if m1 not in parts:
        parts[m1] = []
    parts[m1].append((i1, i2, l, s))

for m, ps in sorted(parts.items()):
    ps.sort(key=lambda i1_i2_l_s: (i1_i2_l_s[0], -i1_i2_l_s[1], -i1_i2_l_s[2]))
    (x2, y2, l2, s2) = (-1, -1, -1, -1)
    for x1, y1, l1, s1 in ps:
        if x1 >= x2 and y1 <= y2 and l1 < l2 and (x1, y1) != (x2, y2):  # 2 contains 1
            pass
        elif x1 > y2:  # 1 is to the right of 2
            if x1 != y2 + 1 and y2 != -1:
                print()  # to see the missing numbers
            (x2, y2, l2, s2) = (x1, y1, l1, s1)
            print(s1)
        else:
            raise Exception("invalid parts intersection: " + s1 + " and " + s2)
    print()
