#!/usr/bin/python
# coding=UTF-8

# Выводит список активных кусков - кусков, не покрытых никаким другим куском.
# Использование: `ls /opt/clickhouse/data/merge/visits | active_parts.py`
# TODO: O(N^2) -> O(N log N)

import sys
import re

parts = {}
for s in sys.stdin.read().split():
	m = re.match('^([0-9]{6})[0-9]{2}_([0-9]{6})[0-9]{2}_([0-9]+)_([0-9]+)_([0-9]+)$', s)
	if m == None:
		continue
	m1 = m.group(1)
	m2 = m.group(2)
	i1 = int(m.group(3))
	i2 = int(m.group(4))
	l = int(m.group(5))
	if m1 != m2:
		raise Exception('not in single month: ' + s)
	if m1 not in parts:
		parts[m1] = []
	parts[m1].append((i1, i2, l, s))

for m in parts:
	for x1, y1, l1, s1 in parts[m]:
		contained = False
		for x2, y2, l2, s2 in parts[m]:
			if x1 >= x2 and y1 <= y2 and l1 < l2: # 2 contains 1
				contained = True
				continue
			if x1 <= x2 and y1 >= y2 and l1 > l2: # 1 contains 2
				continue
			if x1 == x1 and y1 == y2 and l1 == l2: # 1 and 2 coincide
				continue
			if x1 > y2: # 1 is to the right of 2
				continue
			if x2 > y1: # 2 is to the right of 1
				continue
			raise Exception('invalid parts intersection: ' + s1 + ' and ' + s2)
		if not contained:
			print s1
