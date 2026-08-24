SELECT uniq(x) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniq(x) FROM (SELECT arrayJoin([[[]], [['a', 'b']], [['a'], ['b']], [['a', 'b']]]) AS x);
SELECT uniq(x, x) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniq(x, arrayMap(elem -> [elem, elem], x)) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniq(x, toString(x)) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniq((x, x)) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniq((x, arrayMap(elem -> [elem, elem], x))) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniq((x, toString(x))) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniq(x) FROM (SELECT arrayJoin([[], ['a'], ['a', NULL, 'b'], []]) AS x);

SELECT uniqExact(x) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqExact(x) FROM (SELECT arrayJoin([[[]], [['a', 'b']], [['a'], ['b']], [['a', 'b']]]) AS x);
SELECT uniqExact(x, x) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqExact(x, arrayMap(elem -> [elem, elem], x)) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqExact(x, toString(x)) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqExact((x, x)) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqExact((x, arrayMap(elem -> [elem, elem], x))) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqExact((x, toString(x))) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqExact(x) FROM (SELECT arrayJoin([[], ['a'], ['a', NULL, 'b'], []]) AS x);

SELECT uniqUpTo(3)(x) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqUpTo(3)(x) FROM (SELECT arrayJoin([[[]], [['a', 'b']], [['a'], ['b']], [['a', 'b']]]) AS x);
SELECT uniqUpTo(3)(x, x) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqUpTo(3)(x, arrayMap(elem -> [elem, elem], x)) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqUpTo(3)(x, toString(x)) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqUpTo(3)((x, x)) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqUpTo(3)((x, arrayMap(elem -> [elem, elem], x))) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqUpTo(3)((x, toString(x))) FROM (SELECT arrayJoin([[], ['a'], ['a', 'b'], []]) AS x);
SELECT uniqUpTo(3)(x) FROM (SELECT arrayJoin([[], ['a'], ['a', NULL, 'b'], []]) AS x);
