SELECT ('1',2) AS t, t.1, t.2;
SELECT materialize(('1',2)) AS t, t.1, t.2;
SELECT (materialize('1'),2) AS t, t.1, t.2;
SELECT ('1',materialize(2)) AS t, t.1, t.2;
SELECT (materialize('1'),materialize(2)) AS t, t.1, t.2;

SELECT [('1',2)] AS t, t[1].1, t[1].2;
SELECT [materialize(('1',2))] AS t, t[1].1, t[1].2;
SELECT [(materialize('1'),2)] AS t, t[1].1, t[1].2;
SELECT [('1',materialize(2))] AS t, t[1].1, t[1].2;
SELECT [(materialize('1'),materialize(2))] AS t, t[1].1, t[1].2;
SELECT materialize([('1',2)]) AS t, t[1].1, t[1].2;

SELECT [((1, materialize('2')), [(3, [4])])] AS thing,
    thing[1],
    thing[1].1,
    thing[1].2,
    thing[1].1.1,
    thing[1].1.2,
    (thing[1].2)[1],
    (thing[1].2)[1].1,
    (thing[1].2)[1].2,
    ((thing[1].2)[1].2)[1];

select arrayMap(t->tuple(t.1, t.2*2), [('1',2)]);
select arrayMap(t->tuple(t.1, t.2*2), [materialize(('1',2))]);
select arrayMap(t->tuple(t.1, t.2*2), [(materialize('1'),2)]);
select arrayMap(t->tuple(t.1, t.2*2), [('1',materialize(2))]);
select arrayMap(t->tuple(t.1, t.2*2), [(materialize('1'),materialize(2))]);
select arrayMap(t->tuple(t.1, t.2*2), materialize([('1',2)]));
