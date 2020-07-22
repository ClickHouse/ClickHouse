select formatRow('CSV', number, 'good') from numbers(3);
select formatRowNoNewline('TSV', number, DATE '2001-12-12', 1.4) from numbers(3);
select formatRow('JSONEachRow', number, toNullable(3), Null) from numbers(3);
select formatRowNoNewline('JSONEachRow', *) from numbers(3);

-- unknown format
select formatRow('aaa', *) from numbers(3); -- { serverError 73 }
