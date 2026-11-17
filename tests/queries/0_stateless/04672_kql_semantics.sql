-- KQL scalar semantics that differ from the ClickHouse function they most resemble.
-- Every expected value here is the one Microsoft's documentation gives for Kusto.

SET allow_experimental_kusto_dialect = 1;
SET dialect = 'kusto';

print '-- integer division truncates --';
print 7 / 2;                -- 3, not 3.5
print 7.0 / 2;              -- 3.5
print 7 / 2.0;              -- 3.5
print -7 / 2;               -- -3 (truncates toward zero)

print '-- substring indexes from 0, and a negative start counts from the end --';
print substring('abcdefg', 0, 2);
print substring('abcdefg', 1, 2);
print substring('abcdefg', -3, 2);      -- 'ef'
print substring('abcdefg', 2);          -- 'cdefg'

print '-- indexof is 0-based and reports -1 when absent --';
print indexof('abcdef', 'cd');          -- 2
print indexof('abcdef', 'zz');          -- -1
print indexof('abcdef', 'a');           -- 0

print '-- split takes the source first --';
print split('a,b,c', ',');
print split('a', ',');
print array_length(split('a', ','));    -- 1
print split('a,b,c', ',', 1);           -- 'b'

print '-- strcat renders every argument and treats null as empty --';
print strcat('a', 'b', 'c');
print strcat('a', 1, 'b');
print strcat('a', '', 'b');

print '-- bin --';
print bin(4.5, 1);          -- 4
print bin(4.5, 2);          -- 4
print bin(-4.5, 1);         -- -5 (rounds down, not toward zero)
print bin(17, 5);           -- 15

print '-- isempty / isnotempty treat null as empty --';
print isempty('');
print isempty('a');
print isnotempty('a');

print '-- conditionals --';
print iff(1 > 2, 'y', 'n');
print iif(1 < 2, 'y', 'n');
print case(1 > 2, 'a', 3 > 2, 'b', 'c');
print case(1 > 2, 'a', 3 > 4, 'b', 'c');

print '-- casts yield null on failure rather than raising --';
print toint('42');
print toint('abc');
print tolong('9223372036854775807');
print todouble('1.5');
print todecimal('1.5');
print todecimal('abc');

print '-- timespans are intervals, rendered in Kusto form --';
SET interval_output_format = 'kusto';
print 1d;
print 90m;
print 1500ms;
print 2.5h;
print 1tick;

print '-- timespan arithmetic on a datetime --';
print datetime(2020-01-01) + 1d;
print datetime(2020-01-01) - 12h;
-- Subtracting two datetimes yields a number of seconds here, where Kusto yields a
-- timespan. Producing an Interval would need the operand types, so it belongs in a
-- function rather than in the translation; see the guide.
print datetime(2020-01-02) - datetime(2020-01-01);

print '-- datetime parts --';
print getyear(datetime(2020-03-04));
print getmonth(datetime(2020-03-04));
print dayofmonth(datetime(2020-03-04));
print dayofweek(datetime(2020-03-01));   -- Sunday = 0
print startofday(datetime(2020-03-04 10:20:30));

print '-- arrays --';
print array_length(dynamic([1, 2, 3]));
print array_sum(dynamic([1, 2, 3]));
print array_index_of(dynamic([10, 20, 30]), 20);    -- 1, 0-based
print dynamic([10, 20, 30])[1];                      -- 20
print array_slice(dynamic([1, 2, 3, 4, 5]), 1, 3);

print '-- sort defaults to descending, nulls at the small end --';
datatable (N:long) [1, 3, 2] | sort by N | project N;
datatable (N:long) [1, 3, 2] | sort by N asc | project N;

SET dialect = 'clickhouse';
