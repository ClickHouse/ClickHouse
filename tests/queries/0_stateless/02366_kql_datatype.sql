set allow_experimental_kusto_dialect=1;
set dialect = 'kusto';

print '-- bool'
print bool(true);
print bool(true);
print bool(null);
print '-- int';
print int(123);
print int(null);
print int('4'); -- { clientError BAD_ARGUMENTS }
print '-- long';
print long(123);
print long(0xff);
print long(-1);
print long(null);
print 456;
print '-- real';
print real(0.01);
print real(null);
print real(nan);
print real(+inf);
print real(-inf);
print double('4.2'); -- { clientError BAD_ARGUMENTS }
print '-- datetime';
print datetime(2015-12-31 23:59:59.9);
print datetime(2015-12-31);
print datetime('2014-05-25T08:20:03.123456');
print datetime('2014-11-08 15:55:55');
print datetime('2014-11-08 15:55');
print datetime('2014-11-08');
print datetime(null);
print datetime('2014-05-25T08:20:03.123456Z');
print datetime('2014-11-08 15:55:55.123456Z');
print '-- time';
print time('14.02:03:04.12345');
print time('12:30:55.123');
print time(1d);
print time(-1d);
print time(6nanoseconds);
print time(6tick);
print time(2);
print time(2) + 1d;
print '-- guid'
print guid(74be27de-1e4e-49d9-b579-fe0b331d3642);
print guid(null);
print '-- timespan (time)';
print timespan(2d); --              2 days
--print timespan(1.5h); -- 	        1.5 hour
print timespan(30m); -- 	        30 minutes
print timespan(10s); -- 	        10 seconds
--print timespan(0.1s); -- 	        0.1 second
print timespan(100ms); -- 	        100 millisecond
print timespan(10microsecond); -- 	10 microseconds
print timespan(1tick); --           100 nanoseconds
--print timespan(1.5h) / timespan(30m);
print timespan('12.23:12:23') / timespan(1s);
print '-- null';
print isnull(null);
print bool(null), int(null), long(null), real(null), double(null);
print '-- decimal';
print decimal(null);
print decimal(123.345);
print decimal(1e5);
print '-- dynamic'; -- no support for mixed types and bags for now
print dynamic(null);
print dynamic(1);
print dynamic(timespan(1d));
print dynamic([1,2,3]);
print dynamic([[1], [2], [3]]);
print dynamic(['a', "b", 'c']);
print '-- cast functions'
print '--tobool("true")'; -- == true
print tobool('true'); -- == true
print tobool('true') == toboolean('true'); -- == true
print '-- tobool("false")'; -- == false
print tobool('false'); -- == false
print tobool('false') == toboolean('false'); -- == false
print '-- tobool(1)'; -- == true
print tobool(1); -- == true
print tobool(1) == toboolean(1); -- == true
print '-- tobool(123)'; -- == true
print tobool(123); -- == true
print tobool(123) == toboolean(123); -- == true
print '-- tobool("abc")'; -- == null
print tobool('abc'); -- == null
print tobool('abc') == toboolean('abc'); -- == null
print '-- todouble()';
print todouble('123.4');
print todouble('abc') == null;
print '-- toreal()';
print toreal("123.4");
print toreal('abc') == null;
print '-- toint()';
print toint("123") == int(123);
print toint('abc');
print '-- tostring()';
print tostring(123);
print tostring(null) == '';
print '-- todatetime()';
print todatetime("2015-12-24") == datetime(2015-12-24);
print todatetime('abc') == null;
print '-- make_timespan()';
print v1=make_timespan(1,12), v2=make_timespan(1,12,30), v3=make_timespan(1,12,30,55.123);
print '-- totimespan()';
print totimespan(1tick);
print totimespan('0.00:01:00');
print totimespan('abc');
print totimespan('12.23:12:23') / totimespan(1s);
-- print totimespan(strcat('12.', '23', ':12:', '23')) / timespan(1s); -> 1120343
print '-- tolong()';
print tolong('123');
print tolong('abc');
print '-- todecimal()';
print todecimal(123.345);
print todecimal(null);
print todecimal('abc');
-- print todecimal(4 * 2 + 3); -> 11
