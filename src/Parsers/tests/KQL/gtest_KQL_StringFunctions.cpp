#include <Parsers/tests/gtest_common.h>

#include <Parsers/Kusto/ParserKQLQuery.h>

INSTANTIATE_TEST_SUITE_P(ParserKQLQuery_String, ParserKQLTest,
    ::testing::Combine(
        ::testing::Values(std::make_shared<DB::ParserKQLQuery>()),
        ::testing::ValuesIn(std::initializer_list<ParserTestCase>{
        {
            "print base64_encode_fromguid(A)",
            "SELECT if(toTypeName(A) NOT IN ['UUID', 'Nullable(UUID)'], toString(throwIf(true, 'Expected guid as argument')), base64Encode(UUIDStringToNum(toString(A), 2)))"
        },
        {
            "print base64_decode_toguid(A)",
            "SELECT toUUIDOrNull(UUIDNumToString(toFixedString(base64Decode(A), 16), 2))"
        },
        {
            "print base64_decode_toarray('S3VzdG8=')",
            "SELECT arrayMap(x -> reinterpretAsUInt8(x), splitByRegexp('', base64Decode('S3VzdG8=')))"
        },
        {
            "print replace_regex('Hello, World!', '.', '\\0\\0')",
            "SELECT replaceRegexpAll('Hello, World!', '.', '\\0\\0')"
        },
        {
            "print idx = has_any_index('this is an example', dynamic(['this', 'example'])) ",
            "SELECT if(empty(['this', 'example']), -1, indexOf(arrayMap(x -> (x IN splitByChar(' ', 'this is an example')), if(empty(['this', 'example']), [''], arrayMap(x -> toString(x), ['this', 'example']))), 1) - 1) AS idx"
        },
        {
            "print idx = has_any_index('this is an example', dynamic([]))",
            "SELECT if(empty([]), -1, indexOf(arrayMap(x -> (x IN splitByChar(' ', 'this is an example')), if(empty([]), [''], arrayMap(x -> toString(x), []))), 1) - 1) AS idx"
        },
        {
            "print translate('krasp', 'otsku', 'spark')",
            "SELECT if(length('otsku') = 0, '', translate('spark', 'krasp', multiIf(length('otsku') = 0, 'krasp', (length('krasp') - length('otsku')) > 0, concat('otsku', repeat(substr('otsku', length('otsku'), 1), toUInt16(length('krasp') - length('otsku')))), (length('krasp') - length('otsku')) < 0, substr('otsku', 1, length('krasp')), 'otsku')))"
        },
        {
            "print trim_start('[^\\w]+', strcat('-  ','Te st1','// $'))",
            "SELECT replaceRegexpOne(concat('-  ', 'Te st1', '// $'), concat('^', '[^\\\\w]+'), '')"
        },
        {
            "print trim_end('.com', 'bing.com')",
            "SELECT replaceRegexpOne('bing.com', concat('.com', '$'), '')"
        },
        {
            "print trim('--', '--https://bing.com--')",
            "SELECT replaceRegexpOne(replaceRegexpOne('--https://bing.com--', concat('--', '$'), ''), concat('^', '--'), '')"
        },
        {
            "print bool(1)",
            "SELECT toBool(1)"
        },
        {
            "print datetime(2015-12-31 23:59:59.9)",
            "SELECT parseDateTime64BestEffortOrNull('2015-12-31 23:59:59.9', 9, 'UTC')"
        },
        {
            "print datetime(\"2015-12-31 23:59:59.9\")",
            "SELECT parseDateTime64BestEffortOrNull('2015-12-31 23:59:59.9', 9, 'UTC')"
        },
        {
            "print datetime('2015-12-31 23:59:59.9')",
            "SELECT parseDateTime64BestEffortOrNull('2015-12-31 23:59:59.9', 9, 'UTC')"
        },
        {
            "print guid(74be27de-1e4e-49d9-b579-fe0b331d3642)",
            "SELECT toUUIDOrNull('74be27de-1e4e-49d9-b579-fe0b331d3642')"
        },
        {
            "print guid('74be27de-1e4e-49d9-b579-fe0b331d3642')",
            "SELECT toUUIDOrNull('74be27de-1e4e-49d9-b579-fe0b331d3642')"
        },
        {
            "print guid('74be27de1e4e49d9b579fe0b331d3642')",
            "SELECT toUUIDOrNull('74be27de1e4e49d9b579fe0b331d3642')"
        },
        {
            "print int(32.5)",
            "SELECT toInt32(32.5)"
        },
        {
            "print long(32.5)",
            "SELECT toInt64(32.5)"
        },
        {
            "print real(32.5)",
            "SELECT toFloat64(32.5)"
        },
        {
            "print time('1.22:34:8.128')",
            "SELECT CAST('167648.128', 'Float64')"
        },
        {
            "print time('1d')",
            "SELECT CAST('86400', 'Float64')"
        },
        {
            "print time('1.5d')",
            "SELECT CAST('129600', 'Float64')"
        },
        {
            "print timespan('1.5d')",
            "SELECT CAST('129600', 'Float64')"
        },
        {
            "print res = bin_at(6.5, 2.5, 7)",
            "SELECT toFloat64(7) + (toInt64(((toFloat64(6.5) - toFloat64(7)) / 2.5) + -1) * 2.5) AS res"
        },
        {
            "print res = bin_at(1h, 1d, 12h)",
            "SELECT concat(toString(toInt32(((toFloat64(43200) + (toInt64(((toFloat64(3600) - toFloat64(43200)) / 86400) + -1) * 86400)) AS x) / 3600)), ':', toString(toInt32((x % 3600) / 60)), ':', toString(toInt32((x % 3600) % 60))) AS res"
        },
        {
            "print res = bin_at(datetime(2017-05-15 10:20:00.0), 1d, datetime(1970-01-01 12:00:00.0))",
            "SELECT toDateTime64(toFloat64(parseDateTime64BestEffortOrNull('1970-01-01 12:00:00.0', 9, 'UTC')) + (toInt64(((toFloat64(parseDateTime64BestEffortOrNull('2017-05-15 10:20:00.0', 9, 'UTC')) - toFloat64(parseDateTime64BestEffortOrNull('1970-01-01 12:00:00.0', 9, 'UTC'))) / 86400) + 0) * 86400), 9, 'UTC') AS res"
        },
        {
            "print bin(4.5, 1)",
            "SELECT toInt64(toFloat64(4.5) / 1) * 1"
        },
        {
            "print bin(4.5, -1)",
            "SELECT toInt64(toFloat64(4.5) / -1) * -1"
        },
        {
            "print bin(time(16d), 7d)",
            "SELECT concat(toString(toInt32(((toInt64(toFloat64(CAST('1382400', 'Float64')) / 604800) * 604800) AS x) / 3600)), ':', toString(toInt32((x % 3600) / 60)), ':', toString(toInt32((x % 3600) % 60)))"
        },
        {
            "print bin(datetime(1970-05-11 13:45:07), 1d)",
            "SELECT toDateTime64(toInt64(toFloat64(parseDateTime64BestEffortOrNull('1970-05-11 13:45:07', 9, 'UTC')) / 86400) * 86400, 9, 'UTC')"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(bool));",
            "SELECT accurateCastOrNull(toInt64OrNull(extract('hello x=456|wo', '[0-9.]+')), 'Boolean')"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(date));",
            "SELECT accurateCastOrNull(extract('hello x=456|wo', '[0-9.]+'), 'DateTime')"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(guid));",
            "SELECT accurateCastOrNull(extract('hello x=456|wo', '[0-9.]+'), 'UUID')"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(int));",
            "SELECT accurateCastOrNull(extract('hello x=456|wo', '[0-9.]+'), 'Int32')"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(long));",
            "SELECT accurateCastOrNull(extract('hello x=456|wo', '[0-9.]+'), 'Int64')"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(real));",
            "SELECT accurateCastOrNull(extract('hello x=456|wo', '[0-9.]+'), 'Float64')"
        },
        {
            "print extract('x=([0-9.]+)', 1, 'hello x=456|wo' , typeof(decimal));",
            "SELECT toDecimal128OrNull(if(countSubstrings(extract('hello x=456|wo', '[0-9.]+'), '.') > 1, NULL, extract('hello x=456|wo', '[0-9.]+')), length(substr(extract('hello x=456|wo', '[0-9.]+'), position(extract('hello x=456|wo', '[0-9.]+'), '.') + 1)))"
        },
        {
            "print bin(datetime(1970-05-11 13:45:07.456345672), 1ms)",
            "SELECT toDateTime64(toInt64(toFloat64(parseDateTime64BestEffortOrNull('1970-05-11 13:45:07.456345672', 9, 'UTC')) / 0.001) * 0.001, 9, 'UTC')"
        },
        {
            "print bin(datetime(1970-05-11 13:45:07.456345672), 1microseconds)",
            "SELECT toDateTime64(toInt64(toFloat64(parseDateTime64BestEffortOrNull('1970-05-11 13:45:07.456345672', 9, 'UTC')) / 0.000001) * 0.000001, 9, 'UTC')"
        },
        {
            "print parse_version('1.2.3.40')",
            "SELECT if((length(splitByChar('.', '1.2.3.40')) > 4) OR (length(splitByChar('.', '1.2.3.40')) < 1) OR (match('1.2.3.40', '.*[a-zA-Z]+.*') = 1), toDecimal128OrNull('NULL', 0), toDecimal128OrNull(substring(arrayStringConcat(arrayMap(x -> leftPad(x, 8, '0'), arrayMap(x -> if(empty(x), '0', x), arrayResize(splitByChar('.', '1.2.3.40'), 4)))), 8), 0))"
        },
        {
            "print parse_version('1')",
            "SELECT if((length(splitByChar('.', '1')) > 4) OR (length(splitByChar('.', '1')) < 1) OR (match('1', '.*[a-zA-Z]+.*') = 1), toDecimal128OrNull('NULL', 0), toDecimal128OrNull(substring(arrayStringConcat(arrayMap(x -> leftPad(x, 8, '0'), arrayMap(x -> if(empty(x), '0', x), arrayResize(splitByChar('.', '1'), 4)))), 8), 0))"
        },
        {
            "print parse_json( dynamic([1, 2, 3]))",
            "SELECT [1, 2, 3]"
        },
        {
            "print parse_json('{\"a\":123.5, \"b\":\"{\\\"c\\\":456}\"}')",
            "SELECT if(isValidJSON('{\"a\":123.5, \"b\":\"{\"c\":456}\"}'), JSON_QUERY('{\"a\":123.5, \"b\":\"{\"c\":456}\"}', '$'), toJSONString('{\"a\":123.5, \"b\":\"{\"c\":456}\"}'))"
        },
        {
            "print extract_json( '$.a' , '{\"a\":123, \"b\":\"{\"c\":456}\"}' , typeof(long))",
            "SELECT accurateCastOrNull(JSON_VALUE('{\"a\":123, \"b\":\"{\"c\":456}\"}', '$.a'), 'Int64')"
        },
        {
            "print bin(datetime(1970-05-11 13:45:07.456345672), 1ms)",
            "SELECT toDateTime64(toInt64(toFloat64(parseDateTime64BestEffortOrNull('1970-05-11 13:45:07.456345672', 9, 'UTC')) / 0.001) * 0.001, 9, 'UTC')"
        },
        {
            "print bin(datetime(1970-05-11 13:45:07.456345672), 1microseconds)",
            "SELECT toDateTime64(toInt64(toFloat64(parseDateTime64BestEffortOrNull('1970-05-11 13:45:07.456345672', 9, 'UTC')) / 0.000001) * 0.000001, 9, 'UTC')"
        },
        {
            "print parse_command_line('echo \"hello world!\" print$?', 'windows')",
            "SELECT if(empty('echo \"hello world!\" print$?') OR hasAll(splitByChar(' ', 'echo \"hello world!\" print$?'), ['']), arrayMap(x -> NULL, splitByChar(' ', '')), splitByChar(' ', 'echo \"hello world!\" print$?'))"
        },
        {
            "print reverse(123)",
            "SELECT reverse(accurateCastOrNull(123, 'String'))"
        },
        {
            "print reverse(123.34)",
            "SELECT reverse(accurateCastOrNull(123.34, 'String'))"
        },
        {
            "print reverse('clickhouse')",
            "SELECT reverse(accurateCastOrNull('clickhouse', 'String'))"
        },
        {
            "print result=parse_csv('aa,b,cc')",
            "SELECT if(CAST(position('aa,b,cc', '\\n'), 'UInt8'), splitByChar(',', substring('aa,b,cc', 1, position('aa,b,cc', '\\n') - 1)), splitByChar(',', substring('aa,b,cc', 1, length('aa,b,cc')))) AS result"
        },
        {
            "print result_multi_record=parse_csv('record1,a,b,c\nrecord2,x,y,z')",
            "SELECT if(CAST(position('record1,a,b,c\\nrecord2,x,y,z', '\\n'), 'UInt8'), splitByChar(',', substring('record1,a,b,c\\nrecord2,x,y,z', 1, position('record1,a,b,c\\nrecord2,x,y,z', '\\n') - 1)), splitByChar(',', substring('record1,a,b,c\\nrecord2,x,y,z', 1, length('record1,a,b,c\\nrecord2,x,y,z')))) AS result_multi_record"
        },
        {
            "Customers | project name_abbr = strcat(substring(FirstName,0,3), ' ', substring(LastName,2))| order by LastName",
            "SELECT concat(if(toInt64(length(FirstName)) <= 0, '', substr(FirstName, (((0 % toInt64(length(FirstName))) + toInt64(length(FirstName))) % toInt64(length(FirstName))) + 1, 3)), ' ', if(toInt64(length(LastName)) <= 0, '', substr(LastName, (((2 % toInt64(length(LastName))) + toInt64(length(LastName))) % toInt64(length(LastName))) + 1))) AS name_abbr\nFROM Customers\nORDER BY LastName DESC"
        }
})));
