set allow_experimental_variant_type = 1;
set allow_suspicious_variant_types = 1;
set session_timezone = 'UTC';

select 'JSON';
select 'String';
select v, variantElement(v, 'String') from format(JSONEachRow, 'v Variant(String, UInt64)', '{"v" : null}, {"v" : "string"}, {"v" : 42}') format JSONEachRow;

select 'FixedString';
select v, variantElement(v, 'FixedString(4)') from format(JSONEachRow, 'v Variant(String, FixedString(4))', '{"v" : null}, {"v" : "string"}, {"v" : "abcd"}') format JSONEachRow;

select 'Bool';
select v, variantElement(v, 'Bool') from format(JSONEachRow, 'v Variant(String, Bool)', '{"v" : null}, {"v" : "string"}, {"v" : true}') format JSONEachRow;

select 'Integers';
select v, variantElement(v, 'Int8') from format(JSONEachRow, 'v Variant(String, Int8, UInt64)', '{"v" : null}, {"v" : "string"}, {"v" : -1}, {"v" : 0}, {"v" : 10000000000}') format JSONEachRow;
select v, variantElement(v, 'UInt8') from format(JSONEachRow, 'v Variant(String, UInt8, Int64)', '{"v" : null}, {"v" : "string"}, {"v" : -1}, {"v" : 0}, {"v" : 10000000000}') format JSONEachRow;
select v, variantElement(v, 'Int16') from format(JSONEachRow, 'v Variant(String, Int16, Int64)', '{"v" : null}, {"v" : "string"}, {"v" : -1}, {"v" : 0}, {"v" : 10000000000}') format JSONEachRow;
select v, variantElement(v, 'UInt16') from format(JSONEachRow, 'v Variant(String, UInt16, Int64)', '{"v" : null}, {"v" : "string"}, {"v" : -1}, {"v" : 0}, {"v" : 10000000000}') format JSONEachRow;
select v, variantElement(v, 'Int32') from format(JSONEachRow, 'v Variant(String, Int32, Int64)', '{"v" : null}, {"v" : "string"}, {"v" : -1}, {"v" : 0}, {"v" : 10000000000}') format JSONEachRow;
select v, variantElement(v, 'UInt32') from format(JSONEachRow, 'v Variant(String, UInt32, Int64)', '{"v" : null}, {"v" : "string"}, {"v" : -1}, {"v" : 0}, {"v" : 10000000000}') format JSONEachRow;
select v, variantElement(v, 'Int64') from format(JSONEachRow, 'v Variant(String, Int64, Int128)', '{"v" : null}, {"v" : "string"}, {"v" : -1}, {"v" : 0}, {"v" : 10000000000000000000000}') format JSONEachRow;
select v, variantElement(v, 'UInt64') from format(JSONEachRow, 'v Variant(String, UInt64, Int128)', '{"v" : null}, {"v" : "string"}, {"v" : -1}, {"v" : 0}, {"v" : 10000000000000000000000}') format JSONEachRow;
select v, variantElement(v, 'Int128') from format(JSONEachRow, 'v Variant(String, Int128, Int256)', '{"v" : null}, {"v" : "string"}, {"v" : -1}, {"v" : 0}') format JSONEachRow;
select v, variantElement(v, 'UInt128') from format(JSONEachRow, 'v Variant(String, UInt128, Int256)', '{"v" : null}, {"v" : "string"}, {"v" : -1}, {"v" : 0}') format JSONEachRow;

select 'Floats';
select v, variantElement(v, 'Float32') from format(JSONEachRow, 'v Variant(String, Float32)', '{"v" : null}, {"v" : "string"}, {"v" : 42.42}') format JSONEachRow;
select v, variantElement(v, 'Float64') from format(JSONEachRow, 'v Variant(String, Float64)', '{"v" : null}, {"v" : "string"}, {"v" : 42.42}') format JSONEachRow;

select 'Decimals';
select v, variantElement(v, 'Decimal32(6)') from format(JSONEachRow, 'v Variant(String, Decimal32(6))', '{"v" : null}, {"v" : "string"}, {"v" : 42.42}, {"v" : 4242424242424242424242424242424242424242424242424242424242424242424242424242424242424242424242.424242424242424242}') format JSONEachRow;
select v, variantElement(v, 'Decimal64(6)') from format(JSONEachRow, 'v Variant(String, Decimal64(6))', '{"v" : null}, {"v" : "string"}, {"v" : 42.42}, {"v" : 4242424242424242424242424242424242424242424242424242424242424242424242424242424242424242424242.424242424242424242}') format JSONEachRow;
select v, variantElement(v, 'Decimal128(6)') from format(JSONEachRow, 'v Variant(String, Decimal128(6))', '{"v" : null}, {"v" : "string"}, {"v" : 42.42}, {"v" : 4242424242424242424242424242424242424242424242424242424242424242424242424242424242424242424242.424242424242424242}') format JSONEachRow;
select v, variantElement(v, 'Decimal256(6)') from format(JSONEachRow, 'v Variant(String, Decimal256(6))', '{"v" : null}, {"v" : "string"}, {"v" : 42.42}, {"v" : 4242424242424242424242424242424242424242424242424242424242424242424242424242424242424242424242.424242424242424242}') format JSONEachRow;

select 'Dates and DateTimes';
select v, variantElement(v, 'Date') from format(JSONEachRow, 'v Variant(String, Date, DateTime64)', '{"v" : null}, {"v" : "string"}, {"v" : "2020-01-01"}, {"v" : "2020-01-01 00:00:00.999"}') format JSONEachRow;
select v, variantElement(v, 'Date32') from format(JSONEachRow, 'v Variant(String, Date32, DateTime64)', '{"v" : null}, {"v" : "string"}, {"v" : "1900-01-01"}, {"v" : "2020-01-01 00:00:00.999"}') format JSONEachRow;
select v, variantElement(v, 'DateTime') from format(JSONEachRow, 'v Variant(String, DateTime, DateTime64)', '{"v" : null}, {"v" : "string"}, {"v" : "2020-01-01 00:00:00"}, {"v" : "2020-01-01 00:00:00.999"}') format JSONEachRow;
select v, variantElement(v, 'DateTime64') from format(JSONEachRow, 'v Variant(String, DateTime64)', '{"v" : null}, {"v" : "string"}, {"v" : "2020-01-01 00:00:00.999"}, {"v" : "2020-01-01 00:00:00.999999999 ABC"}') format JSONEachRow;

select 'UUID';
select v, variantElement(v, 'UUID') from format(JSONEachRow, 'v Variant(String, UUID)', '{"v" : null}, {"v" : "string"}, {"v" : "c8619cca-0caa-445e-ae76-1d4f6e0b3927"}') format JSONEachRow;

select 'IPv4';
select v, variantElement(v, 'IPv4') from format(JSONEachRow, 'v Variant(String, IPv4)', '{"v" : null}, {"v" : "string"}, {"v" : "127.0.0.1"}') format JSONEachRow;

select 'IPv6';
select v, variantElement(v, 'IPv6') from format(JSONEachRow, 'v Variant(String, IPv6)', '{"v" : null}, {"v" : "string"}, {"v" : "2001:0db8:85a3:0000:0000:8a2e:0370:7334"}') format JSONEachRow;

select 'Enum';
select v, variantElement(v, 'Enum(''a'' = 1)') from format(JSONEachRow, 'v Variant(String, UInt32, Enum(''a'' = 1))', '{"v" : null}, {"v" : "string"}, {"v" : "a"}, {"v" : 1}, {"v" : 2}') format JSONEachRow;

select 'Map';
select v, variantElement(v, 'Map(String, UInt64)') from format(JSONEachRow, 'v Variant(String, Map(String, UInt64))', '{"v" : null}, {"v" : "string"}, {"v" : {"a" : 42, "b" : 43, "c" : null}}, {"v" : {"c" : 44, "d" : [1,2,3]}}') format JSONEachRow;

select 'Tuple';
select v, variantElement(v, 'Tuple(a UInt64, b UInt64)') from format(JSONEachRow, 'v Variant(String, Tuple(a UInt64, b UInt64))', '{"v" : null}, {"v" : "string"}, {"v" : {"a" : 42, "b" : null}}, {"v" : {"a" : 44, "d" : 32}}') format JSONEachRow;
select v, variantElement(v, 'Tuple(a UInt64, b UInt64)') from format(JSONEachRow, 'v Variant(String, Tuple(a UInt64, b UInt64))', '{"v" : null}, {"v" : "string"}, {"v" : {"a" : 42, "b" : null}}, {"v" : {"a" : 44, "d" : 32}}') settings input_format_json_defaults_for_missing_elements_in_named_tuple=0;

select 'Array';
select v, variantElement(v, 'Array(UInt64)') from format(JSONEachRow, 'v Variant(String, Array(UInt64))', '{"v" : null}, {"v" : "string"}, {"v" : [1, 2, 3]}, {"v" : [null, null, null]} {"v" : [1, 2, "hello"]}') format JSONEachRow;

select 'LowCardinality';
select v, variantElement(v, 'LowCardinality(String)') from format(JSONEachRow, 'v Variant(LowCardinality(String), UInt64)', '{"v" : null}, {"v" : "string"}, {"v" : 42}') format JSONEachRow;
select v, variantElement(v, 'Array(LowCardinality(Nullable(String)))') from format(JSONEachRow, 'v Variant(Array(LowCardinality(Nullable(String))), UInt64)', '{"v" : null}, {"v" : ["string", null]}, {"v" : 42}') format JSONEachRow;

select 'Nullable';
select v, variantElement(v, 'Array(Nullable(String))') from format(JSONEachRow, 'v Variant(String, Array(Nullable(String)))', '{"v" : null}, {"v" : "string"}, {"v" : ["hello", null, "world"]}') format JSONEachRow;

select repeat('-', 80) format JSONEachRow;

select 'CSV';
select 'String';
select v, variantElement(v, 'String') from format(CSV, 'v Variant(String, UInt64)', '\\N\n"string"\nstring\n42') format CSV;

select 'FixedString';
select v, variantElement(v, 'FixedString(4)') from format(CSV, 'v Variant(String, FixedString(4))', '\\N\n"string"\nstring\n"abcd"') format CSV;

select 'Bool';
select v, variantElement(v, 'Bool') from format(CSV, 'v Variant(String, Bool)', '\\N\ntruee\ntrue') format CSV;

select 'Integers';
select v, variantElement(v, 'Int8') from format(CSV, 'v Variant(String, Int8, UInt64)', '\n"string"\n-1\n0\n10000000000\n42d42') format CSV;
select v, variantElement(v, 'UInt8') from format(CSV, 'v Variant(String, UInt8, Int64)', '\\N\n"string"\n-1\n0\n10000000000\n42d42') format CSV;
select v, variantElement(v, 'Int16') from format(CSV, 'v Variant(String, Int16, Int64)', '\\N\n"string"\n-1\n0\n10000000000\n42d42') format CSV;
select v, variantElement(v, 'UInt16') from format(CSV, 'v Variant(String, UInt16, Int64)', '\\N\n"string"\n-1\n0\n10000000000\n42d42') format CSV;
select v, variantElement(v, 'Int32') from format(CSV, 'v Variant(String, Int32, Int64)', '\\N\n"string"\n-1\n0\n10000000000\n42d42') format CSV;
select v, variantElement(v, 'UInt32') from format(CSV, 'v Variant(String, UInt32, Int64)', '\\N\n"string"\n-1\n0\n10000000000\n42d42') format CSV;
select v, variantElement(v, 'Int64') from format(CSV, 'v Variant(String, Int64, Int128)', '\\N\n"string"\n-1\n0\n10000000000000000000000\n42d42') format CSV;
select v, variantElement(v, 'UInt64') from format(CSV, 'v Variant(String, UInt64, Int128)', '\\N\n"string"\n-1\n0\n10000000000000000000000\n42d42') format CSV;
select v, variantElement(v, 'Int128') from format(CSV, 'v Variant(String, Int128, Int256)', '\\N\n"string"\n-1\n0\n42d42') format CSV;
select v, variantElement(v, 'UInt128') from format(CSV, 'v Variant(String, UInt128, Int256)', '\\N\n"string"\n-1\n0\n42d42') format CSV;

select 'Floats';
select v, variantElement(v, 'Float32') from format(CSV, 'v Variant(String, Float32)', '\\N\n"string"\n42.42\n42.d42') format CSV;
select v, variantElement(v, 'Float64') from format(CSV, 'v Variant(String, Float64)', '\\N\n"string"\n42.42\n42.d42') format CSV;

select 'Decimals';
select v, variantElement(v, 'Decimal32(6)') from format(CSV, 'v Variant(String, Decimal32(6))', '\\N\n"string"\n42.42\n42d42\n4242424242424242424242424242424242424242424242424242424242424242424242424242424242424242424242.424242424242424242') format CSV;
select v, variantElement(v, 'Decimal64(6)') from format(CSV, 'v Variant(String, Decimal64(6))', '\\N\n"string"\n42.42\n42d42\n4242424242424242424242424242424242424242424242424242424242424242424242424242424242424242424242.424242424242424242') format CSV;
select v, variantElement(v, 'Decimal128(6)') from format(CSV, 'v Variant(String, Decimal128(6))', '\\N\n"string"\n42.42\n42d42\n4242424242424242424242424242424242424242424242424242424242424242424242424242424242424242424242.424242424242424242') format CSV;
select v, variantElement(v, 'Decimal256(6)') from format(CSV, 'v Variant(String, Decimal256(6))', '\\N\n"string"\n42.42\n42d42\n4242424242424242424242424242424242424242424242424242424242424242424242424242424242424242424242.424242424242424242') format CSV;

select 'Dates and DateTimes';
select v, variantElement(v, 'Date') from format(CSV, 'v Variant(String, Date, DateTime64)', '\\N\n"string"\n"2020-01-d1"\n"2020-01-01"\n"2020-01-01 00:00:00.999"') format CSV;
select v, variantElement(v, 'Date32') from format(CSV, 'v Variant(String, Date32, DateTime64)', '\\N\n"string"\n"2020-01-d1"\n"1900-01-01"\n"2020-01-01 00:00:00.999"') format CSV;
select v, variantElement(v, 'DateTime') from format(CSV, 'v Variant(String, DateTime, DateTime64)', '\\N\n"string"\n"2020-01-d1"\n"2020-01-01 00:00:00"\n"2020-01-01 00:00:00.999"') format CSV;
select v, variantElement(v, 'DateTime64') from format(CSV, 'v Variant(String, DateTime64)', '\\N\n"string"\n"2020-01-d1"\n"2020-01-01 00:00:00.999"\n"2020-01-01 00:00:00.999999999 ABC"') format CSV;

select 'UUID';
select v, variantElement(v, 'UUID') from format(CSV, 'v Variant(String, UUID)', '\\N\n"string"\n"c8619cca-0caa-445e-ae76-1d4f6e0b3927"\nc8619cca-0caa-445e-ae76-1d4f6e0b3927AAA') format CSV;

select 'IPv4';
select v, variantElement(v, 'IPv4') from format(CSV, 'v Variant(String, IPv4)', '\\N\n"string"\n"127.0.0.1"\n"127.0.0.1AAA"') format CSV;

select 'IPv6';
select v, variantElement(v, 'IPv6') from format(CSV, 'v Variant(String, IPv6)', '\\N\n"string"\n"2001:0db8:85a3:0000:0000:8a2e:0370:7334"\n2001:0db8:85a3:0000:0000:8a2e:0370:7334AAA') format CSV;

select 'Enum';
select v, variantElement(v, 'Enum(''a'' = 1)') from format(CSV, 'v Variant(String, UInt32, Enum(''a'' = 1))', '\\N\n"string"\n"a"\n1\n2\naa') format CSV;

select 'Map';
select v, variantElement(v, 'Map(String, UInt64)') from format(CSV, 'v Variant(String, Map(String, UInt64))', '\\N\n"string"\n"{''a'' : 42, ''b'' : 43, ''c'' : null}"\n"{''c'' : 44, ''d'' : [1,2,3]}"\n"{''c'' : 44"') format CSV;

select 'Array';
select v, variantElement(v, 'Array(UInt64)') from format(CSV, 'v Variant(String, Array(UInt64))', '\\N\n"string"\n"[1, 2, 3]"\n"[null, null, null]"\n"[1, 2, ''hello'']"\n"[1, 2"') format CSV;

select 'LowCardinality';
select v, variantElement(v, 'LowCardinality(String)') from format(CSV, 'v Variant(LowCardinality(String), UInt64)', '\\N\n"string"\n42') format CSV;
select v, variantElement(v, 'Array(LowCardinality(Nullable(String)))') from format(CSV, 'v Variant(Array(LowCardinality(Nullable(String))), UInt64, String)', '\\N\n"[''string'', null]"\n"[''string'', nul]"\n42') format CSV;

select 'Nullable';
select v, variantElement(v, 'Array(Nullable(String))') from format(CSV, 'v Variant(String, Array(Nullable(String)))', '\\N\n"string"\n"[''hello'', null, ''world'']"\n"[''hello'', nul]"') format CSV;

select repeat('-', 80) format JSONEachRow;

select 'TSV';
select 'String';
select v, variantElement(v, 'String') from format(TSV, 'v Variant(String, UInt64)', '\\N\nstring\n42') format TSV;

select 'FixedString';
select v, variantElement(v, 'FixedString(4)') from format(TSV, 'v Variant(String, FixedString(4))', '\\N\nstring\nabcd') format TSV;

select 'Bool';
select v, variantElement(v, 'Bool') from format(TSV, 'v Variant(String, Bool)', '\\N\ntruee\ntrue') format TSV;

select 'Integers';
select v, variantElement(v, 'Int8') from format(TSV, 'v Variant(String, Int8, UInt64)', '\\N\nstring\n-1\n0\n10000000000\n42d42') format TSV;
select v, variantElement(v, 'UInt8') from format(TSV, 'v Variant(String, UInt8, Int64)', '\\N\nstring\n-1\n0\n10000000000\n42d42') format TSV;
select v, variantElement(v, 'Int16') from format(TSV, 'v Variant(String, Int16, Int64)', '\\N\nstring\n-1\n0\n10000000000\n42d42') format TSV;
select v, variantElement(v, 'UInt16') from format(TSV, 'v Variant(String, UInt16, Int64)', '\\N\nstring\n-1\n0\n10000000000\n42d42') format TSV;
select v, variantElement(v, 'Int32') from format(TSV, 'v Variant(String, Int32, Int64)', '\\N\nstring\n-1\n0\n10000000000\n42d42') format TSV;
select v, variantElement(v, 'UInt32') from format(TSV, 'v Variant(String, UInt32, Int64)', '\\N\nstring\n-1\n0\n10000000000\n42d42') format TSV;
select v, variantElement(v, 'Int64') from format(TSV, 'v Variant(String, Int64, Int128)', '\\N\nstring\n-1\n0\n10000000000000000000000\n42d42') format TSV;
select v, variantElement(v, 'UInt64') from format(TSV, 'v Variant(String, UInt64, Int128)', '\\N\nstring\n-1\n0\n10000000000000000000000\n42d42') format TSV;
select v, variantElement(v, 'Int128') from format(TSV, 'v Variant(String, Int128, Int256)', '\\N\nstring\n-1\n0\n42d42') format TSV;
select v, variantElement(v, 'UInt128') from format(TSV, 'v Variant(String, UInt128, Int256)', '\\N\nstring\n-1\n0\n42d42') format TSV;

select 'Floats';
select v, variantElement(v, 'Float32') from format(TSV, 'v Variant(String, Float32)', '\\N\nstring\n42.42\n42.d42') format TSV;
select v, variantElement(v, 'Float64') from format(TSV, 'v Variant(String, Float64)', '\\N\nstring\n42.42\n42.d42') format TSV;

select 'Decimals';
select v, variantElement(v, 'Decimal32(6)') from format(TSV, 'v Variant(String, Decimal32(6))', '\\N\nstring\n42.42\n42d42\n4242424242424242424242424242424242424242424242424242424242424242424242424242424242424242424242.424242424242424242') format TSV;
select v, variantElement(v, 'Decimal64(6)') from format(TSV, 'v Variant(String, Decimal64(6))', '\\N\nstring\n42.42\n42d42\n4242424242424242424242424242424242424242424242424242424242424242424242424242424242424242424242.424242424242424242') format TSV;
select v, variantElement(v, 'Decimal128(6)') from format(TSV, 'v Variant(String, Decimal128(6))', '\\N\nstring\n42.42\n42d42\n4242424242424242424242424242424242424242424242424242424242424242424242424242424242424242424242.424242424242424242') format TSV;
select v, variantElement(v, 'Decimal256(6)') from format(TSV, 'v Variant(String, Decimal256(6))', '\\N\nstring\n42.42\n42d42\n4242424242424242424242424242424242424242424242424242424242424242424242424242424242424242424242.424242424242424242') format TSV;

select 'Dates and DateTimes';
select v, variantElement(v, 'Date') from format(TSV, 'v Variant(String, Date, DateTime64)', '\\N\nstring\n2020-01-d1\n2020-01-01\n2020-01-01 00:00:00.999') format TSV;
select v, variantElement(v, 'Date32') from format(TSV, 'v Variant(String, Date32, DateTime64)', '\\N\nstring\n2020-01-d1\n1900-01-01\n2020-01-01 00:00:00.999') format TSV;
select v, variantElement(v, 'DateTime') from format(TSV, 'v Variant(String, DateTime, DateTime64)', '\\N\nstring\n2020-01-d1\n2020-01-01 00:00:00\n2020-01-01 00:00:00.999') format TSV;
select v, variantElement(v, 'DateTime64') from format(TSV, 'v Variant(String, DateTime64)', '\\N\nstring\n2020-01-d1\n2020-01-01 00:00:00.999\n2020-01-01 00:00:00.999999999 ABC') format TSV;

select 'UUID';
select v, variantElement(v, 'UUID') from format(TSV, 'v Variant(String, UUID)', '\\N\nstring\nc8619cca-0caa-445e-ae76-1d4f6e0b3927\nc8619cca-0caa-445e-ae76-1d4f6e0b3927AAA') format TSV;

select 'IPv4';
select v, variantElement(v, 'IPv4') from format(TSV, 'v Variant(String, IPv4)', '\\N\nstring\n127.0.0.1\n127.0.0.1AAA') format TSV;

select 'IPv6';
select v, variantElement(v, 'IPv6') from format(TSV, 'v Variant(String, IPv6)', '\\N\nstring\n2001:0db8:85a3:0000:0000:8a2e:0370:7334\n2001:0db8:85a3:0000:0000:8a2e:0370:7334AAA') format TSV;

select 'Enum';
select v, variantElement(v, 'Enum(''a'' = 1)') from format(TSV, 'v Variant(String, UInt32, Enum(''a'' = 1))', '\\N\nstring\na\n1\n2\naa') format TSV;

select 'Map';
select v, variantElement(v, 'Map(String, UInt64)') from format(TSV, 'v Variant(String, Map(String, UInt64))', '\\N\nstring\n{''a'' : 42, ''b'' : 43, ''c'' : null}\n{''c'' : 44, ''d'' : [1,2,3]}\n{''c'' : 44') format TSV;

select 'Array';
select v, variantElement(v, 'Array(UInt64)') from format(TSV, 'v Variant(String, Array(UInt64))', '\\N\nstring\n[1, 2, 3]\n[null, null, null]\n[1, 2, ''hello'']\n[1, 2') format TSV;

select 'LowCardinality';
select v, variantElement(v, 'LowCardinality(String)') from format(TSV, 'v Variant(LowCardinality(String), UInt64)', '\\N\nstring\n42') format TSV;
select v, variantElement(v, 'Array(LowCardinality(Nullable(String)))') from format(TSV, 'v Variant(Array(LowCardinality(Nullable(String))), UInt64, String)', '\\N\n[''string'', null]\n[''string'', nul]\n42') format TSV;

select 'Nullable';
select v, variantElement(v, 'Array(Nullable(String))') from format(TSV, 'v Variant(String, Array(Nullable(String)))', '\\N\nstring\n[''hello'', null, ''world'']\n[''hello'', nul]') format TSV;

select repeat('-', 80) format JSONEachRow;

select 'Values';
select 'String';
select v, variantElement(v, 'String') from format(Values, 'v Variant(String, UInt64)', '(NULL), (''string''), (42)') format Values;

select 'FixedString';
select v, variantElement(v, 'FixedString(4)') from format(Values, 'v Variant(String, FixedString(4))', '(NULL), (''string''), (''abcd'')') format Values;

select 'Bool';
select v, variantElement(v, 'Bool') from format(Values, 'v Variant(String, Bool)', '(NULL), (true)') format Values;

select 'Integers';
select v, variantElement(v, 'Int8') from format(Values, 'v Variant(String, Int8, UInt64)', '(NULL), (''string''), (-1), (0), (10000000000)') format Values;
select v, variantElement(v, 'UInt8') from format(Values, 'v Variant(String, UInt8, Int64)', '(NULL), (''string''), (-1), (0), (10000000000)') format Values;
select v, variantElement(v, 'Int16') from format(Values, 'v Variant(String, Int16, Int64)', '(NULL), (''string''), (-1), (0), (10000000000)') format Values;
select v, variantElement(v, 'UInt16') from format(Values, 'v Variant(String, UInt16, Int64)', '(NULL), (''string''), (-1), (0), (10000000000)') format Values;
select v, variantElement(v, 'Int32') from format(Values, 'v Variant(String, Int32, Int64)', '(NULL), (''string''), (-1), (0), (10000000000)') format Values;
select v, variantElement(v, 'UInt32') from format(Values, 'v Variant(String, UInt32, Int64)', '(NULL), (''string''), (-1), (0), (10000000000)') format Values;
select v, variantElement(v, 'Int64') from format(Values, 'v Variant(String, Int64, Int128)', '(NULL), (''string''), (-1), (0), (10000000000000000000000)') format Values;
select v, variantElement(v, 'UInt64') from format(Values, 'v Variant(String, UInt64, Int128)', '(NULL), (''string''), (-1), (0), (10000000000000000000000)') format Values;
select v, variantElement(v, 'Int128') from format(Values, 'v Variant(String, Int128, Int256)', '(NULL), (''string''), (-1), (0)') format Values;
select v, variantElement(v, 'UInt128') from format(Values, 'v Variant(String, UInt128, Int256)', '(NULL), (''string''), (-1), (0)') format Values;

select 'Floats';
select v, variantElement(v, 'Float32') from format(Values, 'v Variant(String, Float32)', '(NULL), (''string''), (42.42)') format Values;
select v, variantElement(v, 'Float64') from format(Values, 'v Variant(String, Float64)', '(NULL), (''string''), (42.42)') format Values;

select 'Decimals';
select v, variantElement(v, 'Decimal32(6)') from format(Values, 'v Variant(String, Decimal32(6))', '(NULL), (''string''), (42.42)') format Values;
select v, variantElement(v, 'Decimal64(6)') from format(Values, 'v Variant(String, Decimal64(6))', '(NULL), (''string''), (42.42)') format Values;
select v, variantElement(v, 'Decimal128(6)') from format(Values, 'v Variant(String, Decimal128(6))', '(NULL), (''string''), (42.42)') format Values;
select v, variantElement(v, 'Decimal256(6)') from format(Values, 'v Variant(String, Decimal256(6))', '(NULL), (''string''), (42.42)') format Values;

select 'Dates and DateTimes';
select v, variantElement(v, 'Date') from format(Values, 'v Variant(String, Date, DateTime64)', '(NULL), (''string''), (''2020-01-d1''), (''2020-01-01''), (''2020-01-01 00:00:00.999'')') format Values;
select v, variantElement(v, 'Date32') from format(Values, 'v Variant(String, Date32, DateTime64)', '(NULL), (''string''), (''2020-01-d1''), (''1900-01-01''), (''2020-01-01 00:00:00.999'')') format Values;
select v, variantElement(v, 'DateTime') from format(Values, 'v Variant(String, DateTime, DateTime64)', '(NULL), (''string''), (''2020-01-d1''), (''2020-01-01 00:00:00''), (''2020-01-01 00:00:00.999'')') format Values;
select v, variantElement(v, 'DateTime64') from format(Values, 'v Variant(String, DateTime64)', '(NULL), (''string''), (''2020-01-d1''), (''2020-01-01 00:00:00.999''), (''2020-01-01 00:00:00.999999999 ABC'')') format Values;

select 'UUID';
select v, variantElement(v, 'UUID') from format(Values, 'v Variant(String, UUID)', '(NULL), (''string''), (''c8619cca-0caa-445e-ae76-1d4f6e0b3927''), (''c8619cca-0caa-445e-ae76-1d4f6e0b3927AAA'')') format Values;

select 'IPv4';
select v, variantElement(v, 'IPv4') from format(Values, 'v Variant(String, IPv4)', '(NULL), (''string''), (''127.0.0.1''), (''127.0.0.1AAA'')') format Values;

select 'IPv6';
select v, variantElement(v, 'IPv6') from format(Values, 'v Variant(String, IPv6)', '(NULL), (''string''), (''2001:0db8:85a3:0000:0000:8a2e:0370:7334''), (''2001:0db8:85a3:0000:0000:8a2e:0370:7334AAA'')') format Values;

select 'Enum';
select v, variantElement(v, 'Enum(''a'' = 1)') from format(Values, 'v Variant(String, UInt32, Enum(''a'' = 1))', '(NULL), (''string''), (''a''), (1), (2), (''aa'')') format Values;

select 'Map';
select v, variantElement(v, 'Map(String, UInt64)') from format(Values, 'v Variant(String, Map(String, UInt64))', '(NULL), (''string''), ({''a'' : 42, ''b'' : 43, ''c'' : null})') format Values;

select 'Array';
select v, variantElement(v, 'Array(UInt64)') from format(Values, 'v Variant(String, Array(UInt64))', '(NULL), (''string''), ([1, 2, 3]), ([null, null, null])') format Values;

select 'LowCardinality';
select v, variantElement(v, 'LowCardinality(String)') from format(Values, 'v Variant(LowCardinality(String), UInt64)', '(NULL), (''string''), (42)') format Values;
select v, variantElement(v, 'Array(LowCardinality(Nullable(String)))') from format(Values, 'v Variant(Array(LowCardinality(Nullable(String))), UInt64, String)', '(NULL), ([''string'', null]), (42)') format Values;

select 'Nullable';
select v, variantElement(v, 'Array(Nullable(String))') from format(Values, 'v Variant(String, Array(Nullable(String)))', '(NULL), (''string''), ([''hello'', null, ''world''])') format Values;

select '';
