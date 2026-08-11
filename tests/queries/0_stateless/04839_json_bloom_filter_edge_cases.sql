DROP TABLE IF EXISTS json_bf_edges;
DROP TABLE IF EXISTS json_bf_dynamic_edges;
DROP TABLE IF EXISTS json_bf_shared_edges;
DROP TABLE IF EXISTS json_bf_materialize;

CREATE TABLE json_bf_invalid (id UInt64, j JSON, INDEX idx j TYPE jsonbf_v1(0.01) GRANULARITY 1) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE json_bf_invalid (id UInt64, j JSON, INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.01, false_positive_rate = 0.02) GRANULARITY 1) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE json_bf_invalid (id UInt64, j JSON, INDEX idx j TYPE jsonbf_v1(false_positive_rate = 2.0) GRANULARITY 1) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE json_bf_invalid (id UInt64, j JSON, INDEX idx j TYPE jsonbf_v1(tokenizer = 'splitByNonAlpha') GRANULARITY 1) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE json_bf_invalid (id UInt64, j JSON, INDEX idx j TYPE jsonbf_v1(bogus = 1) GRANULARITY 1) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }
CREATE TABLE json_bf_invalid (id UInt64, j JSON, INDEX idx (j, id) TYPE jsonbf_v1() GRANULARITY 1) ENGINE = MergeTree ORDER BY id; -- { serverError INCORRECT_NUMBER_OF_COLUMNS }
CREATE TABLE json_bf_invalid (id UInt64, j JSON, INDEX idx id TYPE jsonbf_v1() GRANULARITY 1) ENGINE = MergeTree ORDER BY id; -- { serverError BAD_ARGUMENTS }

CREATE TABLE json_bf_edges
(
    id UInt64,
    j JSON(
        n Int64,
        u UInt64,
        f Float64,
        dec Decimal64(4),
        big_dec Decimal128(2),
        day Date,
        dt DateTime64(9, 'UTC'),
        ip IPv4,
        ip6 IPv6,
        uuid UUID,
        nf Nullable(Float64),
        lc LowCardinality(String),
        arr Array(Int64),
        dyn_arr Array(Dynamic),
        map_s Map(String, Int64),
        map_lc Map(LowCardinality(String), Int64),
        tup Tuple(a Int64, b String),
        nullable Nullable(String)),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001, tokenizer = ngrams(3)) GRANULARITY 2
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO json_bf_edges FORMAT JSONEachRow
{"id":1,"j":{"n":-9223372036854775808,"u":18446744073709551615,"f":-0.0,"dec":1.2300,"big_dec":10000000000000000.01,"day":"1970-01-02","dt":"1970-01-01 00:00:01.500000000","ip":"192.0.2.1","ip6":"2001:db8::1","uuid":"00000000-0000-0000-0000-000000000001","nf":-0.0,"lc":"low-one","arr":[1,2,3],"dyn_arr":[1,"one",true],"map_s":{"first":7,"second":7},"map_lc":{"first":70,"second":71},"tup":{"a":9,"b":"tuple-one"},"nullable":"present"}}
{"id":2,"j":{"n":42,"u":42,"f":1.5,"dec":2.3400,"big_dec":10000000000000000.02,"day":"1970-01-03","dt":"1970-01-01 00:00:02.500000000","ip":"192.0.2.2","ip6":"2001:db8::2","uuid":"00000000-0000-0000-0000-000000000002","nf":1.5,"lc":"low-two","arr":[3,4,5],"dyn_arr":[2,"two",false],"map_s":{"first":8},"map_lc":{"first":80},"tup":{"a":10,"b":"tuple-two"},"nullable":null}}
{"id":3,"j":{"n":0,"u":0,"f":0.0,"dec":0,"big_dec":0,"day":"1970-01-01","dt":"1970-01-01 00:00:00.000000000","ip":"0.0.0.0","ip6":"::","uuid":"00000000-0000-0000-0000-000000000000","nf":0.0,"lc":"","arr":[],"dyn_arr":[],"map_s":{},"map_lc":{},"tup":{"a":0,"b":""}}}
;

SELECT 'int64 min', groupArray(id) FROM json_bf_edges WHERE j.n = -9223372036854775808 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'uint64 max', groupArray(id) FROM json_bf_edges WHERE j.u = 18446744073709551615 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'decimal typed', groupArray(id) FROM json_bf_edges WHERE j.dec = toDecimal64('1.2300', 4) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'decimal float conservative', groupArray(id) FROM json_bf_edges WHERE j.dec = 1.23;
SELECT 'decimal float collision', groupArray(id) FROM json_bf_edges WHERE j.big_dec = 1e16;
SELECT 'date', groupArray(id) FROM json_bf_edges WHERE j.day = toDate('1970-01-02') SETTINGS force_data_skipping_indices = 'idx';
SELECT 'datetime64 typed', groupArray(id) FROM json_bf_edges WHERE j.dt = toDateTime64('1970-01-01 00:00:01.500000000', 9, 'UTC') SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_bf_edges WHERE j.dt = 1.5; -- { serverError TYPE_MISMATCH }
SELECT 'ipv4', groupArray(id) FROM json_bf_edges WHERE j.ip = toIPv4('192.0.2.1') SETTINGS force_data_skipping_indices = 'idx';
SELECT 'ipv6', groupArray(id) FROM json_bf_edges WHERE j.ip6 = toIPv6('2001:db8::2') SETTINGS force_data_skipping_indices = 'idx';
SELECT 'uuid', groupArray(id) FROM json_bf_edges WHERE j.uuid = toUUID('00000000-0000-0000-0000-000000000002') SETTINGS force_data_skipping_indices = 'idx';
SELECT 'nullable negative zero', arraySort(groupArray(id)) FROM json_bf_edges WHERE j.nf = 0.0 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'low cardinality path', groupArray(id) FROM json_bf_edges WHERE j.lc = 'low-two' SETTINGS force_data_skipping_indices = 'idx';
SELECT 'typed in', arraySort(groupArray(id)) FROM json_bf_edges WHERE j.n IN (-9223372036854775808, 42) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'constant left', groupArray(id) FROM json_bf_edges WHERE 42 = j.n SETTINGS force_data_skipping_indices = 'idx';
SELECT 'array has', groupArray(id) FROM json_bf_edges WHERE has(j.arr, 4) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'array hasAny', groupArray(id) FROM json_bf_edges WHERE hasAny(j.arr, [2, 99]) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'array hasAll', groupArray(id) FROM json_bf_edges WHERE hasAll(j.arr, [3, 4]) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'dynamic array integer', groupArray(id) FROM json_bf_edges WHERE has(j.dyn_arr, toInt64(2)) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'dynamic array string', groupArray(id) FROM json_bf_edges WHERE has(j.dyn_arr, 'one') SETTINGS force_data_skipping_indices = 'idx';
SELECT 'dynamic array bool', groupArray(id) FROM json_bf_edges WHERE has(j.dyn_arr, true) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'dynamic array literal type', groupArray(id) FROM json_bf_edges WHERE has(j.dyn_arr, 2) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'dynamic array hasAny literal type', groupArray(id) FROM json_bf_edges WHERE hasAny(j.dyn_arr, [2, 99]) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'dynamic array hasAny typed', groupArray(id) FROM json_bf_edges WHERE hasAny(j.dyn_arr, [toInt64(2), toInt64(99)]) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'map string key', groupArray(id) FROM json_bf_edges WHERE j.map_s['second'] = 7 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'map string key unoptimized', groupArray(id) FROM json_bf_edges WHERE j.map_s['second'] = 7 SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 0;
SELECT 'map key separation', groupArray(id) FROM json_bf_edges WHERE j.map_s['missing'] = 7 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'map low cardinality key', groupArray(id) FROM json_bf_edges WHERE j.map_lc['second'] = 71 SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_bf_edges WHERE j.n = 42 AND j.map_s[123] = 7 SETTINGS force_data_skipping_indices = 'idx', optimize_functions_to_subcolumns = 0; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT 'tuple integer', groupArray(id) FROM json_bf_edges WHERE j.tup.a = 10 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'tuple string', groupArray(id) FROM json_bf_edges WHERE j.tup.b = 'tuple-one' SETTINGS force_data_skipping_indices = 'idx';
SELECT 'nullable present', groupArray(id) FROM json_bf_edges WHERE j.nullable = 'present' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_bf_edges WHERE CAST(j.nullable AS String) = 'absent'; -- { serverError CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN }
SELECT 'missing typed value', groupArray(id) FROM json_bf_edges WHERE j.n = 123456 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'and', groupArray(id) FROM json_bf_edges WHERE j.n = 42 AND j.tup.b = 'tuple-two' SETTINGS force_data_skipping_indices = 'idx';
SELECT 'or', arraySort(groupArray(id)) FROM json_bf_edges WHERE j.n = 42 OR j.ip = toIPv4('192.0.2.1') SETTINGS force_data_skipping_indices = 'idx';
SELECT 'not with indexable partner', groupArray(id) FROM json_bf_edges WHERE NOT (j.n = 42) AND j.tup.b = 'tuple-one' SETTINGS force_data_skipping_indices = 'idx';
SELECT 'like', groupArray(id) FROM json_bf_edges WHERE j.tup.b LIKE '%ple-tw%' SETTINGS force_data_skipping_indices = 'idx';
SELECT 'startsWith', groupArray(id) FROM json_bf_edges WHERE startsWith(j.tup.b, 'tuple-o') SETTINGS force_data_skipping_indices = 'idx';
SELECT 'endsWith', groupArray(id) FROM json_bf_edges WHERE endsWith(j.tup.b, 'e-two') SETTINGS force_data_skipping_indices = 'idx';
SELECT 'multiSearchAny', groupArray(id) FROM json_bf_edges WHERE multiSearchAny(j.tup.b, ['absent', 'ple-on']) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'match required substring', groupArray(id) FROM json_bf_edges WHERE match(j.tup.b, '^tuple-[ot].*') SETTINGS force_data_skipping_indices = 'idx';
SELECT 'map keys structural subcolumn', groupArray(id) FROM json_bf_edges WHERE has(j.map_s.keys, 'second');
SELECT 'map values structural subcolumn', groupArray(id) FROM json_bf_edges WHERE has(j.map_s.values, 8);
SELECT 'array size structural subcolumn', arraySort(groupArray(id)) FROM json_bf_edges WHERE j.arr.size0 = 3;
SELECT 'whole array conservative', groupArray(id) FROM json_bf_edges WHERE j.arr = [1, 2, 3];

CREATE TABLE json_bf_dynamic_edges
(
    id UInt64,
    j JSON(unsupported Dynamic(max_types = 0)),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001, tokenizer = ngrams(2)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_dynamic_edges FORMAT JSONEachRow
{"id":1,"j":{"value":7,"bool_value":true,"zero":-0.0,"poly":7,"nested":{"leaf":"alpha"},"items":[{"leaf":11},{"leaf":12}],"unsupported":null}}
{"id":2,"j":{"value":1.5,"bool_value":false,"zero":1.0,"poly":[7,8],"nested":{"leaf":"beta"},"items":[{"leaf":21}],"unsupported":[[1,2],[3]]}}
{"id":3,"j":{"value":"9","poly":"seven","nested":{"leaf":"gamma"},"items":[],"unsupported":{"inside":"x"}}}
{"id":4,"j":{"value":true,"nested":{},"unsupported":null}}
{"id":5,"j":{}}
;

SELECT 'dynamic direct', groupArray(id) FROM json_bf_dynamic_edges WHERE j.value = 7 SETTINGS force_data_skipping_indices = 'idx', dynamic_throw_on_type_mismatch = 0;
SELECT 'dynamic cast int', groupArray(id) FROM json_bf_dynamic_edges WHERE CAST(j.value AS Int64) = 7 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'dynamic cast float', groupArray(id) FROM json_bf_dynamic_edges WHERE CAST(j.value AS Float64) = 1.5 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'dynamic cast string', groupArray(id) FROM json_bf_dynamic_edges WHERE CAST(j.value AS String) = '9' SETTINGS force_data_skipping_indices = 'idx';
SELECT 'dynamic cast bool string', groupArray(id) FROM json_bf_dynamic_edges WHERE CAST(j.value AS String) = 'true' SETTINGS force_data_skipping_indices = 'idx';
SELECT 'dynamic bool string direct', groupArray(id) FROM json_bf_dynamic_edges WHERE j.bool_value = 'true' SETTINGS force_data_skipping_indices = 'idx';
SELECT 'dynamic numeric inexact', groupArray(id) FROM json_bf_dynamic_edges WHERE j.value = 7.5 SETTINGS force_data_skipping_indices = 'idx', dynamic_throw_on_type_mismatch = 0;
SELECT count() FROM json_bf_dynamic_edges WHERE j.value = 7 SETTINGS force_data_skipping_indices = 'idx'; -- { serverError NO_COMMON_TYPE }
SELECT count() FROM json_bf_dynamic_edges WHERE j.value = 'x' SETTINGS force_data_skipping_indices = 'idx'; -- { serverError TYPE_MISMATCH }
SELECT 'dynamic negative zero', groupArray(id) FROM json_bf_dynamic_edges WHERE j.zero = 0.0 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'scalar role', groupArray(id) FROM json_bf_dynamic_edges WHERE j.poly = 7 SETTINGS force_data_skipping_indices = 'idx', dynamic_throw_on_type_mismatch = 0;
SELECT 'nested path', groupArray(id) FROM json_bf_dynamic_edges WHERE j.nested.leaf = 'beta' SETTINGS force_data_skipping_indices = 'idx';
SELECT 'array json leaf', groupArray(id) FROM json_bf_dynamic_edges WHERE has(j.items[].leaf, toInt64(12)) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'dynamic ngram', groupArray(id) FROM json_bf_dynamic_edges WHERE j.nested.leaf LIKE '%amm%' SETTINGS force_data_skipping_indices = 'idx';
SELECT count() FROM json_bf_dynamic_edges WHERE j.value LIKE '%foo%' SETTINGS force_data_skipping_indices = 'idx'; -- { serverError ILLEGAL_TYPE_OF_ARGUMENT }
SELECT count() FROM json_bf_dynamic_edges WHERE multiSearchAny(j.value, ['foo']) SETTINGS force_data_skipping_indices = 'idx'; -- { serverError ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER }
SELECT 'unsupported dynamic is conservative', groupArray(id) FROM json_bf_dynamic_edges WHERE j.value = 999 SETTINGS force_data_skipping_indices = 'idx', dynamic_throw_on_type_mismatch = 0;

CREATE TABLE json_bf_shared_edges
(
    id UInt64,
    j JSON(max_dynamic_paths = 0, max_dynamic_types = 0),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001, tokenizer = ngrams(3)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_shared_edges VALUES (1, '{"i":-1,"s":"shared-alpha","arr":[1,2],"nested":{"x":10}}');
INSERT INTO json_bf_shared_edges VALUES (2, '{"i":18446744073709551615,"s":"shared-beta","arr":[3,4],"nested":{"x":20}}');
INSERT INTO json_bf_shared_edges VALUES (3, '{}');

SELECT 'shared signed', groupArray(id) FROM json_bf_shared_edges WHERE j.i = -1 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'shared unsigned', groupArray(id) FROM json_bf_shared_edges WHERE j.i = 18446744073709551615 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'shared string', groupArray(id) FROM json_bf_shared_edges WHERE j.s = 'shared-beta' SETTINGS force_data_skipping_indices = 'idx';
SELECT 'shared nested', groupArray(id) FROM json_bf_shared_edges WHERE j.nested.x = 10 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'shared ngram', groupArray(id) FROM json_bf_shared_edges WHERE j.s LIKE '%alpha%' SETTINGS force_data_skipping_indices = 'idx';

OPTIMIZE TABLE json_bf_shared_edges FINAL;
SELECT 'after merge', groupArray(id) FROM json_bf_shared_edges WHERE j.nested.x = 20 SETTINGS force_data_skipping_indices = 'idx';

CREATE TABLE json_bf_materialize
(
    id UInt64,
    j JSON
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 2;

INSERT INTO json_bf_materialize VALUES (1, '{"v":"before-one"}');
INSERT INTO json_bf_materialize VALUES (2, '{"v":"before-two"}');
ALTER TABLE json_bf_materialize ADD INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.0001) GRANULARITY 2;
ALTER TABLE json_bf_materialize MATERIALIZE INDEX idx SETTINGS mutations_sync = 2;
SELECT 'materialized index', groupArray(id) FROM json_bf_materialize WHERE j.v = 'before-two' SETTINGS force_data_skipping_indices = 'idx';
ALTER TABLE json_bf_materialize UPDATE j = '{"v":"after-two"}'::JSON WHERE id = 2 SETTINGS mutations_sync = 2;
SELECT 'mutation rebuilt index', groupArray(id) FROM json_bf_materialize WHERE j.v = 'after-two' SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_bf_edges;
DROP TABLE json_bf_dynamic_edges;
DROP TABLE json_bf_shared_edges;
DROP TABLE json_bf_materialize;
