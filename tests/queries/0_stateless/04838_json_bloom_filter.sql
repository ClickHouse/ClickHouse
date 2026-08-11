DROP TABLE IF EXISTS json_bf;
DROP TABLE IF EXISTS json_bf_shared;

CREATE TABLE json_bf
(
    id UInt64,
    j JSON(
        d Decimal64(2),
        dt DateTime64(9, 'UTC'),
        arr Array(Nullable(Int64)),
        mixed Array(Dynamic),
        m Map(String, Int64),
        t Tuple(x Int64, y String)),
    INDEX idx j TYPE jsonbf_v1(false_positive_rate = 0.01, tokenizer = ngrams(3)) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf FORMAT JSONEachRow
{"id":1,"j":{"i":1,"s":"alpha","b":true,"nested":{"x":10},"arr":[1,2],"mixed":[1,"one"],"items":[{"x":11},{"x":12}],"d":1.25,"dt":"1970-01-01 00:00:01.500000000","m":{"k":5},"t":{"x":7,"y":"seven"}}}
{"id":2,"j":{"i":2,"s":"beta","b":false,"nested":{"x":20},"arr":[3,4],"mixed":[2,"two"],"items":[{"x":21}],"d":2.50,"dt":"1970-01-01 00:00:02.500000000","m":{"k":6},"t":{"x":8,"y":"eight"}}}
{"id":3,"j":{}}
;

SELECT 'dynamic integer', count() FROM json_bf WHERE j.i = 2 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'dynamic string', count() FROM json_bf WHERE j.s = 'alpha' SETTINGS force_data_skipping_indices = 'idx';
SELECT 'dynamic bool string', count() FROM json_bf WHERE j.b = 'true' SETTINGS force_data_skipping_indices = 'idx';
SELECT 'nested object', count() FROM json_bf WHERE j.nested.x = 20 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'scalar array', count() FROM json_bf WHERE has(j.arr, 4) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'dynamic array', count() FROM json_bf WHERE has(j.mixed, 'two') SETTINGS force_data_skipping_indices = 'idx';
SELECT 'array json', count() FROM json_bf WHERE has(j.items[].x, toInt64(12)) SETTINGS force_data_skipping_indices = 'idx';
SELECT 'map value', count() FROM json_bf WHERE j.m['k'] = 5 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'tuple value', count() FROM json_bf WHERE j.t.x = 7 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'decimal float', count() FROM json_bf WHERE j.d = 1.25;
SELECT 'ngram like', count() FROM json_bf WHERE j.s LIKE '%pha%' SETTINGS force_data_skipping_indices = 'idx';
SELECT 'ngram starts', count() FROM json_bf WHERE startsWith(j.s, 'alp') SETTINGS force_data_skipping_indices = 'idx';

CREATE TABLE json_bf_shared
(
    id UInt64,
    j JSON(max_dynamic_paths = 0),
    INDEX idx j TYPE jsonbf_v1() GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_shared VALUES (1, '{"i":1,"s":"one"}'), (2, '{"i":2,"s":"two"}'), (3, '{}');
SELECT 'shared integer', count() FROM json_bf_shared WHERE j.i = 2 SETTINGS force_data_skipping_indices = 'idx';
SELECT 'shared string', count() FROM json_bf_shared WHERE j.s = 'one' SETTINGS force_data_skipping_indices = 'idx';

DROP TABLE json_bf;
DROP TABLE json_bf_shared;
