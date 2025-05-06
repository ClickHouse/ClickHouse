EXPLAIN SYNTAX SELECT materialize(['a','b','c','d','e','f','g','h','i','j']) AS arr WHERE hasAny(arr, ['o']) OR hasAny(arr, ['u']) OR hasAny(arr, ['ą']) OR hasAny(arr, ['ę']) SETTINGS optimize_or_has_any_chain = 0;
EXPLAIN QUERY TREE run_passes=1 SELECT materialize(['a','b','c','d','e','f','g','h','i','j']) AS arr WHERE hasAny(arr, ['o']) OR hasAny(arr, ['u']) OR hasAny(arr, ['ą']) OR hasAny(arr, ['ę']) SETTINGS optimize_or_has_any_chain = 0, enable_analyzer = 1;

EXPLAIN SYNTAX SELECT materialize(['a','b','c','d','e','f','g','h','i','j']) AS arr WHERE hasAny(arr, ['o']) OR hasAny(arr, ['u']) OR hasAny(arr, ['ą']) OR hasAny(arr, ['ę']) SETTINGS optimize_or_has_any_chain = 1;
EXPLAIN QUERY TREE run_passes=1 SELECT materialize(['a','b','c','d','e','f','g','h','i','j']) AS arr WHERE hasAny(arr, ['o']) OR hasAny(arr, ['u']) OR hasAny(arr, ['ą']) OR hasAny(arr, ['ę']) SETTINGS optimize_or_has_any_chain = 1, enable_analyzer = 1;


DROP TABLE IF EXISTS t3448;
CREATE TABLE t3448 (id Int64, letters Array(String)) ENGINE = MergeTree() Order by id;

INSERT INTO t3448 VALUES
    (0, []),
    (1, ['a','b','c','d','e','f','g','h','i','j','k','l','m','n','o']),
    (2, ['a','i','u','e','o']),
    (3, ['ą','ę','ó','ł','ś','ż','ź']),
    (4, ['あ', 'い', 'う', 'え', 'お']),
    (5, ['ア', 'イ', 'ウ', 'エ', 'オ']),
    (6, ['i', 'é', 'è', 'ê', 'e', 'a', 'â', 'o', 'ô', 'u', 'ù', 'y', 'ë', 'œ', 'ø']),
    (7, ['i', 'n', 'e', 'd', 'i', 'b', 'l', 'e']),
    (8, ['z']),
    (9, ['d', 'e', 's', 'c', 'r', 'i', 'b', 'e']),
    (10, ['t', 'e', 's', 't']),
    (11, ['t', 'r', 's', 't']);
    ;

SELECT 'With optimize_or_has_any_chain=0, enable_analyzer=0';
SELECT id FROM t3448 WHERE hasAny(letters, ['a','i','u','e','o']) SETTINGS optimize_or_has_any_chain = 0, enable_analyzer = 0;
SELECT id FROM t3448 WHERE hasAny(letters, ['a']) OR hasAny(letters, ['i']) OR hasAny(letters, ['u']) OR hasAny(letters, ['e']) OR hasAny(letters, ['o']) SETTINGS optimize_or_has_any_chain = 0, enable_analyzer = 0;
SELECT id FROM t3448 WHERE hasAny(letters, ['a','i','u','e','o']) OR id = 1 OR id = 2 OR id = 3 OR id = 4 OR id = 5 SETTINGS optimize_or_has_any_chain = 0, enable_analyzer = 0;
SELECT id FROM t3448 WHERE hasAny(letters, ['a']) OR hasAny(letters, ['i']) OR hasAny(letters, ['u']) OR hasAny(letters, ['e']) OR hasAny(letters, ['o']) OR id = 1 OR id = 2 OR id = 3 OR id = 4 OR id = 5 SETTINGS optimize_or_has_any_chain = 0, enable_analyzer = 0;
SELECT 'With optimize_or_has_any_chain=0, enable_analyzer=1';
SELECT id FROM t3448 WHERE hasAny(letters, ['a','i','u','e','o']) SETTINGS optimize_or_has_any_chain = 0, enable_analyzer = 1;
SELECT id FROM t3448 WHERE hasAny(letters, ['a']) OR hasAny(letters, ['i']) OR hasAny(letters, ['u']) OR hasAny(letters, ['e']) OR hasAny(letters, ['o']) SETTINGS optimize_or_has_any_chain = 0, enable_analyzer = 1;
SELECT id FROM t3448 WHERE hasAny(letters, ['a','i','u','e','o']) OR id = 1 OR id = 2 OR id = 3 OR id = 4 OR id = 5 SETTINGS optimize_or_has_any_chain = 0, enable_analyzer = 1;
SELECT id FROM t3448 WHERE hasAny(letters, ['a']) OR hasAny(letters, ['i']) OR hasAny(letters, ['u']) OR hasAny(letters, ['e']) OR hasAny(letters, ['o']) OR id = 1 OR id = 2 OR id = 3 OR id = 4 OR id = 5 SETTINGS optimize_or_has_any_chain = 0, enable_analyzer = 1;
SELECT 'With optimize_or_has_any_chain=1, enable_analyzer=0';
SELECT id FROM t3448 WHERE hasAny(letters, ['a','i','u','e','o']) SETTINGS optimize_or_has_any_chain = 1, enable_analyzer = 0;
SELECT id FROM t3448 WHERE hasAny(letters, ['a']) OR hasAny(letters, ['i']) OR hasAny(letters, ['u']) OR hasAny(letters, ['e']) OR hasAny(letters, ['o']) SETTINGS optimize_or_has_any_chain = 1, enable_analyzer = 0;
SELECT id FROM t3448 WHERE hasAny(letters, ['a','i','u','e','o']) OR id = 1 OR id = 2 OR id = 3 OR id = 4 OR id = 5 SETTINGS optimize_or_has_any_chain = 1, enable_analyzer = 0;
SELECT id FROM t3448 WHERE hasAny(letters, ['a']) OR hasAny(letters, ['i']) OR hasAny(letters, ['u']) OR hasAny(letters, ['e']) OR hasAny(letters, ['o']) OR id = 1 OR id = 2 OR id = 3 OR id = 4 OR id = 5 SETTINGS optimize_or_has_any_chain = 1, enable_analyzer = 0;
SELECT 'With optimize_or_has_any_chain=1, enable_analyzer=1';
SELECT id FROM t3448 WHERE hasAny(letters, ['a','i','u','e','o']) SETTINGS optimize_or_has_any_chain = 1, enable_analyzer = 1;
SELECT id FROM t3448 WHERE hasAny(letters, ['a']) OR hasAny(letters, ['i']) OR hasAny(letters, ['u']) OR hasAny(letters, ['e']) OR hasAny(letters, ['o']) SETTINGS optimize_or_has_any_chain = 1, enable_analyzer = 1;
SELECT id FROM t3448 WHERE hasAny(letters, ['a','i','u','e','o']) OR id = 1 OR id = 2 OR id = 3 OR id = 4 OR id = 5 SETTINGS optimize_or_has_any_chain = 1, enable_analyzer = 1;
SELECT id FROM t3448 WHERE hasAny(letters, ['a']) OR hasAny(letters, ['i']) OR hasAny(letters, ['u']) OR hasAny(letters, ['e']) OR hasAny(letters, ['o']) OR id = 1 OR id = 2 OR id = 3 OR id = 4 OR id = 5 SETTINGS optimize_or_has_any_chain = 1, enable_analyzer = 1;
