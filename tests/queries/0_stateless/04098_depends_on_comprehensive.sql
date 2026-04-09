-- PR 6: Comprehensive ORDER BY ... DEPENDS ON tests — DAG shapes, edge cases, interactions.

-- ============================================================
-- Basic topological ordering
-- ============================================================

-- Linear chain: input in reverse, output must be A, B, C
SELECT x FROM (
    SELECT 'C' AS x, ['B'] AS deps
    UNION ALL SELECT 'B', ['A']
    UNION ALL SELECT 'A', []
)
ORDER BY x DEPENDS ON deps;

-- Forward input order also produces A, B, C
SELECT x FROM (
    SELECT 'A' AS x, [] :: Array(String) AS deps
    UNION ALL SELECT 'B', ['A']
    UNION ALL SELECT 'C', ['B']
)
ORDER BY x DEPENDS ON deps;

-- ============================================================
-- DAG shapes
-- ============================================================

-- Diamond: A → B, A → C, B → D, C → D.
-- A must be first, D must be last.
SELECT arr[1] AS first_val, arr[length(arr)] AS last_val
FROM (
    SELECT groupArray(x) AS arr FROM (
        SELECT x FROM (
            SELECT 'D' AS x, ['B', 'C'] AS deps
            UNION ALL SELECT 'B', ['A']
            UNION ALL SELECT 'C', ['A']
            UNION ALL SELECT 'A', []
        )
        ORDER BY x DEPENDS ON deps
    )
);

-- Wide fan-in: D depends on A, B, C (all independent). D must be last.
SELECT arr[length(arr)] AS last_val
FROM (
    SELECT groupArray(x) AS arr FROM (
        SELECT x FROM (
            SELECT 'D' AS x, ['A', 'B', 'C'] AS deps
            UNION ALL SELECT 'A', [] :: Array(String)
            UNION ALL SELECT 'B', []
            UNION ALL SELECT 'C', []
        )
        ORDER BY x DEPENDS ON deps
    )
);

-- Wide fan-out: A → B, A → C, A → D. A must be first.
SELECT arr[1] AS first_val
FROM (
    SELECT groupArray(x) AS arr FROM (
        SELECT x FROM (
            SELECT 'A' AS x, [] :: Array(String) AS deps
            UNION ALL SELECT 'B', ['A']
            UNION ALL SELECT 'C', ['A']
            UNION ALL SELECT 'D', ['A']
        )
        ORDER BY x DEPENDS ON deps
    )
);

-- Forest (two disconnected chains): A→B and C→D.
-- Verify A before B and C before D.
SELECT
    indexOf(arr, 'A') < indexOf(arr, 'B') AS a_before_b,
    indexOf(arr, 'C') < indexOf(arr, 'D') AS c_before_d
FROM (
    SELECT groupArray(x) AS arr FROM (
        SELECT x FROM (
            SELECT 'B' AS x, ['A'] AS deps
            UNION ALL SELECT 'A', []
            UNION ALL SELECT 'D', ['C']
            UNION ALL SELECT 'C', []
        )
        ORDER BY x DEPENDS ON deps
    )
);

-- Long chain of 10 elements (unique topological order)
SELECT groupArray(x) FROM (
    SELECT x FROM (
        SELECT 'j' AS x, ['i'] AS deps
        UNION ALL SELECT 'i', ['h']
        UNION ALL SELECT 'h', ['g']
        UNION ALL SELECT 'g', ['f']
        UNION ALL SELECT 'f', ['e']
        UNION ALL SELECT 'e', ['d']
        UNION ALL SELECT 'd', ['c']
        UNION ALL SELECT 'c', ['b']
        UNION ALL SELECT 'b', ['a']
        UNION ALL SELECT 'a', []
    )
    ORDER BY x DEPENDS ON deps
);

-- ============================================================
-- Different key data types
-- ============================================================

-- Integer keys (unique chain: 1 → 2 → 3)
SELECT x FROM (
    SELECT 3 AS x, [2] AS d
    UNION ALL SELECT 2, [1]
    UNION ALL SELECT 1, [] :: Array(UInt8)
)
ORDER BY x DEPENDS ON d;

-- Large UInt64 keys
SELECT x FROM (
    SELECT toUInt64(1000000) AS x, [toUInt64(999999)] AS d
    UNION ALL SELECT toUInt64(999999), [] :: Array(UInt64)
)
ORDER BY x DEPENDS ON d;

-- ============================================================
-- Edge cases
-- ============================================================

-- Empty input
SELECT count() FROM (
    SELECT x FROM (
        SELECT 'A' AS x, [] :: Array(String) AS deps WHERE 0
    )
    ORDER BY x DEPENDS ON deps
);

-- Single row, no deps
SELECT x FROM (SELECT 'only' AS x, [] :: Array(String) AS deps)
ORDER BY x DEPENDS ON deps;

-- Single row, dep not in result set (ignored)
SELECT x FROM (SELECT 'A' AS x, ['Z'] AS deps)
ORDER BY x DEPENDS ON deps;

-- All rows have empty deps (all independent, all emitted)
SELECT count() FROM (
    SELECT x FROM (
        SELECT 'C' AS x, [] :: Array(String) AS deps
        UNION ALL SELECT 'A', []
        UNION ALL SELECT 'B', []
    )
    ORDER BY x DEPENDS ON deps
);

-- Dep references key not in result set — ignored, both rows emitted
SELECT count() FROM (
    SELECT x FROM (
        SELECT 'A' AS x, ['Z'] AS deps
        UNION ALL SELECT 'B', []
    )
    ORDER BY x DEPENDS ON deps
);

-- Duplicate keys with same deps (multi-row per key node)
-- A appears twice, B once. Both A copies must precede B.
SELECT
    indexOf(arr, 'B') > 2 AS b_after_both_a
FROM (
    SELECT groupArray(x) AS arr FROM (
        SELECT x FROM (
            SELECT 'B' AS x, ['A'] AS deps
            UNION ALL SELECT 'A', []
            UNION ALL SELECT 'A', []
        )
        ORDER BY x DEPENDS ON deps
    )
);

-- NULL in deps array (NULL dep ignored)
SELECT x FROM (
    SELECT 'B' AS x, [NULL, 'A'] :: Array(Nullable(String)) AS deps
    UNION ALL SELECT 'A', [] :: Array(Nullable(String))
)
ORDER BY x DEPENDS ON deps;

-- ============================================================
-- Cycle detection
-- ============================================================

-- Simple cycle: A → B → A
SELECT x FROM (SELECT 'A' AS x, ['B'] AS deps UNION ALL SELECT 'B', ['A']) ORDER BY x DEPENDS ON deps; -- { serverError BAD_ARGUMENTS }

-- Self-dependency: A → A
SELECT x FROM (SELECT 'A' AS x, ['A'] AS deps) ORDER BY x DEPENDS ON deps; -- { serverError BAD_ARGUMENTS }

-- Longer cycle: A → B → C → A
SELECT x FROM (SELECT 'A' AS x, ['C'] AS deps UNION ALL SELECT 'B', ['A'] UNION ALL SELECT 'C', ['B']) ORDER BY x DEPENDS ON deps; -- { serverError BAD_ARGUMENTS }

-- ============================================================
-- Interactions with other clauses
-- ============================================================

-- DEPENDS ON + LIMIT (topological order then truncate: get first 2)
SELECT x FROM (
    SELECT 'C' AS x, ['B'] AS deps
    UNION ALL SELECT 'B', ['A']
    UNION ALL SELECT 'A', []
)
ORDER BY x DEPENDS ON deps
LIMIT 2;

-- DEPENDS ON + WHERE (filter then sort on remaining)
SELECT x FROM (
    SELECT x, deps FROM (
        SELECT 'C' AS x, ['B'] AS deps
        UNION ALL SELECT 'B', ['A']
        UNION ALL SELECT 'A', []
    )
    WHERE x != 'C'
)
ORDER BY x DEPENDS ON deps;

-- DEPENDS ON in subquery, outer has normal ORDER BY DESC
SELECT x FROM (
    SELECT x FROM (
        SELECT 'C' AS x, ['B'] AS deps
        UNION ALL SELECT 'B', ['A']
        UNION ALL SELECT 'A', []
    )
    ORDER BY x DEPENDS ON deps
)
ORDER BY x DESC;

-- ============================================================
-- Error paths
-- ============================================================

-- DEPENDS ON + ASC → syntax error
SELECT 1 AS x ORDER BY x ASC DEPENDS ON x; -- { serverError SYNTAX_ERROR }

-- DEPENDS ON + DESC → syntax error
SELECT 1 AS x ORDER BY x DESC DEPENDS ON x; -- { serverError SYNTAX_ERROR }

-- Multiple ORDER BY elements with DEPENDS ON → syntax error
SELECT 1 AS x, 2 AS y, [3] AS d ORDER BY x DEPENDS ON d, y; -- { serverError SYNTAX_ERROR }

-- Non-Array deps column → BAD_ARGUMENTS at runtime
SELECT x FROM (SELECT 1 AS x, 'not_array' AS deps) ORDER BY x DEPENDS ON deps; -- { serverError BAD_ARGUMENTS }

-- ============================================================
-- EXPLAIN shows TopologicalSort step
-- ============================================================

SELECT count() > 0 AS has_toposort
FROM (
    EXPLAIN PLAN SELECT x FROM (
        SELECT 'A' AS x, [] :: Array(String) AS deps
    )
    ORDER BY x DEPENDS ON deps
)
WHERE explain LIKE '%TopologicalSort%';
