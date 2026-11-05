-- { echo }

-- Bool IN operator should use exact value semantics: only 0 and 1 are
-- representable as Bool, so other numeric values must not match.

-- Bool true with exact and non-exact values
SELECT CAST(1, 'Bool') IN (1);       -- 1: exact match
SELECT CAST(1, 'Bool') IN (0);       -- 0: different value
SELECT CAST(1, 'Bool') IN (10);      -- 0: 10 is not representable as Bool
SELECT CAST(1, 'Bool') IN (255);     -- 0: not representable
SELECT CAST(1, 'Bool') IN (256);     -- 0: not representable
SELECT CAST(1, 'Bool') IN (10000);   -- 0: not representable

-- Bool false with exact and non-exact values
SELECT CAST(0, 'Bool') IN (0);       -- 1: exact match
SELECT CAST(0, 'Bool') IN (1);       -- 0: different value
SELECT CAST(0, 'Bool') IN (10);      -- 0: not representable (UInt8)
SELECT CAST(0, 'Bool') IN (255);     -- 0: not representable (UInt8)
SELECT CAST(0, 'Bool') IN (256);     -- 0: not representable (UInt16)
SELECT CAST(0, 'Bool') IN (10000);   -- 0: not representable (UInt16)

-- Bool with multiple values in IN
SELECT CAST(1, 'Bool') IN (0, 1);    -- 1: 1 is in set
SELECT CAST(1, 'Bool') IN (0, 10);     -- 0: neither 0 nor 10 match true(1)
SELECT CAST(1, 'Bool') IN (0, 256);   -- 0: neither match (UInt16)
SELECT CAST(0, 'Bool') IN (0, 10);    -- 1: 0 is in set
SELECT CAST(0, 'Bool') IN (10, 255);  -- 0: neither representable as Bool
SELECT CAST(0, 'Bool') IN (256, 512); -- 0: neither representable (UInt16)

-- Bool with negative numbers
SELECT CAST(1, 'Bool') IN (-1);      -- 0: -1 is not representable as Bool
SELECT CAST(0, 'Bool') IN (-1);      -- 0: not representable

-- Bool with materialized values
SELECT CAST(materialize(1), 'Bool') IN (1);    -- 1: exact match
SELECT CAST(materialize(1), 'Bool') IN (10);    -- 0: not representable (UInt8)
SELECT CAST(materialize(1), 'Bool') IN (256);   -- 0: not representable (UInt16)
SELECT CAST(materialize(0), 'Bool') IN (0);     -- 1: exact match
SELECT CAST(materialize(0), 'Bool') IN (10);    -- 0: not representable (UInt8)
SELECT CAST(materialize(0), 'Bool') IN (256);   -- 0: not representable (UInt16)

-- Nullable(Bool) with IN operator
SELECT CAST(1, 'Nullable(Bool)') IN (toNullable(1));     -- 1: exact match
SELECT CAST(1, 'Nullable(Bool)') IN (toNullable(10));    -- 0: not representable (UInt8)
SELECT CAST(1, 'Nullable(Bool)') IN (toNullable(256));   -- 0: not representable (UInt16)
SELECT CAST(0, 'Nullable(Bool)') IN (toNullable(0));     -- 1: exact match
SELECT CAST(0, 'Nullable(Bool)') IN (toNullable(10));    -- 0: not representable (UInt8)
SELECT CAST(0, 'Nullable(Bool)') IN (toNullable(256));   -- 0: not representable (UInt16)

-- LowCardinality(Bool) with IN operator
SELECT toLowCardinality(CAST(1, 'Bool')) IN (1);    -- 1: exact match
SELECT toLowCardinality(CAST(1, 'Bool')) IN (10);    -- 0: not representable (UInt8)
SELECT toLowCardinality(CAST(1, 'Bool')) IN (256);   -- 0: not representable (UInt16)
SELECT toLowCardinality(CAST(0, 'Bool')) IN (0);     -- 1: exact match
SELECT toLowCardinality(CAST(0, 'Bool')) IN (10);    -- 0: not representable (UInt8)
SELECT toLowCardinality(CAST(0, 'Bool')) IN (256);   -- 0: not representable (UInt16)

-- LowCardinality(Nullable(Bool)) with IN operator
SELECT toLowCardinality(CAST(1, 'Nullable(Bool)')) IN (1);   -- 1: exact match
SELECT toLowCardinality(CAST(1, 'Nullable(Bool)')) IN (10);   -- 0: not representable (UInt8)
SELECT toLowCardinality(CAST(1, 'Nullable(Bool)')) IN (256);  -- 0: not representable (UInt16)
SELECT toLowCardinality(CAST(0, 'Nullable(Bool)')) IN (0);    -- 1: exact match
SELECT toLowCardinality(CAST(0, 'Nullable(Bool)')) IN (10);   -- 0: not representable (UInt8)
SELECT toLowCardinality(CAST(0, 'Nullable(Bool)')) IN (256);  -- 0: not representable (UInt16)

-- Array(Bool) with IN operator
SELECT CAST([1, 0], 'Array(Bool)') IN ([1, 0]);     -- 1: exact match
SELECT CAST([1, 0], 'Array(Bool)') IN ([10, 0]);     -- 0: 10 not representable (UInt8)
SELECT CAST([1, 0], 'Array(Bool)') IN ([256, 0]);   -- 0: 256 not representable (UInt16)
SELECT CAST([1, 0], 'Array(Bool)') IN ([1, 1]);     -- 0: second element differs
SELECT CAST([0, 1], 'Array(Bool)') IN ([10, 1]);   -- 0: 10 not representable (UInt8)
SELECT CAST([0, 1], 'Array(Bool)') IN ([256, 1]);  -- 0: 256 not representable (UInt16)

-- Tuple with Bool with IN operator
SELECT CAST((1, 0), 'Tuple(Bool, Bool)') IN ((1, 0));   -- 1: exact match
SELECT CAST((1, 0), 'Tuple(Bool, Bool)') IN ((10, 0));   -- 0: 10 not representable (UInt8)
SELECT CAST((1, 0), 'Tuple(Bool, Bool)') IN ((256, 0));  -- 0: 256 not representable (UInt16)
SELECT CAST((1, 0), 'Tuple(Bool, Bool)') IN ((1, 1));    -- 0: second element differs
SELECT CAST((0, 1), 'Tuple(Bool, Bool)') IN ((10, 1));   -- 0: 10 not representable (UInt8)
SELECT CAST((0, 1), 'Tuple(Bool, Bool)') IN ((256, 1));  -- 0: 256 not representable (UInt16)

-- Nested Tuple with Bool with IN operator
SELECT CAST((1, (0, 1)), 'Tuple(Bool, Tuple(Bool, Bool))') IN ((1, (0, 1)));   -- 1: exact match
SELECT CAST((1, (0, 1)), 'Tuple(Bool, Tuple(Bool, Bool))') IN ((10, (0, 1)));   -- 0: 10 not representable (UInt8)
SELECT CAST((1, (0, 1)), 'Tuple(Bool, Tuple(Bool, Bool))') IN ((256, (0, 1))); -- 0: 256 not representable (UInt16)
SELECT CAST((1, (0, 1)), 'Tuple(Bool, Tuple(Bool, Bool))') IN ((1, (0, 10)));  -- 0: nested 10 not representable (UInt8)
SELECT CAST((1, (0, 1)), 'Tuple(Bool, Tuple(Bool, Bool))') IN ((1, (0, 256))); -- 0: nested 256 not representable (UInt16)
SELECT CAST((0, (1, 0)), 'Tuple(Bool, Tuple(Bool, Bool))') IN ((10, (1, 0))); -- 0: 10 not representable (UInt8)
SELECT CAST((0, (1, 0)), 'Tuple(Bool, Tuple(Bool, Bool))') IN ((256, (1, 0))); -- 0: 256 not representable (UInt16)

-- Map(String, Bool) with IN operator
SELECT CAST(map('a', 1, 'b', 0), 'Map(String, Bool)') IN (map('a', 1, 'b', 0));   -- 1: exact match
SELECT CAST(map('a', 1, 'b', 0), 'Map(String, Bool)') IN (map('a', 10, 'b', 0));   -- 0: 10 not representable (UInt8)
SELECT CAST(map('a', 1, 'b', 0), 'Map(String, Bool)') IN (map('a', 256, 'b', 0)); -- 0: 256 not representable (UInt16)
SELECT CAST(map('a', 0, 'b', 1), 'Map(String, Bool)') IN (map('a', 10, 'b', 1));  -- 0: 10 not representable (UInt8)
SELECT CAST(map('a', 0, 'b', 1), 'Map(String, Bool)') IN (map('a', 256, 'b', 1)); -- 0: 256 not representable (UInt16)

-- Table-based tests with Bool column
DROP TABLE IF EXISTS test_bool_in;
CREATE TABLE test_bool_in (b Bool) ENGINE = MergeTree ORDER BY b;
INSERT INTO test_bool_in VALUES (true), (false);
SELECT b FROM test_bool_in WHERE b IN (10);               -- empty: 10 not representable
SELECT b FROM test_bool_in WHERE b IN (1);                 -- true
SELECT b FROM test_bool_in WHERE b IN (0, 1) ORDER BY b;  -- false, true
SELECT b FROM test_bool_in WHERE b IN (SELECT 10);         -- empty: subquery 10 not representable
DROP TABLE test_bool_in;

-- Subquery as RHS
SELECT CAST(1, 'Bool') IN (SELECT 10);    -- 0: not representable (UInt8)
SELECT CAST(1, 'Bool') IN (SELECT 256);   -- 0: not representable (UInt16)
SELECT CAST(1, 'Bool') IN (SELECT 1);     -- 1: exact match
SELECT CAST(0, 'Bool') IN (SELECT 10);    -- 0: not representable (UInt8)
SELECT CAST(0, 'Bool') IN (SELECT 256);   -- 0: not representable (UInt16)
SELECT CAST(0, 'Bool') IN (SELECT 0);     -- 1: exact match

-- Float values on RHS
SELECT CAST(1, 'Bool') IN (1.0);     -- 1: 1.0 matches true
SELECT CAST(1, 'Bool') IN (10.5);    -- 0: not representable
SELECT CAST(1, 'Bool') IN (256.0);   -- 0: not representable (UInt16 range)
SELECT CAST(0, 'Bool') IN (0.0);     -- 1: 0.0 matches false
SELECT CAST(0, 'Bool') IN (10.5);    -- 0: not representable
SELECT CAST(0, 'Bool') IN (256.0);   -- 0: not representable (UInt16 range)

-- String values on RHS
SELECT CAST(1, 'Bool') IN ('true');   -- 1: parsed as Bool true
SELECT CAST(1, 'Bool') IN ('1');      -- 1: parsed as Bool true
SELECT CAST(0, 'Bool') IN ('false');  -- 1: parsed as Bool false
SELECT CAST(0, 'Bool') IN ('0');      -- 1: parsed as Bool false

-- NULL handling
SELECT CAST(1, 'Bool') IN (NULL);                              -- 0
SELECT CAST(0, 'Bool') IN (NULL);                              -- 0
SELECT CAST(1, 'Nullable(Bool)') IN (toNullable(1), NULL);     -- 1: 1 matches
SELECT CAST(1, 'Nullable(Bool)') IN (toNullable(10), NULL);    -- 0: 10 not representable (UInt8)
SELECT CAST(1, 'Nullable(Bool)') IN (toNullable(256), NULL);   -- 0: 256 not representable (UInt16)
SELECT CAST(0, 'Nullable(Bool)') IN (toNullable(0), NULL);     -- 1: 0 matches
SELECT CAST(0, 'Nullable(Bool)') IN (toNullable(10), NULL);    -- 0: 10 not representable (UInt8)
SELECT CAST(0, 'Nullable(Bool)') IN (toNullable(256), NULL);   -- 0: 256 not representable (UInt16)

-- NOT IN
SELECT CAST(1, 'Bool') NOT IN (10);     -- 1: 10 not representable (UInt8)
SELECT CAST(1, 'Bool') NOT IN (256);    -- 1: 256 not representable (UInt16)
SELECT CAST(1, 'Bool') NOT IN (1);      -- 0: exact match
SELECT CAST(1, 'Bool') NOT IN (0, 10);  -- 1: neither matches true
SELECT CAST(1, 'Bool') NOT IN (0, 256); -- 1: neither matches true (UInt16)
SELECT CAST(0, 'Bool') NOT IN (10);     -- 1: 10 not representable (UInt8)
SELECT CAST(0, 'Bool') NOT IN (256);    -- 1: 256 not representable (UInt16)
SELECT CAST(0, 'Bool') NOT IN (0);      -- 0: exact match
SELECT CAST(0, 'Bool') NOT IN (1, 10);  -- 1: neither matches false
SELECT CAST(0, 'Bool') NOT IN (1, 256); -- 1: neither matches false (UInt16)

-- Bool-to-Bool RHS (already clamped)
SELECT CAST(1, 'Bool') IN (CAST(10, 'Bool'));    -- 1: RHS is already true after CAST
SELECT CAST(0, 'Bool') IN (CAST(10, 'Bool'));    -- 0: RHS is true, LHS is false
SELECT CAST(0, 'Bool') IN (CAST(0, 'Bool'));     -- 1: both false

-- Mixed valid and invalid values in IN
SELECT CAST(1, 'Bool') IN (0, 1, 10, 256);    -- 1: 1 matches true
SELECT CAST(1, 'Bool') IN (0, 10, 256);        -- 0: no valid match for true
SELECT CAST(0, 'Bool') IN (0, 1, 10, 256);    -- 1: 0 matches false
SELECT CAST(0, 'Bool') IN (1, 10, 256);        -- 0: no valid match for false

-- Int64 values (positive 0/1 must match, negative and other values must not)
SELECT CAST(1, 'Bool') IN (CAST(1, 'Int64'));    -- 1: exact match
SELECT CAST(1, 'Bool') IN (CAST(0, 'Int64'));    -- 0: different value
SELECT CAST(0, 'Bool') IN (CAST(0, 'Int64'));    -- 1: exact match
SELECT CAST(0, 'Bool') IN (CAST(1, 'Int64'));    -- 0: different value
SELECT CAST(1, 'Bool') IN (-1);      -- 0: not representable
SELECT CAST(0, 'Bool') IN (-1);      -- 0: not representable
SELECT CAST(1, 'Bool') IN (-256);    -- 0: not representable
SELECT CAST(0, 'Bool') IN (-256);    -- 0: not representable

-- Float values
SELECT CAST(1, 'Bool') IN (1.0);     -- 1: exact match
SELECT CAST(0, 'Bool') IN (0.0);     -- 1: exact match
SELECT CAST(1, 'Bool') IN (0.5);     -- 0: not representable
SELECT CAST(1, 'Bool') IN (-1.0);    -- 0: not representable
SELECT CAST(0, 'Bool') IN (-256.0);  -- 0: not representable
