SELECT toTypeName(groupUniqArrayUpTo(3)(number)) FROM numbers(0);

WITH groupUniqArrayUpTo(3)(number % 3) AS result
SELECT arraySort(result.values), result.overflowed FROM numbers(10);
SELECT groupUniqArrayUpTo(3)(number) FROM numbers(4);
SELECT groupUniqArrayUpTo(3)(number) FROM numbers(0);
SELECT groupUniqArrayUpTo(3)(x) FROM values('x Nullable(UInt64)', NULL, NULL);
SELECT groupUniqArray(x) FROM values('x Nullable(UInt64)', NULL, NULL);
SELECT length(hex(groupUniqArrayState(x))) FROM values('x Nullable(UInt64)', NULL, NULL);
SELECT groupUniqArrayUpToDistinct(3)(x) FROM values('x Nullable(UInt64)', NULL, NULL);
SELECT groupUniqArrayUpToOrDefault(3)(x) FROM values('x Nullable(UInt64)', NULL, NULL);
SELECT groupUniqArrayUpToOrDefaultDistinct(3)(x) FROM values('x Nullable(UInt64)', NULL, NULL);
SELECT groupUniqArrayUpToDistinctOrDefault(3)(x) FROM values('x Nullable(UInt64)', NULL, NULL);
SELECT groupUniqArrayUpTo(3)(x), groupUniqArray(x) FROM values('x Nullable(String)', NULL, NULL);

WITH groupUniqArrayUpTo(3)(toLowCardinality(toUInt128(number % 3))) AS result
SELECT arraySort(result.values), result.overflowed FROM numbers(10);
SELECT groupUniqArrayUpTo(3)(toLowCardinality(toUInt128(number))) FROM numbers(4);

WITH groupUniqArrayUpTo(3)(toString(number % 3)) AS result
SELECT arraySort(result.values), result.overflowed FROM numbers(10);
SELECT groupUniqArrayUpTo(3)(toString(number)) FROM numbers(4);
WITH groupUniqArrayUpTo(3)(repeat(toString(number % 3), 40)) AS result
SELECT arraySort(arrayMap(x -> length(x), result.values)), result.overflowed FROM numbers(10);
SELECT groupUniqArrayUpTo(3)(repeat(toString(number), 40)) FROM numbers(4);
WITH groupUniqArrayUpTo(3)(tuple(toString(number % 3), number % 3)) AS result
SELECT arraySort(result.values), result.overflowed FROM numbers(10);
SELECT groupUniqArrayUpTo(3)(tuple(toString(number), number)) FROM numbers(4);

-- An overflowed state contains only its one-byte overflow marker.
SELECT length(hex(groupUniqArrayUpToState(3)(toUInt128(number)))) FROM numbers(4);

DROP TABLE IF EXISTS group_uniq_array_up_to;
CREATE TABLE group_uniq_array_up_to
(
    state AggregateFunction(groupUniqArrayUpTo(3), UInt128)
)
ENGINE = AggregatingMergeTree
ORDER BY tuple();

INSERT INTO group_uniq_array_up_to SELECT groupUniqArrayUpToState(3)(toUInt128(number)) FROM numbers(2);
INSERT INTO group_uniq_array_up_to SELECT groupUniqArrayUpToState(3)(toUInt128(number + 1)) FROM numbers(2);
SELECT arraySort(result.values), result.overflowed
FROM
(
    SELECT groupUniqArrayUpToMerge(3)(state) AS result
    FROM group_uniq_array_up_to
);

-- Overflow while merging two non-overflowed states is absorbing.
INSERT INTO group_uniq_array_up_to SELECT groupUniqArrayUpToState(3)(toUInt128(3));
SELECT groupUniqArrayUpToMerge(3)(state) FROM group_uniq_array_up_to;

TRUNCATE TABLE group_uniq_array_up_to;

-- A state which overflowed before serialization remains overflowed after deserialization and merging.
INSERT INTO group_uniq_array_up_to SELECT groupUniqArrayUpToState(3)(toUInt128(number)) FROM numbers(4);
INSERT INTO group_uniq_array_up_to SELECT groupUniqArrayUpToState(3)(toUInt128(100));
SELECT groupUniqArrayUpToMerge(3)(state) FROM group_uniq_array_up_to;

DROP TABLE group_uniq_array_up_to;

CREATE TABLE group_uniq_array_up_to
(
    state AggregateFunction(groupUniqArrayUpTo(3), String)
)
ENGINE = AggregatingMergeTree
ORDER BY tuple();

INSERT INTO group_uniq_array_up_to SELECT groupUniqArrayUpToState(3)(repeat(toString(number), 40)) FROM numbers(2);
INSERT INTO group_uniq_array_up_to SELECT groupUniqArrayUpToState(3)(repeat(toString(number + 1), 40)) FROM numbers(2);
SELECT arraySort(arrayMap(x -> length(x), result.values)), result.overflowed
FROM
(
    SELECT groupUniqArrayUpToMerge(3)(state) AS result
    FROM group_uniq_array_up_to
);

INSERT INTO group_uniq_array_up_to SELECT groupUniqArrayUpToState(3)(repeat(toString(3), 40));
SELECT groupUniqArrayUpToMerge(3)(state) FROM group_uniq_array_up_to;

DROP TABLE group_uniq_array_up_to;

SELECT groupUniqArrayUpTo(number) FROM numbers(1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
SELECT groupUniqArrayUpTo(0)(number) FROM numbers(1); -- { serverError BAD_ARGUMENTS }
SELECT groupUniqArrayUpTo(1, 2)(number) FROM numbers(1); -- { serverError NUMBER_OF_ARGUMENTS_DOESNT_MATCH }
