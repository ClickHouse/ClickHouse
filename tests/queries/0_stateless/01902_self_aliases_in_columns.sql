CREATE TABLE a
(
    `number` UInt64,
    `x` MATERIALIZED x
)
ENGINE = MergeTree
ORDER BY number; --{ serverError CYCLIC_ALIASES}

CREATE TABLE foo
(
    i Int32,
    j ALIAS j + 1
)
ENGINE = MergeTree() ORDER BY i; --{ serverError CYCLIC_ALIASES}
