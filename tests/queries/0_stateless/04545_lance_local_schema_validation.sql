DROP TABLE IF EXISTS lance_local_explicit;
DROP TABLE IF EXISTS lance_local_mismatch;

CREATE TABLE lance_local_explicit
(
    id UInt64,
    name String,
    score Nullable(Int64)
)
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/basic.lance');

SELECT id, name, score FROM lance_local_explicit ORDER BY id;

CREATE TABLE lance_local_mismatch
(
    id String,
    name String,
    score Nullable(Int64)
)
ENGINE = LanceLocal('tests/queries/0_stateless/data_lance/basic.lance'); -- { serverError BAD_ARGUMENTS }

DROP TABLE lance_local_explicit;
