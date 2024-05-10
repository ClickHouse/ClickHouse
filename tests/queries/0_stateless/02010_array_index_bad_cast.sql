-- This query throws exception about uncomparable data types (but at least it does not introduce bad cast in code).
SELECT has(materialize(CAST(['2021-07-14'] AS Array(LowCardinality(Nullable(DateTime))))), materialize('2021-07-14'::DateTime64(7))); -- { serverError 44 }
