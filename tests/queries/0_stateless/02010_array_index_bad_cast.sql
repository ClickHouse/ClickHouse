SET allow_suspicious_low_cardinality_types=1;
SELECT has(materialize(CAST(['2021-07-14'] AS Array(LowCardinality(Nullable(DateTime))))), materialize('2021-07-14'::DateTime64(7)));
SELECT has(materialize(CAST(['2021-07-14'] AS Array(LowCardinality(Nullable(DateTime))))), materialize('2021-07-14 00:00:01'::DateTime64(7)));
SELECT has(materialize(CAST(['2021-07-14'] AS Array(LowCardinality(Nullable(DateTime))))), materialize(NULL));
