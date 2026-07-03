-- Index arguments must be validated before the index is instantiated (issue #109287)
DROP TABLE IF EXISTS t_hyp_idx_args;
CREATE TABLE t_hyp_idx_args (c0 Int32, s String) ENGINE = MergeTree ORDER BY c0;

CREATE HYPOTHETICAL INDEX hi ON t_hyp_idx_args c0 TYPE set GRANULARITY 1; -- { serverError INCORRECT_QUERY }
CREATE HYPOTHETICAL INDEX hi ON t_hyp_idx_args c0 TYPE set('x') GRANULARITY 1; -- { serverError INCORRECT_QUERY }
CREATE HYPOTHETICAL INDEX hi ON t_hyp_idx_args s TYPE ngrambf_v1 GRANULARITY 1; -- { serverError BAD_ARGUMENTS }

DROP TABLE t_hyp_idx_args;
