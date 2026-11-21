-- Regression: `groupFormat` is order-dependent, so a single sparse argument must be
-- processed in input order. The default `addBatchSparseSinglePlace` appends the non-default
-- sparse values first and then the default rows, which would reorder the output. See PR #93201.

set optimize_trivial_insert_select = 1;
set max_threads = 1;

drop table if exists t_group_format_sparse;

create table t_group_format_sparse (id UInt64, v UInt64)
engine = MergeTree order by id
settings ratio_of_defaults_for_sparse_serialization = 0.9;

system stop merges t_group_format_sparse;

-- A mostly-default column so `v` is stored with sparse serialization; the only non-default
-- values are at rows 1 and 2, so a reordering bug would surface them before the row-0 default.
insert into t_group_format_sparse select number, if(number in (1, 2), number, 0) from numbers(100000);

-- Confirm `v` is actually sparse, otherwise the regression would not be exercised.
select column, serialization_kind
from system.parts_columns
where table = 't_group_format_sparse' and database = currentDatabase() and column = 'v';

-- The first formatted rows must follow input order: row 0 (default 0), then the non-default
-- rows 1 and 2, then more defaults - not the non-default values pulled to the front.
select arraySlice(splitByChar('\n', groupFormat('JSONEachRow')(v)), 1, 4)
from t_group_format_sparse;

drop table t_group_format_sparse;
