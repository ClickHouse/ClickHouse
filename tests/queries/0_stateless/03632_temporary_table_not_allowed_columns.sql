create temporary table test (_row_exists UInt32) engine=MergeTree order by tuple(); -- {serverError ILLEGAL_COLUMN}
create temporary table test (d Dynamic) engine=Log(); -- {serverError ILLEGAL_COLUMN}

