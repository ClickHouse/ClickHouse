create table if not exists ordinary_mt (x String) engine=MergeTree() ORDER BY x SETTINGS force_select_final=1; -- { serverError 181 }
