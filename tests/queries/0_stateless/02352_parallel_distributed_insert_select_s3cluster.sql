create table test(a UInt64, b String) engine=MergeTree() order by tuple();
