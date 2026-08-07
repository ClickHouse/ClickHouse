drop table if exists cdp_segments;
drop table if exists cdp_customers;

create table cdp_segments (seg_id String, mid_seqs AggregateFunction(groupBitmap, UInt32)) engine=ReplacingMergeTree() order by (seg_id);
create table cdp_customers (mid String, mid_seq UInt32) engine=ReplacingMergeTree() order by (mid_seq);
alter table cdp_segments update mid_seqs = bitmapOr(mid_seqs, (select groupBitmapState(mid_seq) from cdp_customers where mid in ('6bf3c2ee-2b33-3030-9dc2-25c6c618d141'))) where seg_id = '1234567890';

drop table cdp_segments;
drop table cdp_customers;
