SELECT count(), uniq(dummy) FROM remote('127.0.0.{2,3}', system.one) SETTINGS distributed_group_by_no_merge = 1;
SELECT count(), uniq(dummy) FROM remote('127.0.0.{2,3,4,5}', system.one) SETTINGS distributed_group_by_no_merge = 1;
