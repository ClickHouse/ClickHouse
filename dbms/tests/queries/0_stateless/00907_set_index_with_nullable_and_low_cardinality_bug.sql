SET allow_experimental_data_skipping_indices=1;

drop table if exists null_lc_set_index;

CREATE TABLE null_lc_set_index (
  timestamp         DateTime,
  action            LowCardinality(Nullable(String)),
  user              LowCardinality(Nullable(String)),
  INDEX test_user_idx (user) TYPE set(0) GRANULARITY 8192
) ENGINE=MergeTree
  PARTITION BY toYYYYMMDD(timestamp)
  ORDER BY (timestamp, action, cityHash64(user))
  SAMPLE BY cityHash64(user);
INSERT INTO null_lc_set_index VALUES (1550883010, 'subscribe', 'alice');
INSERT INTO null_lc_set_index VALUES (1550883020, 'follow', 'bob');

SELECT action, user FROM null_lc_set_index WHERE user = 'alice';

drop table if exists null_lc_set_index;

