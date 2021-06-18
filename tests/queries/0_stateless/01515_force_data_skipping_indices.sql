DROP TABLE IF EXISTS data_01515;
CREATE TABLE data_01515
(
    key Int,
    d1 Int,
    d1_null Nullable(Int),
    INDEX d1_idx d1 TYPE minmax GRANULARITY 1,
    INDEX d1_null_idx assumeNotNull(d1_null) TYPE minmax GRANULARITY 1
)
Engine=MergeTree()
ORDER BY key;

SELECT * FROM data_01515;
SELECT * FROM data_01515 SETTINGS force_data_skipping_indices=''; -- { serverError 6 }
SELECT * FROM data_01515 SETTINGS force_data_skipping_indices='d1_idx'; -- { serverError 277 }
SELECT * FROM data_01515 SETTINGS force_data_skipping_indices='d1_null_idx'; -- { serverError 277 }

SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='d1_idx';
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='`d1_idx`';
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices=' d1_idx ';
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='  d1_idx  ';
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='d1_idx,d1_null_idx'; -- { serverError 277 }
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='d1_null_idx,d1_idx'; -- { serverError 277 }
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='d1_null_idx,d1_idx,,'; -- { serverError 277 }
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='  d1_null_idx,d1_idx'; -- { serverError 277 }
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='  `d1_null_idx`,d1_idx'; -- { serverError 277 }
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='d1_null_idx'; -- { serverError 277 }
SELECT * FROM data_01515 WHERE d1 = 0 SETTINGS force_data_skipping_indices='  d1_null_idx  '; -- { serverError 277 }

SELECT * FROM data_01515 WHERE d1_null = 0 SETTINGS force_data_skipping_indices='d1_null_idx'; -- { serverError 277 }
SELECT * FROM data_01515 WHERE assumeNotNull(d1_null) = 0 SETTINGS force_data_skipping_indices='d1_null_idx';

DROP TABLE data_01515;
