DROP TABLE IF EXISTS json_bf_skip_paths;
DROP TABLE IF EXISTS json_bf_skip_paths_invalid;

CREATE TABLE json_bf_skip_paths_invalid
(
    j JSON,
    INDEX idx j TYPE jsonbf_v1(skip_paths = 'request_id') GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE json_bf_skip_paths_invalid
(
    j JSON,
    INDEX idx j TYPE jsonbf_v1(skip_paths = ['']) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE json_bf_skip_paths_invalid
(
    j JSON,
    INDEX idx j TYPE jsonbf_v1(skip_paths_regexp = ['[']) GRANULARITY 1
)
ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }

CREATE TABLE json_bf_skip_paths
(
    id UInt64,
    j JSON(
        request_id String,
        request String,
        payload Tuple(raw Tuple(secret String), kept String),
        metadata Tuple(trace String),
        other Tuple(trace_id String),
        meta String),
    INDEX idx j TYPE jsonbf_v1(
        false_positive_rate = 0.0001,
        skip_paths = ['request_id', 'payload.raw'],
        skip_paths_regexp = ['^metadata\\.', 'trace_id$']) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_skip_paths FORMAT JSONEachRow
{"id":1,"j":{"request_id":"high-1","request":"keep-1","payload":{"raw":{"secret":"raw-1"},"kept":"nested-1"},"metadata":{"trace":"trace-1"},"other":{"trace_id":"other-1"},"meta":"meta-1"}}
{"id":2,"j":{"request_id":"high-2","request":"keep-2","payload":{"raw":{"secret":"raw-2"},"kept":"nested-2"},"metadata":{"trace":"trace-2"},"other":{"trace_id":"other-2"},"meta":"meta-2"}}
;

SELECT 'exact prefix sibling', groupArray(id) FROM json_bf_skip_paths WHERE j.request = 'keep-2'
SETTINGS force_data_skipping_indices = 'idx';
SELECT 'nested kept', groupArray(id) FROM json_bf_skip_paths WHERE j.payload.kept = 'nested-1'
SETTINGS force_data_skipping_indices = 'idx';
SELECT 'regexp prefix sibling', groupArray(id) FROM json_bf_skip_paths WHERE j.meta = 'meta-2'
SETTINGS force_data_skipping_indices = 'idx';

SELECT count() FROM json_bf_skip_paths WHERE j.request_id = 'high-1'
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_bf_skip_paths WHERE j.payload.raw.secret = 'raw-1'
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_bf_skip_paths WHERE j.metadata.trace = 'trace-1'
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_bf_skip_paths WHERE j.other.trace_id = 'other-1'
SETTINGS force_data_skipping_indices = 'idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE json_bf_skip_paths;
