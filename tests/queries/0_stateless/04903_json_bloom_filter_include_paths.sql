DROP TABLE IF EXISTS json_bf_include_paths;
DROP TABLE IF EXISTS json_bf_include_paths_invalid;

CREATE TABLE json_bf_include_paths
(
    id UInt64,
    j JSON(
        request_id String,
        request String,
        created_at String,
        private_id String,
        message String,
        ignored Dynamic(max_types = 0),
        payload Tuple(
            ids Tuple(primary String, secret String),
            updated_at String,
            text String,
            raw Tuple(secret String),
            kept String),
        items Array(JSON(max_dynamic_paths = 0, item_id String, name String)),
        tuple_map Map(String, Tuple(item_id String, name String)),
        metadata Tuple(trace String),
        other Tuple(trace_id String),
        meta String),
    INDEX exact_idx j TYPE jsonbf_v1(
        false_positive_rate = 0.0001,
        include_paths = ['request_id', 'payload.ids', 'items.item_id', 'tuple_map.item_id'],
        skip_paths = ['payload.ids.secret']) GRANULARITY 1,
    INDEX regexp_idx j TYPE jsonbf_v1(
        false_positive_rate = 0.0001,
        include_paths_regexp = ['(?:^|\\.)(?:.*_id|.*_at)$'],
        skip_paths_regexp = ['(?:^|\\.)private_']) GRANULARITY 1,
    INDEX skip_idx j TYPE jsonbf_v1(
        false_positive_rate = 0.0001,
        skip_paths = ['request_id', 'payload.raw', 'ignored'],
        skip_paths_regexp = ['^metadata\\.', 'trace_id$']) GRANULARITY 1
)
ENGINE = MergeTree
ORDER BY id
SETTINGS index_granularity = 1;

INSERT INTO json_bf_include_paths FORMAT JSONEachRow
{"id":1,"j":{"request_id":"req-1","request":"request-1","created_at":"time-1","private_id":"private-1","message":"message-1","ignored":[[1]],"payload":{"ids":{"primary":"primary-1","secret":"secret-1"},"updated_at":"updated-1","text":"text-1","raw":{"secret":"raw-1"},"kept":"nested-1"},"items":[{"item_id":"item-1","name":"name-1"}],"tuple_map":{"k":{"item_id":"map-id-1","name":"map-name-1"}},"metadata":{"trace":"trace-1"},"other":{"trace_id":"other-1"},"meta":"meta-1"}}
{"id":2,"j":{"request_id":"req-2","request":"request-2","created_at":"time-2","private_id":"private-2","message":"message-2","ignored":[[2]],"payload":{"ids":{"primary":"primary-2","secret":"secret-2"},"updated_at":"updated-2","text":"text-2","raw":{"secret":"raw-2"},"kept":"nested-2"},"items":[{"item_id":"item-2","name":"name-2"}],"tuple_map":{"k":{"item_id":"map-id-2","name":"map-name-2"}},"metadata":{"trace":"trace-2"},"other":{"trace_id":"other-2"},"meta":"meta-2"}}
;

SELECT 'exact direct', groupArray(id) FROM json_bf_include_paths WHERE j.request_id = 'req-2'
SETTINGS force_data_skipping_indices = 'exact_idx';
SELECT 'exact nested', groupArray(id) FROM json_bf_include_paths WHERE j.payload.ids.primary = 'primary-1'
SETTINGS force_data_skipping_indices = 'exact_idx';
SELECT 'exact array JSON', groupArray(id) FROM json_bf_include_paths WHERE has(j.items[].item_id, 'item-2')
SETTINGS force_data_skipping_indices = 'exact_idx';
SELECT 'exact map descendant', groupArray(id) FROM json_bf_include_paths WHERE j.tuple_map['k'].item_id = 'map-id-1'
SETTINGS force_data_skipping_indices = 'exact_idx';

SELECT 'regexp direct', groupArray(id) FROM json_bf_include_paths WHERE j.created_at = 'time-2'
SETTINGS force_data_skipping_indices = 'regexp_idx';
SELECT 'regexp array JSON', groupArray(id) FROM json_bf_include_paths WHERE has(j.items[].item_id, 'item-1')
SETTINGS force_data_skipping_indices = 'regexp_idx';
SELECT 'regexp map descendant', groupArray(id) FROM json_bf_include_paths WHERE j.tuple_map['k'].item_id = 'map-id-2'
SETTINGS force_data_skipping_indices = 'regexp_idx';

SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM json_bf_include_paths WHERE j.request_id = 'missing'
    SETTINGS force_data_skipping_indices = 'exact_idx', parallel_replicas_for_non_replicated_merge_tree = 0
)
WHERE trim(explain) = 'Granules: 0/2';

SELECT trim(explain) FROM
(
    EXPLAIN indexes = 1
    SELECT count() FROM json_bf_include_paths WHERE j.created_at = 'missing'
    SETTINGS force_data_skipping_indices = 'regexp_idx', parallel_replicas_for_non_replicated_merge_tree = 0
)
WHERE trim(explain) = 'Granules: 0/2';

SELECT groupArray(id) FROM json_bf_include_paths WHERE j.request = 'request-1';
SELECT groupArray(id) FROM json_bf_include_paths WHERE j.request = 'request-1'
SETTINGS force_data_skipping_indices = 'exact_idx'; -- { serverError INDEX_NOT_USED }
SELECT groupArray(id) FROM json_bf_include_paths WHERE j.payload.ids.secret = 'secret-1';
SELECT groupArray(id) FROM json_bf_include_paths WHERE j.payload.ids.secret = 'secret-1'
SETTINGS force_data_skipping_indices = 'exact_idx'; -- { serverError INDEX_NOT_USED }
SELECT groupArray(id) FROM json_bf_include_paths WHERE j.message = 'message-1';
SELECT groupArray(id) FROM json_bf_include_paths WHERE j.message = 'message-1'
SETTINGS force_data_skipping_indices = 'regexp_idx'; -- { serverError INDEX_NOT_USED }
SELECT groupArray(id) FROM json_bf_include_paths WHERE j.private_id = 'private-1';
SELECT groupArray(id) FROM json_bf_include_paths WHERE j.private_id = 'private-1'
SETTINGS force_data_skipping_indices = 'regexp_idx'; -- { serverError INDEX_NOT_USED }

SELECT 'exact prefix sibling', groupArray(id) FROM json_bf_include_paths WHERE j.request = 'request-2'
SETTINGS force_data_skipping_indices = 'skip_idx';
SELECT 'nested kept', groupArray(id) FROM json_bf_include_paths WHERE j.payload.kept = 'nested-1'
SETTINGS force_data_skipping_indices = 'skip_idx';
SELECT 'regexp prefix sibling', groupArray(id) FROM json_bf_include_paths WHERE j.meta = 'meta-2'
SETTINGS force_data_skipping_indices = 'skip_idx';

SELECT count() FROM json_bf_include_paths WHERE j.request_id = 'req-1'
SETTINGS force_data_skipping_indices = 'skip_idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_bf_include_paths WHERE j.payload.raw.secret = 'raw-1'
SETTINGS force_data_skipping_indices = 'skip_idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_bf_include_paths WHERE j.metadata.trace = 'trace-1'
SETTINGS force_data_skipping_indices = 'skip_idx'; -- { serverError INDEX_NOT_USED }
SELECT count() FROM json_bf_include_paths WHERE j.other.trace_id = 'other-1'
SETTINGS force_data_skipping_indices = 'skip_idx'; -- { serverError INDEX_NOT_USED }

DROP TABLE json_bf_include_paths;

CREATE TABLE json_bf_include_paths_invalid (j JSON, INDEX idx j TYPE jsonbf_v1(include_paths = 'request_id') GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE json_bf_include_paths_invalid (j JSON, INDEX idx j TYPE jsonbf_v1(include_paths = ['']) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE json_bf_include_paths_invalid (j JSON, INDEX idx j TYPE jsonbf_v1(include_paths_regexp = ['[']) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE json_bf_include_paths_invalid (j JSON, INDEX idx j TYPE jsonbf_v1(skip_paths = 'request_id') GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE json_bf_include_paths_invalid (j JSON, INDEX idx j TYPE jsonbf_v1(skip_paths = ['']) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
CREATE TABLE json_bf_include_paths_invalid (j JSON, INDEX idx j TYPE jsonbf_v1(skip_paths_regexp = ['[']) GRANULARITY 1) ENGINE = MergeTree ORDER BY tuple(); -- { serverError BAD_ARGUMENTS }
