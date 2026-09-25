from dataclasses import dataclass


@dataclass(frozen=True)
class SelectionConfig:
    version: str = "precise-coverage-v5"
    path_version: str = "repo-relative-v1-with-dotted-compatibility"
    narrow_region_max_lines: int = 40
    max_precise_region_owners: int = 150
    max_selected_tests_temporary: int = 250
    selection_target: int = 100
    coverage_run_count: int = 3
    coverage_search_days: int = 14
    coverage_max_age_hours: int = 72
    coverage_settle_hours: int = 1
    coverage_shards: int = 8
    min_exported_tests_per_shard: int = 100
    # Timestamps whose exported tests are counted per snapshot query.
    snapshot_batch_timestamps: int = 5
    # Per-attempt timeout of the snapshot queries; the default 60 s is too
    # short while the coverage export is inserting.
    snapshot_query_timeout_sec: int = 180
    # The snapshot queries only read settled exports, so their results do not
    # change and can be shared by all jobs and pull requests.
    snapshot_query_cache_ttl_sec: int = 3600
    hunk_context_weight: float = 0.5
    entry_count_bonus_bound: float = 0.1
    # Enable only after pre-PR replay and shadow reports establish recall near 100.
    expanded_targeted_matrix: bool = False


SELECTION_CONFIG = SelectionConfig()
