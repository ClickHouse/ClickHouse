from dataclasses import dataclass


@dataclass(frozen=True)
class SelectionConfig:
    version: str = "precise-coverage-v1"
    path_version: str = "repo-relative-v1-with-dotted-compatibility"
    narrow_region_max_lines: int = 40
    max_precise_region_owners: int = 150
    max_selected_tests_temporary: int = 250
    selection_target: int = 100
    coverage_run_count: int = 3
    coverage_search_days: int = 14
    coverage_max_age_hours: int = 72
    coverage_shards: int = 8
    min_exported_tests_per_shard: int = 100
    hunk_context_weight: float = 0.5
    entry_count_bonus_bound: float = 0.1
    # Enable only after pre-PR replay and shadow reports establish recall near 100.
    expanded_targeted_matrix: bool = False


SELECTION_CONFIG = SelectionConfig()
