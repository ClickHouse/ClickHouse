#include "common/options.h"


std::vector<Option> get_mds_options() {
  return std::vector<Option>({
    Option("mds_alternate_name_max", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("set the maximum length of alternate names for dentries")
    .set_default(8_K)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_fscrypt_last_block_max_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("maximum size of the last block without the header along with a truncate request when the fscrypt is enabled.")
    .set_default(4_K)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_valgrind_exit", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_standby_replay_damaged", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME),

    Option("mds_numa_node", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("set mds's cpu affinity to a numa node (-1 for none)")
    .set_default(-1)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("mds"),

    Option("mds_data", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("path to MDS data and keyring")
    .set_default("/var/lib/ceph/mds/$cluster-$id")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .add_service("mds"),

    Option("mds_join_fs", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("file system MDS prefers to join")
    .set_long_description("This setting indicates which file system name the MDS should prefer to join (affinity). The monitors will try to have the MDS cluster safely reach a state where all MDS have strong affinity, even via failovers to a standby.")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_cache_trim_interval", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("interval in seconds between cache trimming")
    .set_default(1)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_cache_release_free_interval", Option::TYPE_SECS, Option::LEVEL_DEV)
    .set_description("interval in seconds between heap releases")
    .set_default(10)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_cache_memory_limit", Option::TYPE_SIZE, Option::LEVEL_BASIC)
    .set_description("target maximum memory usage of MDS cache")
    .set_long_description("This sets a target maximum memory usage of the MDS cache and is the primary tunable to limit the MDS memory usage. The MDS will try to stay under a reservation of this limit (by default 95%; 1 - mds_cache_reservation) by trimming unused metadata in its cache and recalling cached items in the client caches. It is possible for the MDS to exceed this limit due to slow recall from clients. The mds_health_cache_threshold (150%) sets a cache full threshold for when the MDS signals a cluster health warning.")
    .set_default(4_G)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_cache_reservation", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("amount of memory to reserve for future cached objects")
    .set_default(0.05)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_health_cache_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("threshold for cache size to generate health warning")
    .set_default(1.5)
    .add_service("mds"),

    Option("mds_cache_mid", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("midpoint for MDS cache LRU")
    .set_default(0.7)
    .add_service("mds"),

    Option("mds_cache_trim_decay_rate", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("decay rate for trimming MDS cache throttle")
    .set_default(1.0)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_cache_trim_threshold", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("threshold for number of dentries that can be trimmed")
    .set_default(256_K)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_max_file_recover", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of files to recover file sizes in parallel")
    .set_default(32)
    .add_service("mds"),

    Option("mds_dir_max_commit_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("maximum size in megabytes for a RADOS write to a directory")
    .set_default(10)
    .add_service("mds"),

    Option("mds_dir_keys_per_op", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("number of directory entries to read in one RADOS operation")
    .set_default(16384)
    .add_service("mds"),

    Option("mds_decay_halflife", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("rate of decay for temperature counters on each directory for balancing")
    .set_default(5.0)
    .add_service("mds"),

    Option("mds_beacon_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("interval in seconds between MDS beacon messages sent to monitors")
    .set_default(4.0)
    .add_service("mds"),

    Option("mds_beacon_grace", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("tolerance in seconds for missed MDS beacons to monitors")
    .set_default(15.0)
    .add_service("mds"),

    Option("mds_heartbeat_reset_grace", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the basic unit of tolerance in how many circles in a loop, which will keep running by holding the mds_lock, it must trigger to reset heartbeat")
    .set_default(1000)
    .add_service("mds"),

    Option("mds_heartbeat_grace", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("tolerance in seconds for MDS internal heartbeat")
    .set_default(15.0)
    .add_service("mds"),

    Option("mds_enforce_unique_name", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("require MDS name is unique in the cluster")
    .set_default(true)
    .add_service("mds"),

    Option("mds_session_blocklist_on_timeout", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("blocklist clients whose sessions have become stale")
    .set_default(true)
    .add_service("mds"),

    Option("mds_session_blocklist_on_evict", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("blocklist clients that have been evicted")
    .set_default(true)
    .add_service("mds"),

    Option("mds_sessionmap_keys_per_op", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of omap keys to read from the SessionMap in one operation")
    .set_default(1_K)
    .add_service("mds"),

    Option("mds_recall_max_caps", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("maximum number of caps to recall from client session in single recall")
    .set_default(30000)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_recall_max_decay_rate", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("decay rate for throttle on recalled caps on a session")
    .set_default(1.5)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_recall_max_decay_threshold", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("decay threshold for throttle on recalled caps on a session")
    .set_default(128_K)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_recall_global_max_decay_threshold", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("decay threshold for throttle on recalled caps globally")
    .set_default(128_K)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_recall_warning_threshold", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("decay threshold for warning on slow session cap recall")
    .set_default(256_K)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_recall_warning_decay_rate", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("decay rate for warning on slow session cap recall")
    .set_default(60.0)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_session_cache_liveness_decay_rate", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("decay rate for session liveness leading to preemptive cap recall")
    .set_long_description("This determines how long a session needs to be quiescent before the MDS begins preemptively recalling capabilities. The default of 5 minutes will cause 10 halvings of the decay counter after 1 hour, or 1/1024. The default magnitude of 10 (1^10 or 1024) is chosen so that the MDS considers a previously chatty session (approximately) to be quiescent after 1 hour.")
    .set_default(5_min)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds")
    .add_see_also({"mds_session_cache_liveness_magnitude"}),

    Option("mds_session_cache_liveness_magnitude", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("decay magnitude for preemptively recalling caps on quiet client")
    .set_long_description("This is the order of magnitude difference (in base 2) of the internal liveness decay counter and the number of capabilities the session holds. When this difference occurs, the MDS treats the session as quiescent and begins recalling capabilities.")
    .set_default(10)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds")
    .add_see_also({"mds_session_cache_liveness_decay_rate"}),

    Option("mds_session_cap_acquisition_decay_rate", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("decay rate for session readdir caps leading to readdir throttle")
    .set_long_description("The half-life for the session cap acquisition counter of caps acquired by readdir. This is used for throttling readdir requests from clients.")
    .set_default(30.0)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_session_cap_acquisition_throttle", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("threshold at which the cap acquisition decay counter throttles")
    .set_default(100000)
    .add_service("mds"),

    Option("mds_session_max_caps_throttle_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("ratio of mds_max_caps_per_client that client must exceed before readdir may be throttled by cap acquisition throttle")
    .set_default(1.1)
    .add_service("mds"),

    Option("mds_cap_acquisition_throttle_retry_request_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("timeout in seconds after which a client request is retried due to cap acquisition throttling")
    .set_default(0.5)
    .add_service("mds"),

    Option("mds_freeze_tree_timeout", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(30.0)
    .add_service("mds"),

    Option("mds_health_summarize_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("threshold of number of clients to summarize late client recall")
    .set_default(10)
    .add_service("mds"),

    Option("mds_reconnect_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("timeout in seconds to wait for clients to reconnect during MDS reconnect recovery state")
    .set_default(45.0)
    .add_service("mds"),

    Option("mds_deny_all_reconnect", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("flag to deny all client reconnects during failover")
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_dir_prefetch", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("flag to prefetch entire dir")
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_tick_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("time in seconds between upkeep tasks")
    .set_default(5.0)
    .add_service("mds"),

    Option("mds_dirstat_min_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1.0)
    .add_service("mds"),

    Option("mds_scatter_nudge_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("minimum interval between scatter lock updates")
    .set_default(5.0)
    .add_service("mds"),

    Option("mds_client_prealloc_inos", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("number of unused inodes to pre-allocate to clients for file creation")
    .set_default(1000)
    .add_service("mds"),

    Option("mds_client_delegate_inos_pct", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("percentage of preallocated inos to delegate to client")
    .set_default(50)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_early_reply", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("additional reply to clients that metadata requests are complete but not yet durable")
    .set_default(true)
    .add_service("mds"),

    Option("mds_replay_unsafe_with_closed_session", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("complete all the replay request when mds is restarted, no matter the session is closed or not")
    .set_default(false)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("mds"),

    Option("mds_default_dir_hash", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("hash function to select directory fragment for dentry name")
    .set_default(2)
    .add_service("mds"),

    Option("mds_log_pause", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds"),

    Option("mds_log_event_large_threshold", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(512_K)
    .set_min(1_K)
    .add_service("mds"),

    Option("mds_log_skip_corrupt_events", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_log_skip_unbounded_events", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_log_max_events", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of events in the MDS journal (-1 is unlimited)")
    .set_default(-1)
    .add_service("mds"),

    Option("mds_log_events_per_segment", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of events in an MDS journal segment")
    .set_default(1024)
    .set_min(1)
    .add_service("mds"),

    Option("mds_log_major_segment_event_ratio", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("multiple of mds_log_events_per_segment between major segments")
    .set_default(12)
    .set_min(1)
    .add_service("mds")
    .add_see_also({"mds_log_events_per_segment"}),

    Option("mds_log_segment_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("size in bytes of each MDS log segment")
    .set_default(0)
    .add_service("mds"),

    Option("mds_log_max_segments", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of segments which may be untrimmed")
    .set_default(128)
    .set_min(8)
    .add_service("mds"),

    Option("mds_log_warn_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("trigger MDS_HEALTH_TRIM warning when the mds log is longer than mds_log_max_segments * mds_log_warn_factor")
    .set_default(2.0)
    .set_min(1.0)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_bal_export_pin", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("allow setting directory export pins to particular ranks")
    .set_default(true)
    .add_service("mds"),

    Option("mds_export_ephemeral_random", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("allow ephemeral random pinning of the loaded subtrees")
    .set_long_description("probabilistically pin the loaded directory inode and the subtree beneath it to an MDS based on the consistent hash of the inode number. The higher this value the more likely the loaded subtrees get pinned")
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_export_ephemeral_random_max", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("the maximum percent permitted for random ephemeral pin policy")
    .set_default(0.01)
    .set_min_max(0.0, 1.0)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds")
    .add_see_also({"mds_export_ephemeral_random"}),

    Option("mds_export_ephemeral_distributed", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("allow ephemeral distributed pinning of the loaded subtrees")
    .set_long_description("pin the immediate child directories of the loaded directory inode based on the consistent hash of the child's inode number. ")
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_export_ephemeral_distributed_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("multiple of max_mds for splitting and distributing directory")
    .set_default(2.0)
    .set_min_max(1.0, 100.0)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_bal_sample_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("interval in seconds between balancer ticks")
    .set_default(3.0)
    .add_service("mds"),

    Option("mds_bal_replicate_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("hot popularity threshold to replicate a subtree")
    .set_default(8000.0)
    .add_service("mds"),

    Option("mds_bal_unreplicate_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("cold popularity threshold to merge subtrees")
    .set_default(0.0)
    .add_service("mds"),

    Option("mds_bal_split_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("minimum size of directory fragment before splitting")
    .set_default(10000)
    .add_service("mds"),

    Option("mds_bal_split_rd", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("hot read popularity threshold for splitting a directory fragment")
    .set_default(25000.0)
    .add_service("mds"),

    Option("mds_bal_split_wr", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("hot write popularity threshold for splitting a directory fragment")
    .set_default(10000.0)
    .add_service("mds"),

    Option("mds_bal_split_bits", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("power of two child fragments for a fragment on split")
    .set_default(3)
    .set_min_max(1, 24)
    .add_service("mds"),

    Option("mds_bal_merge_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("size of fragments where merging should occur")
    .set_default(50)
    .add_service("mds"),

    Option("mds_bal_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("interval between MDS balancer cycles")
    .set_default(10)
    .add_service("mds"),

    Option("mds_bal_fragment_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("delay in seconds before interrupting client IO to perform splits")
    .set_default(5)
    .add_service("mds"),

    Option("mds_bal_fragment_size_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("maximum size of a directory fragment before new creat/links fail")
    .set_default(100000)
    .add_service("mds"),

    Option("mds_bal_fragment_fast_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("ratio of mds_bal_split_size at which fast fragment splitting occurs")
    .set_default(1.5)
    .add_service("mds"),

    Option("mds_bal_fragment_dirs", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("enable directory fragmentation")
    .set_long_description("Directory fragmentation is a standard feature of CephFS that allows sharding directories across multiple objects for performance and stability. Additionally, this allows fragments to be distributed across multiple active MDSs to increase throughput. Disabling (new) fragmentation should only be done in exceptional circumstances and may lead to performance issues.")
    .set_default(true)
    .add_service("mds"),

    Option("mds_bal_idle_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("idle metadata popularity threshold before rebalancing")
    .set_default(0.0)
    .add_service("mds"),

    Option("mds_bal_max", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(-1)
    .add_service("mds"),

    Option("mds_bal_max_until", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(-1)
    .add_service("mds"),

    Option("mds_bal_mode", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mds"),

    Option("mds_bal_min_rebalance", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("amount overloaded over internal target before balancer begins offloading")
    .set_default(0.1)
    .add_service("mds"),

    Option("mds_bal_overload_epochs", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(2)
    .add_service("mds"),

    Option("mds_bal_min_start", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.2)
    .add_service("mds"),

    Option("mds_bal_need_min", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.8)
    .add_service("mds"),

    Option("mds_bal_need_max", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1.2)
    .add_service("mds"),

    Option("mds_bal_midchunk", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.3)
    .add_service("mds"),

    Option("mds_bal_minchunk", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.001)
    .add_service("mds"),

    Option("mds_bal_target_decay", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("rate of decay for export targets communicated to clients")
    .set_default(10.0)
    .add_service("mds"),

    Option("mds_oft_prefetch_dirfrags", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("prefetch dirfrags recorded in open file table on startup")
    .set_default(false)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("mds"),

    Option("mds_replay_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("time in seconds between replay of updates to journal by standby replay MDS")
    .set_default(1.0)
    .add_service("mds"),

    Option("mds_shutdown_check", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mds"),

    Option("mds_thrash_exports", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mds"),

    Option("mds_thrash_fragments", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mds"),

    Option("mds_dump_cache_on_map", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds"),

    Option("mds_dump_cache_after_rejoin", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds"),

    Option("mds_verify_scatter", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds"),

    Option("mds_debug_scatterstat", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds"),

    Option("mds_debug_frag", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds"),

    Option("mds_debug_auth_pins", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds"),

    Option("mds_debug_subtrees", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds"),

    Option("mds_abort_on_newly_corrupt_dentry", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .add_service("mds"),

    Option("mds_go_bad_corrupt_dentry", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_inject_rename_corrupt_dentry_first", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_inject_journal_corrupt_dentry_first", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0)
    .add_service("mds"),

    Option("mds_kill_shutdown_at", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_kill_mdstable_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mds"),

    Option("mds_max_export_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(20_M)
    .add_service("mds"),

    Option("mds_kill_export_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mds"),

    Option("mds_kill_import_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mds"),

    Option("mds_kill_link_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mds"),

    Option("mds_kill_rename_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mds"),

    Option("mds_kill_openc_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mds"),

    Option("mds_kill_journal_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mds"),

    Option("mds_kill_journal_expire_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mds"),

    Option("mds_kill_journal_replay_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mds"),

    Option("mds_journal_format", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(1)
    .add_service("mds"),

    Option("mds_kill_create_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mds"),

    Option("mds_inject_health_dummy", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds"),

    Option("mds_kill_skip_replaying_inotable", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds"),

    Option("mds_inject_skip_replaying_inotable", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds"),

    Option("mds_inject_traceless_reply_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0)
    .add_service("mds"),

    Option("mds_wipe_sessions", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds"),

    Option("mds_wipe_ino_prealloc", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds"),

    Option("mds_skip_ino", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mds"),

    Option("mds_enable_op_tracker", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("track remote operation progression and statistics")
    .set_default(true)
    .add_service("mds"),

    Option("mds_op_history_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum size for list of historical operations")
    .set_default(20)
    .add_service("mds"),

    Option("mds_op_history_duration", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("expiration time in seconds of historical operations")
    .set_default(600)
    .add_service("mds"),

    Option("mds_op_history_slow_op_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum size for list of historical slow operations")
    .set_default(20)
    .add_service("mds"),

    Option("mds_op_history_slow_op_threshold", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("track the op if over this threshold")
    .set_default(10)
    .add_service("mds"),

    Option("mds_op_complaint_time", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("time in seconds to consider an operation blocked after no updates")
    .set_default(30.0)
    .add_service("mds"),

    Option("mds_op_log_threshold", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(5)
    .add_service("mds"),

    Option("mds_snap_min_uid", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("minimum uid of client to perform snapshots")
    .set_default(0)
    .add_service("mds"),

    Option("mds_snap_max_uid", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum uid of client to perform snapshots")
    .set_default(4294967294)
    .add_service("mds"),

    Option("mds_snap_rstat", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("enabled nested rstat for snapshots")
    .set_default(false)
    .add_service("mds"),

    Option("mds_verify_backtrace", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(1)
    .add_service("mds"),

    Option("mds_max_completed_flushes", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(100000)
    .add_service("mds"),

    Option("mds_max_completed_requests", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(100000)
    .add_service("mds"),

    Option("mds_action_on_write_error", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("action to take when MDS cannot write to RADOS (0:ignore, 1:read-only, 2:suicide)")
    .set_default(1)
    .add_service("mds"),

    Option("mds_mon_shutdown_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("time to wait for mon to receive damaged MDS rank notification")
    .set_default(5.0)
    .add_service("mds"),

    Option("mds_max_purge_files", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of deleted files to purge in parallel")
    .set_default(64)
    .add_service("mds"),

    Option("mds_max_purge_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of purge operations performed in parallel")
    .set_default(8_K)
    .add_service("mds"),

    Option("mds_max_purge_ops_per_pg", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("number of parallel purge operations performed per PG")
    .set_default(0.5)
    .add_service("mds"),

    Option("mds_purge_queue_busy_flush_period", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1.0)
    .add_service("mds"),

    Option("mds_root_ino_uid", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("default uid for new root directory")
    .set_default(0)
    .add_service("mds"),

    Option("mds_root_ino_gid", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("default gid for new root directory")
    .set_default(0)
    .add_service("mds"),

    Option("mds_max_scrub_ops_in_progress", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of scrub operations performed in parallel")
    .set_default(5)
    .add_service("mds"),

    Option("mds_forward_all_requests_to_auth", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("always process op on auth mds")
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_damage_table_max_entries", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of damage table entries")
    .set_default(10000)
    .add_service("mds"),

    Option("mds_client_writeable_range_max_inc_objs", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of objects in writeable range of a file for a client")
    .set_default(1_K)
    .add_service("mds"),

    Option("mds_min_caps_per_client", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("minimum number of capabilities a client may hold")
    .set_default(100)
    .add_service("mds"),

    Option("mds_min_caps_working_set", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of capabilities a client may hold without cache pressure warnings generated")
    .set_default(10000)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_max_caps_per_client", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of capabilities a client may hold")
    .set_default(1_M)
    .add_service("mds"),

    Option("mds_hack_allow_loading_invalid_metadata", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("INTENTIONALLY CAUSE DATA LOSS by bypasing checks for invalid metadata on disk. Allows testing repair tools.")
    .set_default(false)
    .add_service("mds"),

    Option("mds_defer_session_stale", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true)
    .add_service("mds"),

    Option("mds_inject_migrator_session_race", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds"),

    Option("mds_request_load_average_decay_rate", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("rate of decay in seconds for calculating request load average")
    .set_default(1_min)
    .add_service("mds"),

    Option("mds_cap_revoke_eviction_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("number of seconds after which clients which have not responded to cap revoke messages by the MDS are evicted.")
    .set_default(0.0)
    .add_service("mds"),

    Option("mds_dump_cache_threshold_formatter", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("threshold for cache usage to disallow \"dump cache\" operation to formatter")
    .set_long_description("Disallow MDS from dumping caches to formatter via \"dump cache\" command if cache usage exceeds this threshold.")
    .set_default(1_G)
    .add_service("mds"),

    Option("mds_dump_cache_threshold_file", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("threshold for cache usage to disallow \"dump cache\" operation to file")
    .set_long_description("Disallow MDS from dumping caches to file via \"dump cache\" command if cache usage exceeds this threshold.")
    .set_default(0)
    .add_service("mds"),

    Option("mds_task_status_update_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("task status update interval to manager")
    .set_long_description("interval (in seconds) for sending mds task status to ceph manager")
    .set_default(2.0)
    .add_service("mds"),

    Option("mds_max_snaps_per_dir", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("max snapshots per directory")
    .set_long_description("maximum number of snapshots that can be created per directory")
    .set_default(100)
    .set_min_max(0ULL, 4_K)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_asio_thread_count", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Size of thread pool for ASIO completions")
    .set_default(2)
    .set_min(1)
    .add_service("mds")
    .add_tag("mds"),

    Option("mds_ping_grace", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("timeout after which an MDS is considered laggy by rank 0 MDS.")
    .set_long_description("timeout for replying to a ping message sent by rank 0 after which an active MDS considered laggy (delayed metrics) by rank 0.")
    .set_default(15)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_ping_interval", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("interval in seconds for sending ping messages to active MDSs.")
    .set_long_description("interval in seconds for rank 0 to send ping messages to all active MDSs.")
    .set_default(5)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_metrics_update_interval", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("interval in seconds for metrics data update.")
    .set_long_description("interval in seconds after which active MDSs send client metrics data to rank 0.")
    .set_default(2)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_dir_max_entries", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of entries per directory before new creat/links fail")
    .set_long_description("The maximum number of entries before any new entries are rejected with ENOSPC.")
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_sleep_rank_change", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("mds_connect_bootstrapping", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME),

    Option("mds_symlink_recovery", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Stores symlink target on the first data object of symlink file. Allows recover of symlink using recovery tools.")
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_extraordinary_events_dump_interval", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("Interval in seconds for dumping the recent in-memory logs when there is an extra-ordinary event.")
    .set_long_description("Interval in seconds for dumping the recent in-memory logs when there is an extra-ordinary event. The default is ``0`` (disabled). The log level should be ``< 10`` and the gather level should be ``>=10`` in debug_mds for enabling this option.")
    .set_default(0)
    .set_min_max(0, 60)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("defer_client_eviction_on_laggy_osds", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Do not evict client if any osd is laggy")
    .set_long_description("Laggy OSD(s) can make clients laggy or unresponsive, this can lead to their eviction, this option once enabled can help defer client eviction.")
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),

    Option("mds_session_metadata_threshold", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Evict non-advancing client-tid sessions exceeding the config size.")
    .set_long_description("Evict clients which are not advancing their request tids which causes a large buildup of session metadata (`completed_requests`) in the MDS causing the MDS to go read-only since the RADOS operation exceeds the size threashold. This config is the maximum size (in bytes) that a session metadata (encoded) can grow.")
    .set_default(16_M)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds"),


  });
}
