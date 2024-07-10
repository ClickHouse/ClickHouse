#include "common/options.h"


std::vector<Option> get_osd_options() {
  return std::vector<Option>({
    Option("osd_numa_prefer_iface", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("prefer IP on network interface on same numa node as storage")
    .set_default(true)
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"osd_numa_auto_affinity"}),

    Option("osd_numa_auto_affinity", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("automatically set affinity to numa node when storage and network match")
    .set_default(true)
    .set_flag(Option::FLAG_STARTUP),

    Option("osd_numa_node", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("set affinity to a numa node (-1 for none)")
    .set_default(-1)
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"osd_numa_auto_affinity"}),

    Option("set_keepcaps", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("set the keepcaps flag before changing UID, preserving the permitted capability set")
    .set_long_description("When ceph switches from root to the ceph uid, all capabilities in all sets are eraseed. If a component that is capability aware needs a specific capability, the keepcaps flag maintains the permitted capability set, allowing the capabilities in the effective set to be activated as needed.")
    .set_default(false)
    .set_flag(Option::FLAG_STARTUP),

    Option("osd_smart_report_timeout", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Timeout (in seconds) for smartctl to run, default is set to 5")
    .set_default(5),

    Option("osd_check_max_object_name_len_on_startup", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(true),

    Option("osd_max_backfills", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Maximum number of concurrent local and remote backfills or recoveries per OSD")
    .set_long_description("There can be osd_max_backfills local reservations AND the same remote reservations per OSD. So a value of 1 lets this OSD participate as 1 PG primary in recovery and 1 shard of another recovering PG.")
    .set_default(1)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_min_recovery_priority", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Minimum priority below which recovery is not performed")
    .set_long_description("The purpose here is to prevent the cluster from doing *any* lower priority work (e.g., rebalancing) below this threshold and focus solely on higher priority work (e.g., replicating degraded objects).")
    .set_default(0),

    Option("osd_backfill_retry_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("how frequently to retry backfill reservations after being denied (e.g., due to a full OSD)")
    .set_default(30.0),

    Option("osd_recovery_retry_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("how frequently to retry recovery reservations after being denied (e.g., due to a full OSD)")
    .set_default(30.0),

    Option("osd_recovery_sleep", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Time in seconds to sleep before next recovery or backfill op. This setting overrides _ssd, _hdd, and _hybrid if non-zero.")
    .set_default(0.0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_recovery_sleep_hdd", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Time in seconds to sleep before next recovery or backfill op for HDDs")
    .set_default(0.1)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_recovery_sleep_ssd", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Time in seconds to sleep before next recovery or backfill op for SSDs")
    .set_default(0.0)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"osd_recovery_sleep"}),

    Option("osd_recovery_sleep_hybrid", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Time in seconds to sleep before next recovery or backfill op when data is on HDD and journal is on SSD")
    .set_default(0.025)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"osd_recovery_sleep"}),

    Option("osd_snap_trim_sleep", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Time in seconds to sleep before next snap trim. This setting overrides _ssd, _hdd, and _hybrid if non-zero.")
    .set_default(0.0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_snap_trim_sleep_hdd", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Time in seconds to sleep before next snap trim for HDDs")
    .set_default(5.0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_snap_trim_sleep_ssd", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Time in seconds to sleep before next snap trim for SSDs")
    .set_default(0.0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_snap_trim_sleep_hybrid", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Time in seconds to sleep before next snap trim when data is on HDD and journal is on SSD")
    .set_default(2.0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_scrub_invalid_stats", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true),

    Option("osd_max_scrubs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Maximum concurrent scrubs on a single OSD")
    .set_default(3),

    Option("osd_scrub_during_recovery", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Allow scrubbing when PGs on the OSD are undergoing recovery")
    .set_default(false),

    Option("osd_debug_trim_objects", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Asserts that no clone-objects were added to a snap after we start trimming it")
    .set_default(false),

    Option("osd_repair_during_recovery", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Allow requested repairing when PGs on the OSD are undergoing recovery")
    .set_default(false),

    Option("osd_scrub_begin_hour", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Restrict scrubbing to this hour of the day or later")
    .set_long_description("Use osd_scrub_begin_hour=0 and osd_scrub_end_hour=0 for the entire day.")
    .set_default(0)
    .set_min_max(0, 23)
    .add_see_also({"osd_scrub_end_hour"}),

    Option("osd_scrub_end_hour", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Restrict scrubbing to hours of the day earlier than this")
    .set_long_description("Use osd_scrub_begin_hour=0 and osd_scrub_end_hour=0 for the entire day.")
    .set_default(0)
    .set_min_max(0, 23)
    .add_see_also({"osd_scrub_begin_hour"}),

    Option("osd_scrub_begin_week_day", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Restrict scrubbing to this day of the week or later")
    .set_long_description("0 = Sunday, 1 = Monday, etc. Use osd_scrub_begin_week_day=0 osd_scrub_end_week_day=0 for the entire week.")
    .set_default(0)
    .set_min_max(0, 6)
    .add_see_also({"osd_scrub_end_week_day"}),

    Option("osd_scrub_end_week_day", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Restrict scrubbing to days of the week earlier than this")
    .set_long_description("0 = Sunday, 1 = Monday, etc. Use osd_scrub_begin_week_day=0 osd_scrub_end_week_day=0 for the entire week.")
    .set_default(0)
    .set_min_max(0, 6)
    .add_see_also({"osd_scrub_begin_week_day"}),

    Option("osd_scrub_load_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Allow scrubbing when system load divided by number of CPUs is below this value")
    .set_default(0.5),

    Option("osd_scrub_min_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("The desired interval between scrubs of a specific PG.")
    .set_default(1_day)
    .add_see_also({"osd_scrub_max_interval"}),

    Option("osd_scrub_max_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Scrub each PG no less often than this interval")
    .set_default(7_day)
    .add_see_also({"osd_scrub_min_interval"}),

    Option("osd_scrub_interval_randomize_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Ratio of scrub interval to randomly vary")
    .set_long_description("This prevents a scrub 'stampede' by randomly varying the scrub intervals so that they are soon uniformly distributed over the week")
    .set_default(0.5)
    .add_see_also({"osd_scrub_min_interval"}),

    Option("osd_scrub_backoff_ratio", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("Backoff ratio for scheduling scrubs")
    .set_long_description("Probability that a particular OSD tick instance will skip scrub scheduling. 66% means that approximately one of three ticks will cause scrub scheduling.")
    .set_default(0.66),

    Option("osd_scrub_chunk_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Minimum number of objects to deep-scrub in a single chunk")
    .set_default(5)
    .add_see_also({"osd_scrub_chunk_max"}),

    Option("osd_scrub_chunk_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Maximum number of objects to deep-scrub in a single chunk")
    .set_default(25)
    .add_see_also({"osd_scrub_chunk_min"}),

    Option("osd_shallow_scrub_chunk_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Minimum number of objects to scrub in a single chunk")
    .set_default(50)
    .add_see_also({"osd_shallow_scrub_chunk_max", "osd_scrub_chunk_min"}),

    Option("osd_shallow_scrub_chunk_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Maximum number of objects to scrub in a single chunk")
    .set_default(100)
    .add_see_also({"osd_shallow_scrub_chunk_min", "osd_scrub_chunk_max"}),

    Option("osd_scrub_sleep", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Duration to inject a delay during scrubbing")
    .set_default(0.0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_scrub_extended_sleep", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Duration to inject a delay during scrubbing out of scrubbing hours (seconds)")
    .set_default(0.0)
    .add_see_also({"osd_scrub_begin_hour", "osd_scrub_end_hour", "osd_scrub_begin_week_day", "osd_scrub_end_week_day"}),

    Option("osd_scrub_auto_repair", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Automatically repair damaged objects detected during scrub")
    .set_default(false),

    Option("osd_scrub_auto_repair_num_errors", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Maximum number of detected errors to automatically repair")
    .set_default(5)
    .add_see_also({"osd_scrub_auto_repair"}),

    Option("osd_scrub_max_preemptions", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Set the maximum number of times we will preempt a deep scrub due to a client operation before blocking client IO to complete the scrub")
    .set_default(5)
    .set_min_max(0, 30),

    Option("osd_deep_scrub_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Deep scrub each PG (i.e., verify data checksums) at least this often")
    .set_default(7_day),

    Option("osd_deep_scrub_randomize_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Scrubs will randomly become deep scrubs at this rate (0.15 -> 15% of scrubs are deep)")
    .set_long_description("This prevents a deep scrub 'stampede' by spreading deep scrubs so they are uniformly distributed over the week")
    .set_default(0.15),

    Option("osd_deep_scrub_stride", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Number of bytes to read from an object at a time during deep scrub")
    .set_default(512_K),

    Option("osd_deep_scrub_keys", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Number of keys to read from an object at a time during deep scrub")
    .set_default(1024),

    Option("osd_deep_scrub_update_digest_min_age", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Update overall object digest only if object was last modified longer ago than this")
    .set_default(2_hr),

    Option("osd_deep_scrub_large_omap_object_key_threshold", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Warn when we encounter an object with more omap keys than this")
    .set_default(200000)
    .add_service({"osd", "mds"})
    .add_see_also({"osd_deep_scrub_large_omap_object_value_sum_threshold"}),

    Option("osd_deep_scrub_large_omap_object_value_sum_threshold", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Warn when we encounter an object with more omap key bytes than this")
    .set_default(1_G)
    .add_service("osd")
    .add_see_also({"osd_deep_scrub_large_omap_object_key_threshold"}),

    Option("osd_blocked_scrub_grace_period", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Time (seconds) before issuing a cluster-log warning")
    .set_long_description("Waiting too long for an object in the scrubbed chunk to be unlocked.")
    .set_default(120),

    Option("osd_stats_update_period_scrubbing", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Stats update period (seconds) when scrubbing")
    .set_long_description("A PG actively scrubbing (or blocked while scrubbing) publishes its stats (inc. scrub/block duration) every this many seconds.")
    .set_default(15),

    Option("osd_stats_update_period_not_scrubbing", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Stats update period (seconds) when not scrubbing")
    .set_long_description("A PG we are a primary of, publishes its stats (inc. scrub/block duration) every this many seconds.")
    .set_default(120),

    Option("osd_scrub_slow_reservation_response", Option::TYPE_MILLISECS, Option::LEVEL_ADVANCED)
    .set_description("Maximum wait (milliseconds) for scrub reservations before issuing a cluster-log warning")
    .set_long_description("Waiting too long for a replica to respond to scrub resource reservation request. Disable by setting to a very large value.")
    .set_default(30000)
    .set_min(500)
    .add_see_also({"osd_scrub_reservation_timeout"}),

    Option("osd_scrub_reservation_timeout", Option::TYPE_MILLISECS, Option::LEVEL_ADVANCED)
    .set_description("Maximum wait (milliseconds) for replicas' response to scrub reservation requests")
    .set_long_description("Maximum wait (milliseconds) for all replicas to respond to scrub reservation requests, before the scrub session is aborted. Disable by setting to a very large value.")
    .set_default(300000)
    .set_min(2000)
    .add_see_also({"osd_scrub_slow_reservation_response"}),

    Option("osd_class_dir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("lib/rados-classes"),

    Option("osd_open_classes_on_start", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true),

    Option("osd_class_load_list", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("cephfs hello journal lock log numops otp rbd refcount rgw rgw_gc timeindex user version cas cmpomap queue 2pc_queue fifo"),

    Option("osd_class_default_list", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("cephfs hello journal lock log numops otp rbd refcount rgw rgw_gc timeindex user version cas cmpomap queue 2pc_queue fifo"),

    Option("osd_agent_max_ops", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("maximum concurrent tiering operations for tiering agent")
    .set_default(4),

    Option("osd_agent_max_low_ops", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("maximum concurrent low-priority tiering operations for tiering agent")
    .set_default(2),

    Option("osd_agent_min_evict_effort", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("minimum effort to expend evicting clean objects")
    .set_default(0.1)
    .set_min_max(0.0, 0.99),

    Option("osd_agent_quantize_effort", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("size of quantize unit for eviction effort")
    .set_default(0.1),

    Option("osd_agent_delay_time", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("how long agent should sleep if it has no work to do")
    .set_default(5.0),

    Option("osd_agent_hist_halflife", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("halflife of agent atime and temp histograms")
    .set_default(1000),

    Option("osd_agent_slop", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("slop factor to avoid switching tiering flush and eviction mode")
    .set_default(0.02),

    Option("osd_find_best_info_ignore_history_les", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("ignore last_epoch_started value when peering AND PROBABLY LOSE DATA")
    .set_long_description("THIS IS AN EXTREMELY DANGEROUS OPTION THAT SHOULD ONLY BE USED AT THE DIRECTION OF A DEVELOPER.  It makes peering ignore the last_epoch_started value when peering, which can allow the OSD to believe an OSD has an authoritative view of a PG's contents even when it is in fact old and stale, typically leading to data loss (by believing a stale PG is up to date).")
    .set_default(false),

    Option("osd_uuid", Option::TYPE_UUID, Option::LEVEL_ADVANCED)
    .set_description("uuid label for a new OSD")
    .set_flag(Option::FLAG_CREATE),

    Option("osd_data", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("path to OSD data")
    .set_default("/var/lib/ceph/osd/$cluster-$id")
    .set_flag(Option::FLAG_NO_MON_UPDATE),

    Option("osd_journal", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("path to OSD journal (when FileStore backend is in use)")
    .set_default("/var/lib/ceph/osd/$cluster-$id/journal")
    .set_flag(Option::FLAG_NO_MON_UPDATE),

    Option("osd_journal_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("size of FileStore journal (in MiB)")
    .set_default(5_K)
    .set_flag(Option::FLAG_CREATE),

    Option("osd_journal_flush_on_shutdown", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("flush FileStore journal contents during clean OSD shutdown")
    .set_default(true),

    Option("osd_compact_on_start", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("compact OSD's object store's OMAP on start")
    .set_default(false),

    Option("osd_os_flags", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("flags to skip filestore omap or journal initialization")
    .set_default(0),

    Option("osd_max_write_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Maximum size of a RADOS write operation in megabytes")
    .set_long_description("This setting prevents clients from doing very large writes to RADOS.  If you set this to a value below what clients expect, they will receive an error when attempting to write to the cluster.")
    .set_default(90)
    .set_min(4),

    Option("osd_max_pgls", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of results when listing objects in a pool")
    .set_default(1_K),

    Option("osd_client_message_size_cap", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("maximum memory to devote to in-flight client requests")
    .set_long_description("If this value is exceeded, the OSD will not read any new client data off of the network until memory is freed.")
    .set_default(500_M),

    Option("osd_client_message_cap", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of in-flight client requests")
    .set_default(256),

    Option("osd_crush_update_on_start", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("update OSD CRUSH location on startup")
    .set_default(true),

    Option("osd_class_update_on_start", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("set OSD device class on startup")
    .set_default(true),

    Option("osd_crush_initial_weight", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("if >= 0, initial CRUSH weight for newly created OSDs")
    .set_long_description("If this value is negative, the size of the OSD in TiB is used.")
    .set_default(-1.0),

    Option("osd_allow_recovery_below_min_size", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("allow replicated pools to recover with < min_size active members")
    .set_default(true)
    .add_service("osd"),

    Option("osd_map_share_max_epochs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(40),

    Option("osd_map_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(50),

    Option("osd_pg_epoch_max_lag_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Max multiple of the map cache that PGs can lag before we throttle map injest")
    .set_default(2.0)
    .add_see_also({"osd_map_cache_size"}),

    Option("osd_inject_bad_map_crc_probability", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(0.0),

    Option("osd_inject_failure_on_pg_removal", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false),

    Option("osd_max_markdown_period", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10_min),

    Option("osd_max_markdown_count", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5),

    Option("osd_op_thread_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(15),

    Option("osd_op_thread_suicide_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(150),

    Option("osd_op_pq_max_tokens_per_priority", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(4_M),

    Option("osd_op_pq_min_cost", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(64_K),

    Option("osd_recover_clone_overlap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true),

    Option("osd_num_cache_shards", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("The number of cache shards to use in the object store.")
    .set_default(32)
    .set_flag(Option::FLAG_STARTUP),

    Option("osd_aggregated_slow_ops_logging", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Allow OSD daemon to send an aggregated slow ops to the cluster log")
    .set_default(true),

    Option("osd_op_num_threads_per_shard", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_flag(Option::FLAG_STARTUP),

    Option("osd_op_num_threads_per_shard_hdd", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(1)
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"osd_op_num_threads_per_shard"}),

    Option("osd_op_num_threads_per_shard_ssd", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(2)
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"osd_op_num_threads_per_shard"}),

    Option("osd_op_num_shards", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(0)
    .set_flag(Option::FLAG_STARTUP),

    Option("osd_op_num_shards_hdd", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(5)
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"osd_op_num_shards"}),

    Option("osd_op_num_shards_ssd", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(8)
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"osd_op_num_shards"}),

    Option("osd_skip_data_digest", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Do not store full-object checksums if the backend (bluestore) does its own checksums.  Only usable with all BlueStore OSDs.")
    .set_default(false),

    Option("osd_op_queue", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("which operation priority queue algorithm to use")
    .set_long_description("which operation priority queue algorithm to use")
    .set_default("mclock_scheduler")
    .set_enum_allowed({"wpq", "mclock_scheduler", "debug_random"})
    .add_see_also({"osd_op_queue_cut_off"}),

    Option("osd_op_queue_cut_off", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("the threshold between high priority ops and low priority ops")
    .set_long_description("the threshold between high priority ops that use strict priority ordering and low priority ops that use a fairness algorithm that may or may not incorporate priority")
    .set_default("high")
    .set_enum_allowed({"low", "high", "debug_random"})
    .add_see_also({"osd_op_queue"}),

    Option("osd_mclock_scheduler_client_res", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("IO proportion reserved for each client (default). The default value of 0 specifies the lowest possible reservation. Any value greater than 0 and up to 1.0 specifies the minimum IO proportion to reserve for each client in terms of a fraction of the OSD's maximum IOPS capacity.")
    .set_long_description("Only considered for osd_op_queue = mclock_scheduler")
    .set_default(0.0)
    .set_min_max(0.0, 1.0)
    .add_see_also({"osd_op_queue"}),

    Option("osd_mclock_scheduler_client_wgt", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("IO share for each client (default) over reservation")
    .set_long_description("Only considered for osd_op_queue = mclock_scheduler")
    .set_default(1)
    .add_see_also({"osd_op_queue"}),

    Option("osd_mclock_scheduler_client_lim", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("IO limit for each client (default) over reservation. The default value of 0 specifies no limit enforcement, which means each client can use the maximum possible IOPS capacity of the OSD. Any value greater than 0 and up to 1.0 specifies the upper IO limit over reservation that each client receives in terms of a fraction of the OSD's maximum IOPS capacity.")
    .set_long_description("Only considered for osd_op_queue = mclock_scheduler")
    .set_default(0.0)
    .set_min_max(0.0, 1.0)
    .add_see_also({"osd_op_queue"}),

    Option("osd_mclock_scheduler_background_recovery_res", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("IO proportion reserved for background recovery (default). The default value of 0 specifies the lowest possible reservation. Any value greater than 0 and up to 1.0 specifies the minimum IO proportion to reserve for background recovery operations in terms of a fraction of the OSD's maximum IOPS capacity.")
    .set_long_description("Only considered for osd_op_queue = mclock_scheduler")
    .set_default(0.0)
    .set_min_max(0.0, 1.0)
    .add_see_also({"osd_op_queue"}),

    Option("osd_mclock_scheduler_background_recovery_wgt", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("IO share for each background recovery over reservation")
    .set_long_description("Only considered for osd_op_queue = mclock_scheduler")
    .set_default(1)
    .add_see_also({"osd_op_queue"}),

    Option("osd_mclock_scheduler_background_recovery_lim", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("IO limit for background recovery over reservation. The default value of 0 specifies no limit enforcement, which means background recovery operation can use the maximum possible IOPS capacity of the OSD. Any value greater than 0 and up to 1.0 specifies the upper IO limit over reservation that background recovery operation receives in terms of a fraction of the OSD's maximum IOPS capacity.")
    .set_long_description("Only considered for osd_op_queue = mclock_scheduler")
    .set_default(0.0)
    .set_min_max(0.0, 1.0)
    .add_see_also({"osd_op_queue"}),

    Option("osd_mclock_scheduler_background_best_effort_res", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("IO proportion reserved for background best_effort (default). The default value of 0 specifies the lowest possible reservation. Any value greater than 0 and up to 1.0 specifies the minimum IO proportion to reserve for background best_effort operations in terms of a fraction of the OSD's maximum IOPS capacity.")
    .set_long_description("Only considered for osd_op_queue = mclock_scheduler")
    .set_default(0.0)
    .set_min_max(0.0, 1.0)
    .add_see_also({"osd_op_queue"}),

    Option("osd_mclock_scheduler_background_best_effort_wgt", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("IO share for each background best_effort over reservation")
    .set_long_description("Only considered for osd_op_queue = mclock_scheduler")
    .set_default(1)
    .add_see_also({"osd_op_queue"}),

    Option("osd_mclock_scheduler_background_best_effort_lim", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("IO limit for background best_effort over reservation. The default value of 0 specifies no limit enforcement, which means background best_effort operation can use the maximum possible IOPS capacity of the OSD. Any value greater than 0 and up to 1.0 specifies the upper IO limit over reservation that background best_effort operation receives in terms of a fraction of the OSD's maximum IOPS capacity.")
    .set_long_description("Only considered for osd_op_queue = mclock_scheduler")
    .set_default(0.0)
    .set_min_max(0.0, 1.0)
    .add_see_also({"osd_op_queue"}),

    Option("osd_mclock_scheduler_anticipation_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("mclock anticipation timeout in seconds")
    .set_long_description("the amount of time that mclock waits until the unused resource is forfeited")
    .set_default(0.0),

    Option("osd_mclock_max_sequential_bandwidth_hdd", Option::TYPE_SIZE, Option::LEVEL_BASIC)
    .set_description("The maximum sequential bandwidth in bytes/second of the OSD (for rotational media)")
    .set_long_description("This option specifies the maximum sequential bandwidth to consider for an OSD whose underlying device type is rotational media. This is considered by the mclock scheduler to derive the cost factor to be used in QoS calculations. Only considered for osd_op_queue = mclock_scheduler")
    .set_default(150_M)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_mclock_max_sequential_bandwidth_ssd", Option::TYPE_SIZE, Option::LEVEL_BASIC)
    .set_description("The maximum sequential bandwidth in bytes/second of the OSD (for solid state media)")
    .set_long_description("This option specifies the maximum sequential bandwidth to consider for an OSD whose underlying device type is solid state media. This is considered by the mclock scheduler to derive the cost factor to be used in QoS calculations. Only considered for osd_op_queue = mclock_scheduler")
    .set_default(1200_M)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_mclock_max_capacity_iops_hdd", Option::TYPE_FLOAT, Option::LEVEL_BASIC)
    .set_description("Max random write IOPS capacity (at 4KiB block size) to consider per OSD (for rotational media)")
    .set_long_description("This option specifies the max OSD random write IOPS capacity per OSD. Contributes in QoS calculations when enabling a dmclock profile. Only considered for osd_op_queue = mclock_scheduler")
    .set_default(315.0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_mclock_max_capacity_iops_ssd", Option::TYPE_FLOAT, Option::LEVEL_BASIC)
    .set_description("Max random write IOPS capacity (at 4 KiB block size) to consider per OSD (for solid state media)")
    .set_long_description("This option specifies the max OSD random write IOPS capacity per OSD. Contributes in QoS calculations when enabling a dmclock profile. Only considered for osd_op_queue = mclock_scheduler")
    .set_default(21500.0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_mclock_force_run_benchmark_on_init", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Force run the OSD benchmark on OSD initialization/boot-up")
    .set_long_description("This option specifies whether the OSD benchmark must be run during the OSD boot-up sequence even if historical data about the OSD iops capacity is available in the MON config store. Enable this to refresh the OSD iops capacity if the underlying device's performance characteristics have changed significantly. Only considered for osd_op_queue = mclock_scheduler.")
    .set_default(false)
    .set_flag(Option::FLAG_STARTUP)
    .add_see_also({"osd_mclock_max_capacity_iops_hdd", "osd_mclock_max_capacity_iops_ssd"}),

    Option("osd_mclock_skip_benchmark", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Skip the OSD benchmark on OSD initialization/boot-up")
    .set_long_description("This option specifies whether the OSD benchmark must be skipped during the OSD boot-up sequence. Only considered for osd_op_queue = mclock_scheduler.")
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"osd_mclock_max_capacity_iops_hdd", "osd_mclock_max_capacity_iops_ssd"}),

    Option("osd_mclock_profile", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Which mclock profile to use")
    .set_long_description("This option specifies the mclock profile to enable - one among the set of built-in profiles or a custom profile. Only considered for osd_op_queue = mclock_scheduler")
    .set_default("balanced")
    .set_enum_allowed({"balanced", "high_recovery_ops", "high_client_ops", "custom"})
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"osd_op_queue"}),

    Option("osd_mclock_override_recovery_settings", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Setting this option enables the override of recovery/backfill limits for the mClock scheduler.")
    .set_long_description("This option when set enables the override of the max recovery active and the max backfills limits with mClock scheduler active. These options are not modifiable when mClock scheduler is active. Any attempt to modify these values without setting this option will reset the recovery or backfill option back to its default value.")
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"osd_recovery_max_active_hdd", "osd_recovery_max_active_ssd", "osd_max_backfills"}),

    Option("osd_mclock_iops_capacity_threshold_hdd", Option::TYPE_FLOAT, Option::LEVEL_BASIC)
    .set_description("The threshold IOPs capacity (at 4KiB block size) beyond which to ignore the OSD bench results for an OSD (for rotational media)")
    .set_long_description("This option specifies the threshold IOPS capacity for an OSD under which the OSD bench results can be considered for QoS calculations. Only considered for osd_op_queue = mclock_scheduler")
    .set_default(500.0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_mclock_iops_capacity_threshold_ssd", Option::TYPE_FLOAT, Option::LEVEL_BASIC)
    .set_description("The threshold IOPs capacity (at 4KiB block size) beyond which to ignore the OSD bench results for an OSD (for solid state media)")
    .set_long_description("This option specifies the threshold IOPS capacity for an OSD under which the OSD bench results can be considered for QoS calculations. Only considered for osd_op_queue = mclock_scheduler")
    .set_default(80000.0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_read_ec_check_for_errors", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false),

    Option("osd_recovery_delay_start", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.0),

    Option("osd_recovery_max_active", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of simultaneous active recovery operations per OSD (overrides _ssd and _hdd if non-zero)")
    .set_default(0)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"osd_recovery_max_active_hdd", "osd_recovery_max_active_ssd"}),

    Option("osd_recovery_max_active_hdd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of simultaneous active recovery operations per OSD (for rotational devices)")
    .set_default(3)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"osd_recovery_max_active", "osd_recovery_max_active_ssd"}),

    Option("osd_recovery_max_active_ssd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of simultaneous active recovery operations per OSD (for non-rotational solid state devices)")
    .set_default(10)
    .set_flag(Option::FLAG_RUNTIME)
    .add_see_also({"osd_recovery_max_active", "osd_recovery_max_active_hdd"}),

    Option("osd_recovery_max_single_start", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(1),

    Option("osd_recovery_max_chunk", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(8_M),

    Option("osd_recovery_max_omap_entries_per_chunk", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(8096),

    Option("osd_copyfrom_max_chunk", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(8_M),

    Option("osd_push_per_object_cost", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(1000),

    Option("osd_max_push_cost", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(8_M),

    Option("osd_max_push_objects", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10),

    Option("osd_recover_clone_overlap_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_debug_feed_pullee", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("Feed a pullee, and force primary to pull a currently missing object from it")
    .set_default(-1),

    Option("osd_backfill_scan_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(64),

    Option("osd_backfill_scan_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(512),

    Option("osd_extblkdev_plugins", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("extended block device plugins to load, provide compression feedback at runtime")
    .set_default("vdo")
    .set_flag(Option::FLAG_STARTUP),

    Option("osd_heartbeat_min_peers", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10),

    Option("osd_delete_sleep", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Time in seconds to sleep before next removal transaction. This setting overrides _ssd, _hdd, and _hybrid if non-zero.")
    .set_default(0.0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_delete_sleep_hdd", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Time in seconds to sleep before next removal transaction for HDDs")
    .set_default(5.0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_delete_sleep_ssd", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Time in seconds to sleep before next removal transaction for SSDs")
    .set_default(1.0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_delete_sleep_hybrid", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Time in seconds to sleep before next removal transaction when OSD data is on HDD and OSD journal or WAL+DB is on SSD")
    .set_default(1.0)
    .set_flag(Option::FLAG_RUNTIME),

    Option("osd_rocksdb_iterator_bounds_enabled", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Whether omap iterator bounds are applied to rocksdb iterator ReadOptions")
    .set_default(true),


  });
}
