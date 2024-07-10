#include "common/options.h"


std::vector<Option> get_mon_options() {
  return std::vector<Option>({
    Option("osd_crush_update_weight_set", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("update CRUSH weight-set weights when updating weights")
    .set_long_description("If this setting is true, we will update the weight-set weights when adjusting an item's weight, effectively making changes take effect immediately, and discarding any previous optimization in the weight-set value.  Setting this value to false will leave it to the balancer to (slowly, presumably) adjust weights to approach the new target value.")
    .set_default(true),

    Option("osd_pool_erasure_code_stripe_unit", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("the amount of data (in bytes) in a data chunk, per stripe")
    .set_default(4_K)
    .add_service("mon"),

    Option("osd_pool_default_crimson", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Create pools by default with FLAG_CRIMSON")
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon"),

    Option("mon_max_pool_pg_num", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(64_K),

    Option("mon_mgr_digest_period", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("Period in seconds between monitor-to-manager health/status updates")
    .set_default(5)
    .add_service("mon"),

    Option("mon_down_mkfs_grace", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("Period in seconds that the cluster may have a mon down after cluster creation")
    .set_default(1_min)
    .add_service("mon"),

    Option("mon_mgr_beacon_grace", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("Period in seconds from last beacon to monitor marking a manager daemon as failed")
    .set_default(30)
    .add_service("mon"),

    Option("mon_mgr_inactive_grace", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Period in seconds after cluster creation during which cluster may have no active manager")
    .set_long_description("This grace period enables the cluster to come up cleanly without raising spurious health check failures about managers that aren't online yet")
    .set_default(1_min)
    .add_service("mon"),

    Option("mon_mgr_mkfs_grace", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Period in seconds that the cluster may have no active manager before this is reported as an ERR rather than a WARN")
    .set_default(2_min)
    .add_service("mon"),

    Option("mon_mgr_proxy_client_bytes_ratio", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("ratio of mon_client_bytes that can be consumed by proxied mgr commands before we error out to client")
    .set_default(0.3)
    .add_service("mon"),

    Option("mon_cluster_log_to_stderr", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Make monitor send cluster log messages to stderr (prefixed by channel)")
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .add_see_also({"log_stderr_prefix"}),

    Option("mon_cluster_log_to_syslog", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Make monitor send cluster log messages to syslog")
    .set_default("default=false")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon"),

    Option("mon_cluster_log_to_syslog_level", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Syslog level for cluster log messages")
    .set_default("info")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .add_see_also({"mon_cluster_log_to_syslog"}),

    Option("mon_cluster_log_to_syslog_facility", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Syslog facility for cluster log messages")
    .set_default("daemon")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .add_see_also({"mon_cluster_log_to_syslog"}),

    Option("mon_cluster_log_to_file", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Make monitor send cluster log messages to file")
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .add_see_also({"mon_cluster_log_file"}),

    Option("mon_cluster_log_file", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("File(s) to write cluster log to")
    .set_long_description("This can either be a simple file name to receive all messages, or a list of key/value pairs where the key is the log channel and the value is the filename, which may include $cluster and $channel metavariables")
    .set_default("default=/var/log/ceph/$cluster.$channel.log cluster=/var/log/ceph/$cluster.log")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .add_see_also({"mon_cluster_log_to_file"}),

    Option("mon_cluster_log_file_level", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Lowest level to include is cluster log file")
    .set_default("debug")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .add_see_also({"mon_cluster_log_file"}),

    Option("mon_cluster_log_to_graylog", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Make monitor send cluster log to graylog")
    .set_default("false")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon"),

    Option("mon_cluster_log_to_graylog_host", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Graylog host for cluster log messages")
    .set_default("127.0.0.1")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .add_see_also({"mon_cluster_log_to_graylog"}),

    Option("mon_cluster_log_to_graylog_port", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Graylog port for cluster log messages")
    .set_default("12201")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .add_see_also({"mon_cluster_log_to_graylog"}),

    Option("mon_cluster_log_to_journald", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Make monitor send cluster log to journald")
    .set_default("false")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon"),

    Option("mon_log_max", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of recent cluster log messages to retain")
    .set_default(10000)
    .add_service("mon"),

    Option("mon_log_max_summary", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of recent cluster log messages to dedup against")
    .set_default(50)
    .add_service("mon"),

    Option("mon_log_full_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("how many epochs before we encode a full copy of recent log keys")
    .set_default(50)
    .add_service("mon"),

    Option("mon_max_log_entries_per_event", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("max cluster log entries per paxos event")
    .set_default(4096)
    .add_service("mon"),

    Option("mon_health_to_clog", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("log monitor health to cluster log")
    .set_default(true)
    .add_service("mon"),

    Option("mon_health_to_clog_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("frequency to log monitor health to cluster log")
    .set_default(10_min)
    .add_service("mon")
    .add_see_also({"mon_health_to_clog"}),

    Option("mon_health_to_clog_tick_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_default(1_min)
    .add_service("mon"),

    Option("mon_health_detail_to_clog", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("log health detail to cluster log")
    .set_default(true),

    Option("mon_warn_on_filestore_osds", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("log health warn for filestore OSDs")
    .set_default(true),

    Option("mon_health_max_detail", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("max detailed pgs to report in health detail")
    .set_default(50)
    .add_service("mon"),

    Option("mon_health_log_update_period", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("minimum time in seconds between log messages about each health check")
    .set_default(5)
    .set_min(0)
    .add_service("mon"),

    Option("mon_data_avail_crit", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("issue MON_DISK_CRIT health error when mon available space below this percentage")
    .set_default(5)
    .add_service("mon"),

    Option("mon_data_avail_warn", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("issue MON_DISK_LOW health warning when mon available space below this percentage")
    .set_default(30)
    .add_service("mon"),

    Option("mon_data_size_warn", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("issue MON_DISK_BIG health warning when mon database is above this size")
    .set_default(15_G)
    .add_service("mon"),

    Option("mon_daemon_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("max bytes of outstanding mon messages mon will read off the network")
    .set_default(400_M)
    .add_service("mon"),

    Option("mon_election_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("maximum time for a mon election (seconds)")
    .set_default(5.0)
    .add_service("mon"),

    Option("mon_election_default_strategy", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("The election strategy to set when constructing the first monmap.")
    .set_default(1)
    .set_min_max(1, 3),

    Option("mon_lease", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("lease interval between quorum monitors (seconds)")
    .set_long_description("This setting controls how sensitive your mon quorum is to intermittent network issues or other failures.")
    .set_default(5.0)
    .add_service("mon"),

    Option("mon_lease_renew_interval_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("multiple of mon_lease for the lease renewal interval")
    .set_long_description("Leases must be renewed before they time out.  A smaller value means frequent renewals, while a value close to 1 makes a lease expiration more likely.")
    .set_default(0.6)
    .set_min_max(0.0, 0.9999999)
    .add_service("mon")
    .add_see_also({"mon_lease"}),

    Option("mon_lease_ack_timeout_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("multiple of mon_lease for the lease ack interval before calling new election")
    .set_default(2.0)
    .set_min_max(1.0001, 100.0)
    .add_service("mon")
    .add_see_also({"mon_lease"}),

    Option("mon_accept_timeout_factor", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("multiple of mon_lease for follower mons to accept proposed state changes before calling a new election")
    .set_default(2.0)
    .add_service("mon")
    .add_see_also({"mon_lease"}),

    Option("mon_elector_ping_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("The time after which a ping 'times out' and a connection is considered down")
    .set_default(2.0)
    .add_service("mon")
    .add_see_also({"mon_elector_ping_divisor"}),

    Option("mon_elector_ping_divisor", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("We will send a ping up to this many times per timeout per")
    .set_default(2)
    .add_service("mon")
    .add_see_also({"mon_elector_ping_timeout"}),

    Option("mon_con_tracker_persist_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("how many updates the ConnectionTracker takes before it persists to disk")
    .set_default(10)
    .set_min_max(1, 100000)
    .add_service("mon"),

    Option("mon_con_tracker_score_halflife", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("The 'halflife' used when updating/calculating peer connection scores")
    .set_default(43200)
    .set_min(60)
    .add_service("mon"),

    Option("mon_elector_ignore_propose_margin", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("The difference in connection score allowed before a peon stops ignoring out-of-quorum PROPOSEs")
    .set_default(0.0005)
    .add_service("mon"),

    Option("mon_warn_on_cache_pools_without_hit_sets", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("issue CACHE_POOL_NO_HIT_SET health warning for cache pools that do not have hit sets configured")
    .set_default(true)
    .add_service("mon"),

    Option("mon_warn_on_pool_pg_num_not_power_of_two", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("issue POOL_PG_NUM_NOT_POWER_OF_TWO warning if pool has a non-power-of-two pg_num value")
    .set_default(true)
    .add_service("mon"),

    Option("mon_allow_pool_size_one", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("allow configuring pool with no replicas")
    .set_default(false)
    .add_service("mon"),

    Option("mon_warn_on_crush_straw_calc_version_zero", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("issue OLD_CRUSH_STRAW_CALC_VERSION health warning if the CRUSH map's straw_calc_version is zero")
    .set_default(true)
    .add_service("mon"),

    Option("mon_warn_on_pool_no_redundancy", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Issue a health warning if any pool is configured with no replicas")
    .set_default(true)
    .add_service("mon")
    .add_see_also({"osd_pool_default_size", "osd_pool_default_min_size"}),

    Option("mon_warn_on_osd_down_out_interval_zero", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("issue OSD_NO_DOWN_OUT_INTERVAL health warning if mon_osd_down_out_interval is zero")
    .set_long_description("Having mon_osd_down_out_interval set to 0 means that down OSDs are not marked out automatically and the cluster does not heal itself without administrator intervention.")
    .set_default(true)
    .add_service("mon")
    .add_see_also({"mon_osd_down_out_interval"}),

    Option("mon_warn_on_legacy_crush_tunables", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("issue OLD_CRUSH_TUNABLES health warning if CRUSH tunables are older than mon_crush_min_required_version")
    .set_default(true)
    .add_service("mon")
    .add_see_also({"mon_crush_min_required_version"}),

    Option("mon_crush_min_required_version", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("minimum ceph release to use for mon_warn_on_legacy_crush_tunables")
    .set_default("hammer")
    .add_service("mon")
    .add_see_also({"mon_warn_on_legacy_crush_tunables"}),

    Option("mon_warn_on_degraded_stretch_mode", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Issue a health warning if we are in degraded stretch mode")
    .set_default(true)
    .add_service("mon"),

    Option("mon_stretch_cluster_recovery_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("the ratio of up OSDs at which a degraded stretch cluster enters recovery")
    .set_default(0.6)
    .set_min_max(0.51, 1.0)
    .add_service("mon"),

    Option("mon_stretch_recovery_min_wait", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("how long the monitors wait before considering fully-healthy PGs as evidence the stretch mode is repaired")
    .set_default(15.0)
    .set_min(1.0)
    .add_service("mon"),

    Option("mon_stretch_pool_size", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(4)
    .set_min_max(3, 6)
    .add_service("mon"),

    Option("mon_stretch_pool_min_size", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(2)
    .set_min_max(2, 4)
    .add_service("mon"),

    Option("mon_clock_drift_allowed", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("allowed clock drift (in seconds) between mons before issuing a health warning")
    .set_default(0.05)
    .add_service("mon"),

    Option("mon_clock_drift_warn_backoff", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("exponential backoff factor for logging clock drift warnings in the cluster log")
    .set_default(5.0)
    .add_service("mon"),

    Option("mon_timecheck_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("frequency of clock synchronization checks between monitors (seconds)")
    .set_default(5_min)
    .add_service("mon"),

    Option("mon_timecheck_skew_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("frequency of clock synchronization (re)checks between monitors while clocks are believed to be skewed (seconds)")
    .set_default(30.0)
    .add_service("mon")
    .add_see_also({"mon_timecheck_interval"}),

    Option("paxos_stash_full_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(25)
    .add_service("mon"),

    Option("paxos_max_join_drift", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .add_service("mon"),

    Option("paxos_propose_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(1.0)
    .add_service("mon"),

    Option("paxos_min_wait", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.05)
    .add_service("mon"),

    Option("paxos_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .add_service("mon"),

    Option("paxos_trim_min", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(250)
    .add_service("mon"),

    Option("paxos_trim_max", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .add_service("mon"),

    Option("paxos_service_trim_min", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(250)
    .add_service("mon"),

    Option("paxos_service_trim_max", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(500)
    .add_service("mon"),

    Option("paxos_service_trim_max_multiplier", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("factor by which paxos_service_trim_max will be multiplied to get a new upper bound when trim sizes are high  (0 disables it)")
    .set_default(20)
    .set_min(0)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon"),

    Option("paxos_kill_at", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mon"),

    Option("mon_auth_validate_all_caps", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Whether to parse non-monitor capabilities set by the 'ceph auth ...' commands. Disabling this saves CPU on the monitor, but allows invalid capabilities to be set, and only be rejected later, when they are used.")
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon"),

    Option("mon_mds_force_trim_to", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("force mons to trim mdsmaps/fsmaps up to this epoch")
    .set_default(0)
    .add_service("mon"),

    Option("mds_beacon_mon_down_grace", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("tolerance in seconds for missed MDS beacons to monitors")
    .set_default(1_min),

    Option("mon_mds_skip_sanity", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("skip sanity checks on fsmap/mdsmap")
    .set_default(false)
    .add_service("mon"),

    Option("mon_mds_blocklist_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("Duration in seconds that blocklist entries for MDS daemons remain in the OSD map")
    .set_default(1_day)
    .set_min(1_hr)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon"),

    Option("mon_mgr_blocklist_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("Duration in seconds that blocklist entries for mgr daemons remain in the OSD map")
    .set_default(1_day)
    .set_min(1_hr)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon"),

    Option("mon_osd_laggy_halflife", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("halflife of OSD 'lagginess' factor")
    .set_default(1_hr)
    .add_service("mon"),

    Option("mon_osd_laggy_weight", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("how heavily to weight OSD marking itself back up in overall laggy_probability")
    .set_long_description("1.0 means that an OSD marking itself back up (because it was marked down but not actually dead) means a 100% laggy_probability; 0.0 effectively disables tracking of laggy_probability.")
    .set_default(0.3)
    .set_min_max(0.0, 1.0)
    .add_service("mon"),

    Option("mon_osd_laggy_max_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("cap value for period for OSD to be marked for laggy_interval calculation")
    .set_default(5_min)
    .add_service("mon"),

    Option("mon_osd_adjust_heartbeat_grace", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("increase OSD heartbeat grace if peers appear to be laggy")
    .set_long_description("If an OSD is marked down but then marks itself back up, it implies it wasn't actually down but was unable to respond to heartbeats.  If this option is true, we can use the laggy_probability and laggy_interval values calculated to model this situation to increase the heartbeat grace period for this OSD so that it isn't marked down again.  laggy_probability is an estimated probability that the given OSD is down because it is laggy (not actually down), and laggy_interval is an estiate on how long it stays down when it is laggy.")
    .set_default(true)
    .add_service("mon")
    .add_see_also({"mon_osd_laggy_halflife", "mon_osd_laggy_weight", "mon_osd_laggy_max_interval"}),

    Option("mon_osd_adjust_down_out_interval", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("increase the mon_osd_down_out_interval if an OSD appears to be laggy")
    .set_default(true)
    .add_service("mon")
    .add_see_also({"mon_osd_adjust_heartbeat_grace"}),

    Option("mon_osd_auto_mark_in", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("mark any OSD that comes up 'in'")
    .set_default(false)
    .add_service("mon"),

    Option("mon_osd_auto_mark_auto_out_in", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("mark any OSD that comes up that was automatically marked 'out' back 'in'")
    .set_default(true)
    .add_service("mon")
    .add_see_also({"mon_osd_down_out_interval"}),

    Option("mon_osd_auto_mark_new_in", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("mark any new OSD that comes up 'in'")
    .set_default(true)
    .add_service("mon"),

    Option("mon_osd_destroyed_out_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("mark any OSD 'out' that has been 'destroy'ed for this long (seconds)")
    .set_default(10_min)
    .add_service("mon"),

    Option("mon_osd_down_out_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("mark any OSD 'out' that has been 'down' for this long (seconds)")
    .set_default(10_min)
    .add_service("mon"),

    Option("mon_osd_down_out_subtree_limit", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("do not automatically mark OSDs 'out' if an entire subtree of this size is down")
    .set_default("rack")
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon")
    .add_see_also({"mon_osd_down_out_interval"}),

    Option("mon_osd_min_up_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("do not automatically mark OSDs 'out' if fewer than this many OSDs are 'up'")
    .set_default(0.3)
    .add_service("mon")
    .add_see_also({"mon_osd_down_out_interval"}),

    Option("mon_osd_min_in_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("do not automatically mark OSDs 'out' if fewer than this many OSDs are 'in'")
    .set_default(0.75)
    .add_service("mon")
    .add_see_also({"mon_osd_down_out_interval"}),

    Option("mon_osd_warn_op_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("issue REQUEST_SLOW health warning if OSD ops are slower than this age (seconds)")
    .set_default(32.0)
    .add_service("mgr"),

    Option("mon_osd_warn_num_repaired", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("issue OSD_TOO_MANY_REPAIRS health warning if an OSD has more than this many read repairs")
    .set_default(10)
    .add_service("mon"),

    Option("mon_osd_prime_pg_temp", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("minimize peering work by priming pg_temp values after a map change")
    .set_default(true)
    .add_service("mon"),

    Option("mon_osd_prime_pg_temp_max_time", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("maximum time to spend precalculating PG mappings on map change (seconds)")
    .set_default(0.5)
    .add_service("mon"),

    Option("mon_osd_prime_pg_temp_max_estimate", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("calculate all PG mappings if estimated fraction of PGs that change is above this amount")
    .set_default(0.25)
    .add_service("mon"),

    Option("mon_osd_blocklist_default_expire", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Duration in seconds that blocklist entries for clients remain in the OSD map")
    .set_default(1_hr)
    .add_service("mon"),

    Option("mon_osd_crush_smoke_test", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("perform a smoke test on any new CRUSH map before accepting changes")
    .set_default(true)
    .add_service("mon"),

    Option("mon_smart_report_timeout", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Timeout (in seconds) for smartctl to run, default is set to 5")
    .set_default(5)
    .add_service("mon"),

    Option("mon_warn_on_older_version", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("issue DAEMON_OLD_VERSION health warning if daemons are not all running the same version")
    .set_default(true)
    .add_service("mon"),

    Option("mon_warn_older_version_delay", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("issue DAEMON_OLD_VERSION health warning after this amount of time has elapsed")
    .set_default(7_day)
    .add_service("mon"),

    Option("mon_data", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("path to mon database")
    .set_default("/var/lib/ceph/mon/$cluster-$id")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .add_service("mon"),

    Option("mon_rocksdb_options", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_default("write_buffer_size=33554432,compression=kNoCompression,level_compaction_dynamic_level_bytes=true"),

    Option("mon_enable_op_tracker", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("enable/disable MON op tracking")
    .set_default(true)
    .add_service("mon"),

    Option("mon_compact_on_start", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .add_service("mon"),

    Option("mon_compact_on_bootstrap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .add_service("mon"),

    Option("mon_compact_on_trim", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(true)
    .add_service("mon"),

    Option("mon_op_complaint_time", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("time after which to consider a monitor operation blocked after no updates")
    .set_default(30)
    .add_service("mon"),

    Option("mon_op_log_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("max number of slow ops to display")
    .set_default(5)
    .add_service("mon"),

    Option("mon_op_history_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("max number of completed ops to track")
    .set_default(20)
    .add_service("mon"),

    Option("mon_op_history_duration", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("expiration time in seconds of historical MON OPS")
    .set_default(10_min)
    .add_service("mon"),

    Option("mon_op_history_slow_op_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("max number of slow historical MON OPS to keep")
    .set_default(20)
    .add_service("mon"),

    Option("mon_op_history_slow_op_threshold", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("duration of an op to be considered as a historical slow op")
    .set_default(10)
    .add_service("mon"),

    Option("mon_osdmap_full_prune_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("enables pruning full osdmap versions when we go over a given number of maps")
    .set_default(true)
    .add_service("mon")
    .add_see_also({"mon_osdmap_full_prune_min", "mon_osdmap_full_prune_interval", "mon_osdmap_full_prune_txsize"}),

    Option("mon_osdmap_full_prune_min", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("minimum number of versions in the store to trigger full map pruning")
    .set_default(10000)
    .add_service("mon")
    .add_see_also({"mon_osdmap_full_prune_enabled", "mon_osdmap_full_prune_interval", "mon_osdmap_full_prune_txsize"}),

    Option("mon_osdmap_full_prune_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("interval between maps that will not be pruned; maps in the middle will be pruned.")
    .set_default(10)
    .add_service("mon")
    .add_see_also({"mon_osdmap_full_prune_enabled", "mon_osdmap_full_prune_interval", "mon_osdmap_full_prune_txsize"}),

    Option("mon_osdmap_full_prune_txsize", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of maps we will prune per iteration")
    .set_default(100)
    .add_service("mon")
    .add_see_also({"mon_osdmap_full_prune_enabled", "mon_osdmap_full_prune_interval", "mon_osdmap_full_prune_txsize"}),

    Option("mon_osd_cache_size", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of OSDMaps to cache in memory")
    .set_default(500)
    .add_service("mon"),

    Option("mon_osd_cache_size_min", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("The minimum amount of bytes to be kept mapped in memory for osd monitor caches.")
    .set_default(128_M)
    .add_service("mon"),

    Option("mon_osd_mapping_pgs_per_chunk", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("granularity of PG placement calculation background work")
    .set_default(4096)
    .add_service("mon"),

    Option("mon_clean_pg_upmaps_per_chunk", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("granularity of PG upmap validation background work")
    .set_default(256)
    .add_service("mon"),

    Option("mon_osd_max_creating_pgs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of PGs the mon will create at once")
    .set_default(1024)
    .add_service("mon"),

    Option("mon_osd_max_initial_pgs", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of PGs a pool will created with")
    .set_long_description("If the user specifies more PGs than this, the cluster will subsequently split PGs after the pool is created in order to reach the target.")
    .set_default(1024)
    .add_service("mon"),

    Option("mon_memory_target", Option::TYPE_SIZE, Option::LEVEL_BASIC)
    .set_description("The amount of bytes pertaining to osd monitor caches and kv cache to be kept mapped in memory with cache auto-tuning enabled")
    .set_default(2_G)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon"),

    Option("mon_memory_autotune", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_description("Autotune the cache memory being used for osd monitors and kv database")
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mon"),

    Option("mon_cpu_threads", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("worker threads for CPU intensive background work")
    .set_default(4)
    .add_service("mon"),

    Option("mon_tick_interval", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("interval for internal mon background checks")
    .set_default(5)
    .add_service("mon"),

    Option("mon_session_timeout", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("close inactive mon client connections after this many seconds")
    .set_default(5_min)
    .add_service("mon"),

    Option("mon_subscribe_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("subscribe interval for pre-jewel clients")
    .set_default(1_day)
    .add_service("mon"),

    Option("mon_use_min_delay_socket", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("priority packets between mons")
    .set_default(false)
    .add_see_also({"osd_heartbeat_use_min_delay_socket"}),


  });
}
