#include "common/options.h"


std::vector<Option> get_mgr_options() {
  return std::vector<Option>({
    Option("mgr_data", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Filesystem path to the ceph-mgr data directory, used to contain keyring.")
    .set_default("/var/lib/ceph/mgr/$cluster-$id")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .add_service("mgr"),

    Option("mgr_pool", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Allow use/creation of .mgr pool.")
    .set_default(true)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("mgr"),

    Option("mgr_stats_period", Option::TYPE_INT, Option::LEVEL_BASIC)
    .set_description("Period in seconds of OSD/MDS stats reports to manager")
    .set_long_description("Use this setting to control the granularity of time series data collection from daemons.  Adjust upwards if the manager CPU load is too high, or if you simply do not require the most up to date performance counter data.")
    .set_default(5)
    .add_service({"mgr", "common"}),

    Option("mgr_client_bytes", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(128_M)
    .add_service("mgr"),

    Option("mgr_client_messages", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(512)
    .add_service("mgr"),

    Option("mgr_osd_bytes", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(512_M)
    .add_service("mgr"),

    Option("mgr_osd_messages", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(8_K)
    .add_service("mgr"),

    Option("mgr_mds_bytes", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(128_M)
    .add_service("mgr"),

    Option("mgr_mds_messages", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(128)
    .add_service("mgr"),

    Option("mgr_mon_bytes", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(128_M)
    .add_service("mgr"),

    Option("mgr_mon_messages", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_default(128)
    .add_service("mgr"),

    Option("mgr_service_beacon_grace", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Period in seconds from last beacon to manager dropping state about a monitored service (RGW, rbd-mirror etc)")
    .set_default(1_min)
    .add_service("mgr"),

    Option("mgr_debug_aggressive_pg_num_changes", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Bypass most throttling and safety checks in pg[p]_num controller")
    .set_default(false)
    .add_service("mgr"),

    Option("mgr_max_pg_num_change", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("maximum change in pg_num")
    .set_default(128)
    .add_service("mgr"),

    Option("mgr_module_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Filesystem path to manager modules.")
    .set_default("/mgr")
    .add_service("mgr"),

    Option("mgr_standby_modules", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Start modules in standby (redirect) mode when mgr is standby")
    .set_long_description("By default, the standby modules will answer incoming requests with a HTTP redirect to the active manager, allowing users to point their browser at any mgr node and find their way to an active mgr.  However, this mode is problematic when using a load balancer because (1) the redirect locations are usually private IPs and (2) the load balancer can't identify which mgr is the right one to send traffic to. If a load balancer is being used, set this to false.")
    .set_default(true),

    Option("mgr_disabled_modules", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("List of manager modules never get loaded")
    .set_long_description("A comma delimited list of module names. This list is read by manager when it starts. By default, manager loads all modules found in specified 'mgr_module_path', and it starts the enabled ones as instructed. The modules in this list will not be loaded at all.")
    .set_flag(Option::FLAG_STARTUP)
    .add_service("mgr")
    .add_see_also({"mgr_module_path"}),

    Option("mgr_initial_modules", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("List of manager modules to enable when the cluster is first started")
    .set_long_description("This list of module names is read by the monitor when the cluster is first started after installation, to populate the list of enabled manager modules.  Subsequent updates are done using the 'mgr module [enable|disable]' commands.  List may be comma or space separated.")
    .set_default("restful iostat nfs")
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_CLUSTER_CREATE)
    .add_service({"mon", "common"}),

    Option("cephadm_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("Path to cephadm utility")
    .set_default("/usr/sbin/cephadm")
    .add_service("mgr"),

    Option("mon_delta_reset_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("window duration for rate calculations in 'ceph status'")
    .set_default(10.0)
    .add_service("mgr"),

    Option("mon_stat_smooth_intervals", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of PGMaps stats over which we calc the average read/write throughput of the whole cluster")
    .set_default(6)
    .set_min(1)
    .add_service("mgr"),

    Option("mon_pool_quota_warn_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("percent of quota at which to issue warnings")
    .set_default(0)
    .add_service("mgr"),

    Option("mon_pool_quota_crit_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("percent of quota at which to issue errors")
    .set_default(0)
    .add_service("mgr"),

    Option("mon_cache_target_full_warn_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("issue CACHE_POOL_NEAR_FULL health warning when cache pool utilization exceeds this ratio of usable space")
    .set_default(0.66)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_CLUSTER_CREATE)
    .add_service("mgr"),

    Option("mon_pg_check_down_all_threshold", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("threshold of down osds after which we check all pgs")
    .set_default(0.5)
    .add_service("mgr"),

    Option("mon_pg_stuck_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("number of seconds after which pgs can be considered stuck inactive, unclean, etc")
    .set_long_description("see doc/control.rst under dump_stuck for more info")
    .set_default(1_min)
    .add_service("mgr"),

    Option("mon_pg_warn_min_per_osd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("minimal number PGs per (in) osd before we warn the admin")
    .set_default(0)
    .add_service("mgr"),

    Option("mon_pg_warn_max_object_skew", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("max skew few average in objects per pg")
    .set_default(10.0)
    .add_service("mgr"),

    Option("mon_pg_warn_min_objects", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("do not warn below this object #")
    .set_default(10000)
    .add_service("mgr"),

    Option("mon_pg_warn_min_pool_objects", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("do not warn on pools below this object #")
    .set_default(1000)
    .add_service("mgr"),

    Option("mon_warn_on_misplaced", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Issue a health warning if there are misplaced objects")
    .set_default(false)
    .add_service("mgr"),

    Option("mon_warn_on_pool_no_app", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("issue POOL_APP_NOT_ENABLED health warning if pool has not application enabled")
    .set_default(true)
    .add_service("mgr"),

    Option("mon_warn_on_too_few_osds", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Issue a health warning if there are fewer OSDs than osd_pool_default_size")
    .set_default(true)
    .add_service("mgr"),

    Option("mon_target_pg_per_osd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Automated PG management creates this many PGs per OSD")
    .set_long_description("When creating pools, the automated PG management logic will attempt to reach this target.  In some circumstances, it may exceed this target, up to the ``mon_max_pg_per_osd`` limit. Conversely, a lower number of PGs per OSD may be created if the cluster is not yet fully utilised")
    .set_default(100)
    .set_min(1),

    Option("mon_reweight_min_pgs_per_osd", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_default(10)
    .add_service("mgr"),

    Option("mon_reweight_min_bytes_per_osd", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_default(100_M)
    .add_service("mgr"),

    Option("mon_reweight_max_osds", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_default(4)
    .add_service("mgr"),

    Option("mon_reweight_max_change", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_default(0.05)
    .add_service("mgr"),

    Option("mgr_stats_threshold", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Lowest perfcounter priority collected by mgr")
    .set_long_description("Daemons only set perf counter data to the manager daemon if the counter has a priority higher than this.")
    .set_default(5)
    .set_min_max(0, 11),

    Option("mgr_tick_period", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("Period in seconds of beacon messages to monitor")
    .set_default(2)
    .add_service({"mgr", "mon"}),

    Option("mon_osd_err_op_age_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("issue REQUEST_STUCK health error if OSD ops are slower than is age (seconds)")
    .set_default(128.0)
    .add_service("mgr"),


  });
}
