#include "common/options.h"


std::vector<Option> get_rbd_mirror_options() {
  return std::vector<Option>({
    Option("rbd_mirror_journal_commit_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("commit time interval, seconds")
    .set_default(5.0)
    .add_service("rbd-mirror"),

    Option("rbd_mirror_journal_poll_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("maximum age (in seconds) between successive journal polls")
    .set_default(5.0)
    .add_service("rbd-mirror"),

    Option("rbd_mirror_sync_point_update_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("number of seconds between each update of the image sync point object number")
    .set_default(30.0)
    .add_service("rbd-mirror"),

    Option("rbd_mirror_concurrent_image_syncs", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of image syncs in parallel")
    .set_default(5)
    .add_service("rbd-mirror"),

    Option("rbd_mirror_pool_replayers_refresh_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("interval to refresh peers in rbd-mirror daemon")
    .set_default(30)
    .add_service("rbd-mirror"),

    Option("rbd_mirror_concurrent_image_deletions", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of image deletions in parallel")
    .set_default(1)
    .set_min(1)
    .add_service("rbd-mirror"),

    Option("rbd_mirror_delete_retry_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("interval to check and retry the failed deletion requests")
    .set_default(30.0)
    .add_service("rbd-mirror"),

    Option("rbd_mirror_image_state_check_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("interval to get images from pool watcher and set sources in replayer")
    .set_default(30)
    .set_min(1)
    .add_service("rbd-mirror"),

    Option("rbd_mirror_leader_heartbeat_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("interval (in seconds) between mirror leader heartbeats")
    .set_default(5)
    .set_min(1)
    .add_service("rbd-mirror"),

    Option("rbd_mirror_leader_max_missed_heartbeats", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of missed heartbeats for non-lock owner to attempt to acquire lock")
    .set_default(2)
    .add_service("rbd-mirror"),

    Option("rbd_mirror_leader_max_acquire_attempts_before_break", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of failed attempts to acquire lock after missing heartbeats before breaking lock")
    .set_default(3)
    .add_service("rbd-mirror"),

    Option("rbd_mirror_image_policy_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("active/active policy type for mapping images to instances")
    .set_default("simple")
    .set_enum_allowed({"none", "simple"})
    .add_service("rbd-mirror"),

    Option("rbd_mirror_image_policy_migration_throttle", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of seconds after which an image can be reshuffled (migrated) again")
    .set_default(300)
    .add_service("rbd-mirror"),

    Option("rbd_mirror_image_policy_update_throttle_interval", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("interval (in seconds) to throttle images for mirror daemon peer updates")
    .set_default(1.0)
    .set_min(1.0)
    .add_service("rbd-mirror"),

    Option("rbd_mirror_image_policy_rebalance_timeout", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("number of seconds policy should be idle before triggering reshuffle (rebalance) of images")
    .set_default(0.0)
    .add_service("rbd-mirror"),

    Option("rbd_mirror_perf_stats_prio", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Priority level for mirror daemon replication perf counters")
    .set_long_description("The daemon will send perf counter data to the manager daemon if the priority is not lower than mgr_stats_threshold.")
    .set_default(5)
    .set_min_max(0, 11)
    .add_service("rbd-mirror"),

    Option("rbd_mirror_image_perf_stats_prio", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("Priority level for mirror daemon per-image replication perf counters")
    .set_long_description("The daemon will send per-image perf counter data to the manager daemon if the priority is not lower than mgr_stats_threshold.")
    .set_default(5)
    .set_min_max(0, 11)
    .add_service("rbd-mirror"),

    Option("rbd_mirror_memory_autotune", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Automatically tune the ratio of caches while respecting min values.")
    .set_default(true)
    .add_service("rbd-mirror")
    .add_see_also({"rbd_mirror_memory_target"}),

    Option("rbd_mirror_memory_target", Option::TYPE_SIZE, Option::LEVEL_BASIC)
    .set_description("When tcmalloc and cache autotuning is enabled, try to keep this many bytes mapped in memory.")
    .set_default(4_G)
    .add_service("rbd-mirror")
    .add_see_also({"rbd_mirror_memory_autotune"}),

    Option("rbd_mirror_memory_base", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("When tcmalloc and cache autotuning is enabled, estimate the minimum amount of memory in bytes the rbd-mirror daemon will need.")
    .set_default(768_M)
    .add_service("rbd-mirror")
    .add_see_also({"rbd_mirror_memory_autotune"}),

    Option("rbd_mirror_memory_expected_fragmentation", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("When tcmalloc and cache autotuning is enabled, estimate the percent of memory fragmentation.")
    .set_default(0.15)
    .set_min_max(0.0, 1.0)
    .add_service("rbd-mirror")
    .add_see_also({"rbd_mirror_memory_autotune"}),

    Option("rbd_mirror_memory_cache_min", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("When tcmalloc and cache autotuning is enabled, set the minimum amount of memory used for cache.")
    .set_default(128_M)
    .add_service("rbd-mirror")
    .add_see_also({"rbd_mirror_memory_autotune"}),

    Option("rbd_mirror_memory_cache_resize_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("When tcmalloc and cache autotuning is enabled, wait this many seconds between resizing caches.")
    .set_default(5.0)
    .add_service("rbd-mirror")
    .add_see_also({"rbd_mirror_memory_autotune"}),

    Option("rbd_mirror_memory_cache_autotune_interval", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("The number of seconds to wait between rebalances when cache autotune is enabled.")
    .set_default(30.0)
    .add_service("rbd-mirror")
    .add_see_also({"rbd_mirror_memory_autotune"}),


  });
}
