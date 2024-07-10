#include "common/options.h"


std::vector<Option> get_cephfs_mirror_options() {
  return std::vector<Option>({
    Option("cephfs_mirror_max_concurrent_directory_syncs", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of concurrent snapshot synchronization threads")
    .set_long_description("maximum number of directory snapshots that can be synchronized concurrently by cephfs-mirror daemon. Controls the number of synchronization threads.")
    .set_default(3)
    .set_min(1)
    .add_service("cephfs-mirror"),

    Option("cephfs_mirror_action_update_interval", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("interval for driving asynchronous mirror actions")
    .set_long_description("Interval in seconds to process pending mirror update actions.")
    .set_default(2)
    .set_min(1)
    .add_service("cephfs-mirror"),

    Option("cephfs_mirror_restart_mirror_on_blocklist_interval", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("interval to restart blocklisted instances")
    .set_long_description("Interval in seconds to restart blocklisted mirror instances. Setting to zero (0) disables restarting blocklisted instances.")
    .set_default(30)
    .set_min(0)
    .add_service("cephfs-mirror"),

    Option("cephfs_mirror_max_snapshot_sync_per_cycle", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of snapshots to mirror in one cycle")
    .set_long_description("maximum number of snapshots to mirror when a directory is picked up for mirroring by worker threads.")
    .set_default(3)
    .set_min(1)
    .add_service("cephfs-mirror"),

    Option("cephfs_mirror_directory_scan_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("interval to scan directories to mirror snapshots")
    .set_long_description("interval in seconds to scan configured directories for snapshot mirroring.")
    .set_default(10)
    .set_min(1)
    .add_service("cephfs-mirror"),

    Option("cephfs_mirror_max_consecutive_failures_per_directory", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("consecutive failed directory synchronization attempts before marking a directory as \"failed")
    .set_long_description("number of consecutive snapshot synchronization failures to mark a directory as \"failed\". failed directories are retried for synchronization less frequently.")
    .set_default(10)
    .set_min(0)
    .add_service("cephfs-mirror"),

    Option("cephfs_mirror_retry_failed_directories_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("failed directory retry interval for synchronization")
    .set_long_description("interval in seconds to retry synchronization for failed directories.")
    .set_default(60)
    .set_min(1)
    .add_service("cephfs-mirror"),

    Option("cephfs_mirror_restart_mirror_on_failure_interval", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("interval to restart failed mirror instances")
    .set_long_description("Interval in seconds to restart failed mirror instances. Setting to zero (0) disables restarting failed mirror instances.")
    .set_default(20)
    .set_min(0)
    .add_service("cephfs-mirror"),

    Option("cephfs_mirror_mount_timeout", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("timeout for mounting primary/secondary ceph file system")
    .set_long_description("Timeout in seconds for mounting primary or secondary (remote) ceph file system by the cephfs-mirror daemon. Setting this to a higher value could result in the mirror daemon getting stalled when mounting a file system if the cluster is not reachable. This option is used to override the usual client_mount_timeout.")
    .set_default(10)
    .set_min(0)
    .add_service("cephfs-mirror"),


  });
}
