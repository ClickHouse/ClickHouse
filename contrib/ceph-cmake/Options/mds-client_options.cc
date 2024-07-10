#include "common/options.h"


std::vector<Option> get_mds_client_options() {
  return std::vector<Option>({
    Option("client_cache_size", Option::TYPE_SIZE, Option::LEVEL_BASIC)
    .set_description("soft maximum number of directory entries in client cache")
    .set_default(16_K)
    .add_service("mds_client"),

    Option("client_cache_mid", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("mid-point of client cache LRU")
    .set_default(0.75)
    .add_service("mds_client"),

    Option("client_use_random_mds", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("issue new requests to a random active MDS")
    .set_default(false)
    .add_service("mds_client"),

    Option("client_mount_timeout", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("timeout for mounting CephFS (seconds)")
    .set_default(5_min)
    .add_service("mds_client"),

    Option("client_tick_interval", Option::TYPE_SECS, Option::LEVEL_DEV)
    .set_description("seconds between client upkeep ticks")
    .set_default(1)
    .add_service("mds_client"),

    Option("client_trace", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("file containing trace of client operations")
    .add_service("mds_client"),

    Option("client_readahead_min", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("minimum bytes to readahead in a file")
    .set_default(128_K)
    .add_service("mds_client"),

    Option("client_readahead_max_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("maximum bytes to readahead in a file (zero is unlimited)")
    .set_default(0)
    .add_service("mds_client"),

    Option("client_readahead_max_periods", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("maximum stripe periods to readahead in a file")
    .set_default(4)
    .add_service("mds_client"),

    Option("client_reconnect_stale", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("reconnect when the session becomes stale")
    .set_default(false)
    .add_service("mds_client"),

    Option("client_snapdir", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("pseudo directory for snapshot access to a directory")
    .set_default(".snap")
    .add_service("mds_client"),

    Option("client_mountpoint", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("default mount-point")
    .set_default("/")
    .add_service("mds_client"),

    Option("client_mount_uid", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("uid to mount as")
    .set_default(-1)
    .add_service("mds_client"),

    Option("client_mount_gid", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("gid to mount as")
    .set_default(-1)
    .add_service("mds_client"),

    Option("client_notify_timeout", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(10)
    .add_service("mds_client"),

    Option("osd_client_watch_timeout", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_default(30)
    .add_service("mds_client"),

    Option("client_caps_release_delay", Option::TYPE_SECS, Option::LEVEL_DEV)
    .set_default(5)
    .add_service("mds_client"),

    Option("client_quota_df", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("show quota usage for statfs (df)")
    .set_default(true)
    .add_service("mds_client"),

    Option("client_oc", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("enable object caching")
    .set_default(true)
    .add_service("mds_client"),

    Option("client_oc_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("maximum size of object cache")
    .set_default(200_M)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds_client"),

    Option("client_oc_max_dirty", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("maximum size of dirty pages in object cache")
    .set_default(100_M)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds_client"),

    Option("client_oc_target_dirty", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("target size of dirty pages object cache")
    .set_default(8_M)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds_client"),

    Option("client_oc_max_dirty_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("maximum age of dirty pages in object cache (seconds)")
    .set_default(5.0)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds_client"),

    Option("client_oc_max_objects", Option::TYPE_INT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of objects in cache")
    .set_default(1000)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds_client"),

    Option("client_debug_getattr_caps", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds_client"),

    Option("client_debug_force_sync_read", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds_client"),

    Option("client_debug_inject_tick_delay", Option::TYPE_SECS, Option::LEVEL_DEV)
    .set_default(0)
    .add_service("mds_client"),

    Option("client_max_inline_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_default(4_K)
    .add_service("mds_client"),

    Option("client_inject_release_failure", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds_client"),

    Option("client_inject_fixed_oldest_tid", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds_client"),

    Option("client_metadata", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("metadata key=value comma-delimited pairs appended to session metadata")
    .add_service("mds_client"),

    Option("client_acl_type", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("ACL type to enforce (none or \"posix_acl\")")
    .add_service("mds_client"),

    Option("client_permissions", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("client-enforced permission checking")
    .set_default(true)
    .add_service("mds_client"),

    Option("client_dirsize_rbytes", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("set the directory size as the number of file bytes recursively used")
    .set_long_description("This option enables a CephFS feature that stores the recursive directory size (the bytes used by files in the directory and its descendents) in the st_size field of the stat structure.")
    .set_default(true)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("mds_client"),

    Option("client_force_lazyio", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_default(false)
    .add_service("mds_client"),

    Option("fuse_use_invalidate_cb", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("use fuse 2.8+ invalidate callback to keep page cache consistent")
    .set_default(true)
    .add_service("mds_client"),

    Option("fuse_disable_pagecache", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("disable page caching in the kernel for this FUSE mount")
    .set_default(false)
    .add_service("mds_client"),

    Option("fuse_allow_other", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("pass allow_other to FUSE on mount")
    .set_default(true)
    .add_service("mds_client"),

    Option("fuse_default_permissions", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("pass default_permisions to FUSE on mount")
    .set_default(false)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("mds_client"),

    Option("fuse_splice_read", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("enable splice read to reduce the memory copies")
    .set_default(true)
    .add_service("mds_client"),

    Option("fuse_splice_write", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("enable splice write to reduce the memory copies")
    .set_default(true)
    .add_service("mds_client"),

    Option("fuse_splice_move", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("enable splice move to reduce the memory copies")
    .set_default(true)
    .add_service("mds_client"),

    Option("fuse_big_writes", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("big_writes is deprecated in libfuse 3.0.0")
    .set_default(true)
    .add_service("mds_client"),

    Option("fuse_max_write", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("set the maximum number of bytes in a single write operation")
    .set_long_description("Set the maximum number of bytes in a single write operation that may pass atomically through FUSE. The FUSE default is 128kB and may be indicated by setting this option to 0.")
    .set_default(0)
    .add_service("mds_client"),

    Option("fuse_atomic_o_trunc", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("pass atomic_o_trunc flag to FUSE on mount")
    .set_default(true)
    .add_service("mds_client"),

    Option("fuse_debug", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("enable debugging for the libfuse")
    .set_default(false)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("mds_client"),

    Option("fuse_multithreaded", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("allow parallel processing through FUSE library")
    .set_default(true)
    .add_service("mds_client"),

    Option("fuse_require_active_mds", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("require active MDSs in the file system when mounting")
    .set_default(true)
    .add_service("mds_client"),

    Option("fuse_syncfs_on_mksnap", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("synchronize all local metadata/file changes after snapshot")
    .set_default(true)
    .add_service("mds_client"),

    Option("fuse_set_user_groups", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("check for ceph-fuse to consider supplementary groups for permissions")
    .set_default(true)
    .add_service("mds_client"),

    Option("client_try_dentry_invalidate", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds_client"),

    Option("client_max_retries_on_remount_failure", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of consecutive failed remount attempts for invalidating kernel dcache after which client would abort.")
    .set_default(5)
    .add_service("mds_client"),

    Option("client_die_on_failed_remount", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .add_service("mds_client"),

    Option("client_die_on_failed_dentry_invalidate", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("kill the client when no dentry invalidation options are available")
    .set_long_description("The CephFS client requires a mechanism to invalidate dentries in the caller (e.g. the kernel for ceph-fuse) when capabilities must be recalled. If the client cannot do this then the MDS cache cannot shrink which can cause the MDS to fail.")
    .set_default(true)
    .add_service("mds_client"),

    Option("client_check_pool_perm", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("confirm access to inode's data pool/namespace described in file layout")
    .set_default(true)
    .add_service("mds_client"),

    Option("client_use_faked_inos", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_default(false)
    .set_flag(Option::FLAG_STARTUP)
    .set_flag(Option::FLAG_NO_MON_UPDATE)
    .add_service("mds_client"),

    Option("client_fs", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("CephFS file system name to mount")
    .set_long_description("Use this with ceph-fuse, or with any process that uses libcephfs.  Programs using libcephfs may also pass the filesystem name into mount(), which will override this setting. If no filesystem name is given in mount() or this setting, the default filesystem will be mounted (usually the first created).")
    .set_flag(Option::FLAG_STARTUP)
    .add_service("mds_client"),

    Option("client_mds_namespace", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_flag(Option::FLAG_STARTUP)
    .add_service("mds_client"),

    Option("fake_statfs_for_testing", Option::TYPE_INT, Option::LEVEL_DEV)
    .set_description("Set a value for kb and compute kb_used from total of num_bytes")
    .set_default(0)
    .add_service("mds_client"),

    Option("debug_allow_any_pool_priority", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Allow any pool priority to be set to test conversion to new range")
    .set_default(false)
    .add_service("mds_client"),

    Option("client_asio_thread_count", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Size of thread pool for ASIO completions")
    .set_default(2)
    .set_min(1)
    .add_service("mds_client")
    .add_tag("client"),

    Option("client_shutdown_timeout", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("timeout for shutting down CephFS")
    .set_long_description("Timeout for shutting down CephFS via unmount or shutdown.")
    .set_default(30)
    .set_min(0)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds_client")
    .add_tag("client"),

    Option("client_collect_and_send_global_metrics", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("to enable and force collecting and sending the global metrics to MDS")
    .set_long_description("To be careful for this, when connecting to some old ceph clusters it may crash the MDS daemons while upgrading.")
    .set_default(false)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds_client")
    .add_tag("client"),

    Option("client_quota", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Enable quota enforcement")
    .set_long_description("Enable quota_bytes and quota_files enforcement for the client.")
    .set_default(true)
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("mds_client"),


  });
}
