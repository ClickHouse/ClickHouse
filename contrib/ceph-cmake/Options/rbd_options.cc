#include "common/options.h"
#include <bit>
// rbd feature and io operation validation
#include "include/stringify.h"
#include "common/strtol.h"
#include "common/ceph_regex.h"
#include "librbd/Features.h"
#include "librbd/io/IoOperations.h"


std::vector<Option> get_rbd_options() {
  return std::vector<Option>({
    Option("rbd_default_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("default pool for storing new images")
    .set_default("rbd")
    .set_validator([](std::string *value, std::string *error_message) {
      if (!ceph_regex_match(*value, "^[^@/]+$")) {
        *value = "rbd";
        *error_message = "invalid RBD default pool, resetting to 'rbd'";
      }
      return 0;
    })
    .add_service("rbd"),

    Option("rbd_default_data_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("default pool for storing data blocks for new images")
    .set_validator([](std::string *value, std::string *error_message) {
      if (!ceph_regex_match(*value, "^[^@/]*$")) {
        *value = "";
        *error_message = "ignoring invalid RBD data pool";
      }
      return 0;
    })
    .add_service("rbd"),

    Option("rbd_default_features", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("default v2 image features for new images")
    .set_long_description("RBD features are only applicable for v2 images. This setting accepts either an integer bitmask value or comma-delimited string of RBD feature names. This setting is always internally stored as an integer bitmask value. The mapping between feature bitmask value and feature name is as follows: +1 -> layering, +2 -> striping, +4 -> exclusive-lock, +8 -> object-map, +16 -> fast-diff, +32 -> deep-flatten, +64 -> journaling, +128 -> data-pool")
    .set_default("layering,exclusive-lock,object-map,fast-diff,deep-flatten")
    .set_validator([](std::string *value, std::string *error_message) {
      std::stringstream ss;
      uint64_t features = librbd::rbd_features_from_string(*value, &ss);
      // Leave this in integer form to avoid breaking Cinder.  Someday
      // we would like to present this in string form instead...
      *value = stringify(features);
      if (ss.str().size()) {
        return -EINVAL;
      }
      return 0;
    })
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("rbd"),

    Option("rbd_op_threads", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of threads to utilize for internal processing")
    .set_default(1)
    .add_service("rbd"),

    Option("rbd_op_thread_timeout", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("time in seconds for detecting a hung thread")
    .set_default(60)
    .add_service("rbd"),

    Option("rbd_disable_zero_copy_writes", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("Disable the use of zero-copy writes to ensure unstable writes from clients cannot cause a CRC mismatch")
    .set_default(true)
    .add_service("rbd"),

    Option("rbd_non_blocking_aio", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("process AIO ops from a dispatch thread to prevent blocking")
    .set_default(true)
    .add_service("rbd"),

    Option("rbd_cache", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("whether to enable caching (writeback unless rbd_cache_max_dirty is 0)")
    .set_default(true)
    .add_service("rbd"),

    Option("rbd_cache_policy", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("cache policy for handling writes.")
    .set_default("writearound")
    .set_enum_allowed({"writethrough", "writeback", "writearound"})
    .add_service("rbd"),

    Option("rbd_cache_writethrough_until_flush", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("whether to make writeback caching writethrough until flush is called, to be sure the user of librbd will send flushes so that writeback is safe")
    .set_default(true)
    .add_service("rbd"),

    Option("rbd_cache_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("cache size in bytes")
    .set_default(32_M)
    .add_service("rbd"),

    Option("rbd_cache_max_dirty", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("dirty limit in bytes - set to 0 for write-through caching")
    .set_default(24_M)
    .add_service("rbd"),

    Option("rbd_cache_target_dirty", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("target dirty limit in bytes")
    .set_default(16_M)
    .add_service("rbd"),

    Option("rbd_cache_max_dirty_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("seconds in cache before writeback starts")
    .set_default(1.0)
    .add_service("rbd"),

    Option("rbd_cache_max_dirty_object", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("dirty limit for objects - set to 0 for auto calculate from rbd_cache_size")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_cache_block_writes_upfront", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("whether to block writes to the cache before the aio_write call completes")
    .set_default(false)
    .add_service("rbd"),

    Option("rbd_parent_cache_enabled", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("whether to enable rbd shared ro cache")
    .set_default(false)
    .add_service("rbd"),

    Option("rbd_concurrent_management_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("how many operations can be in flight for a management operation like deleting or resizing an image")
    .set_default(10)
    .set_min(1)
    .add_service("rbd"),

    Option("rbd_balance_snap_reads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("distribute snap read requests to random OSD")
    .set_default(false)
    .add_service("rbd")
    .add_see_also({"rbd_read_from_replica_policy"}),

    Option("rbd_localize_snap_reads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("localize snap read requests to closest OSD")
    .set_default(false)
    .add_service("rbd")
    .add_see_also({"rbd_read_from_replica_policy"}),

    Option("rbd_balance_parent_reads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("distribute parent read requests to random OSD")
    .set_default(false)
    .add_service("rbd")
    .add_see_also({"rbd_read_from_replica_policy"}),

    Option("rbd_localize_parent_reads", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("localize parent requests to closest OSD")
    .set_default(false)
    .add_service("rbd")
    .add_see_also({"rbd_read_from_replica_policy"}),

    Option("rbd_sparse_read_threshold_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("threshold for issuing a sparse-read")
    .set_long_description("minimum number of sequential bytes to read against an object before issuing a sparse-read request to the cluster. 0 implies it must be a full object read to issue a sparse-read, 1 implies always use sparse-read, and any value larger than the maximum object size will disable sparse-read for all requests")
    .set_default(64_K)
    .add_service("rbd"),

    Option("rbd_readahead_trigger_requests", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of sequential requests necessary to trigger readahead")
    .set_default(10)
    .add_service("rbd"),

    Option("rbd_readahead_max_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("set to 0 to disable readahead")
    .set_default(512_K)
    .add_service("rbd"),

    Option("rbd_readahead_disable_after_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("how many bytes are read in total before readahead is disabled")
    .set_default(50_M)
    .add_service("rbd"),

    Option("rbd_clone_copy_on_read", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("copy-up parent image blocks to clone upon read request")
    .set_default(false)
    .add_service("rbd"),

    Option("rbd_blocklist_on_break_lock", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("whether to blocklist clients whose lock was broken")
    .set_default(true)
    .add_service("rbd"),

    Option("rbd_blocklist_expire_seconds", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of seconds to blocklist - set to 0 for OSD default")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_request_timed_out_seconds", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of seconds before maintenance request times out")
    .set_default(30)
    .add_service("rbd"),

    Option("rbd_skip_partial_discard", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("skip discard (zero) of unaligned extents within an object")
    .set_default(true)
    .add_service("rbd"),

    Option("rbd_discard_granularity_bytes", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("minimum aligned size of discard operations")
    .set_default(64_K)
    .set_min_max(4_K, 32_M)
    .set_validator([](std::string *value, std::string *error_message) {
      uint64_t f = strict_si_cast<uint64_t>(*value, error_message);
      if (!error_message->empty()) {
        return -EINVAL;
      } else if (!std::has_single_bit(f)) {
        *error_message = "value must be a power of two";
        return -EINVAL;
      }
      return 0;
    })
    .add_service("rbd"),

    Option("rbd_enable_alloc_hint", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("when writing a object, it will issue a hint to osd backend to indicate the expected size object need")
    .set_default(true)
    .add_service("rbd"),

    Option("rbd_compression_hint", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("Compression hint to send to the OSDs during writes")
    .set_default("none")
    .set_enum_allowed({"none", "compressible", "incompressible"})
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("rbd"),

    Option("rbd_read_from_replica_policy", Option::TYPE_STR, Option::LEVEL_BASIC)
    .set_description("Read replica policy send to the OSDS during reads")
    .set_default("default")
    .set_enum_allowed({"default", "balance", "localize"})
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("rbd"),

    Option("rbd_tracing", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("true if LTTng-UST tracepoints should be enabled")
    .set_default(false)
    .add_service("rbd"),

    Option("rbd_blkin_trace_all", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("create a blkin trace for all RBD requests")
    .set_default(false)
    .add_service("rbd"),

    Option("rbd_validate_pool", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("validate empty pools for RBD compatibility")
    .set_default(true)
    .add_service("rbd"),

    Option("rbd_validate_names", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("validate new image names for RBD compatibility")
    .set_default(true)
    .add_service("rbd"),

    Option("rbd_invalidate_object_map_on_timeout", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("true if object map should be invalidated when load or update timeout")
    .set_default(true)
    .add_service("rbd"),

    Option("rbd_auto_exclusive_lock_until_manual_request", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("automatically acquire/release exclusive lock until it is explicitly requested")
    .set_default(true)
    .add_service("rbd"),

    Option("rbd_move_to_trash_on_remove", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_description("automatically move images to the trash when deleted")
    .set_default(false)
    .add_service("rbd"),

    Option("rbd_move_to_trash_on_remove_expire_seconds", Option::TYPE_UINT, Option::LEVEL_BASIC)
    .set_description("default number of seconds to protect deleted images in the trash")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_move_parent_to_trash_on_remove", Option::TYPE_BOOL, Option::LEVEL_BASIC)
    .set_description("move parent with clone format v2 children to the trash when deleted")
    .set_default(false)
    .add_service("rbd"),

    Option("rbd_mirroring_resync_after_disconnect", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("automatically start image resync after mirroring is disconnected due to being laggy")
    .set_default(false)
    .add_service("rbd"),

    Option("rbd_mirroring_delete_delay", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("time-delay in seconds for rbd-mirror delete propagation")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_mirroring_replay_delay", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("time-delay in seconds for rbd-mirror asynchronous replication")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_mirroring_max_mirroring_snapshots", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("mirroring snapshots limit")
    .set_default(5)
    .set_min(3)
    .add_service("rbd"),

    Option("rbd_default_format", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("default image format for new images")
    .set_default(2)
    .add_service("rbd"),

    Option("rbd_default_order", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("default order (data block object size) for new images")
    .set_long_description("This configures the default object size for new images. The value is used as a power of two, meaning ``default_object_size = 2 ^ rbd_default_order``. Configure a value between 12 and 25 (inclusive), translating to 4KiB lower and 32MiB upper limit.")
    .set_default(22)
    .add_service("rbd"),

    Option("rbd_default_stripe_count", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("default stripe count for new images")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_default_stripe_unit", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("default stripe width for new images")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_default_map_options", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("default krbd map options")
    .add_service("rbd"),

    Option("rbd_default_clone_format", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("default internal format for handling clones")
    .set_long_description("This sets the internal format for tracking cloned images. The setting of '1' requires attaching to protected snapshots that cannot be removed until the clone is removed/flattened. The setting of '2' will allow clones to be attached to any snapshot and permits removing in-use parent snapshots but requires Mimic or later clients. The default setting of 'auto' will use the v2 format if the cluster is configured to require mimic or later clients.")
    .set_default("auto")
    .set_enum_allowed({"1", "2", "auto"})
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("rbd"),

    Option("rbd_journal_order", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("default order (object size) for journal data objects")
    .set_default(24)
    .set_min_max(12, 26)
    .add_service("rbd"),

    Option("rbd_journal_splay_width", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("number of active journal objects")
    .set_default(4)
    .add_service("rbd"),

    Option("rbd_journal_commit_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("commit time interval, seconds")
    .set_default(5.0)
    .add_service("rbd"),

    Option("rbd_journal_object_writethrough_until_flush", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("when enabled, the rbd_journal_object_flush* configuration options are ignored until the first flush so that batched journal IO is known to be safe for consistency")
    .set_default(true)
    .add_service("rbd"),

    Option("rbd_journal_object_flush_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of pending commits per journal object")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_journal_object_flush_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("maximum number of pending bytes per journal object")
    .set_default(1_M)
    .add_service("rbd"),

    Option("rbd_journal_object_flush_age", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("maximum age (in seconds) for pending commits")
    .set_default(0.0)
    .add_service("rbd"),

    Option("rbd_journal_object_max_in_flight_appends", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of in-flight appends per journal object")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_journal_pool", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("pool for journal objects")
    .add_service("rbd"),

    Option("rbd_journal_max_payload_bytes", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("maximum journal payload size before splitting")
    .set_default(16_K)
    .add_service("rbd"),

    Option("rbd_journal_max_concurrent_object_sets", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum number of object sets a journal client can be behind before it is automatically unregistered")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_qos_iops_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired limit of IO operations per second")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_qos_bps_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired limit of IO bytes per second")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_qos_read_iops_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired limit of read operations per second")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_qos_write_iops_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired limit of write operations per second")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_qos_read_bps_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired limit of read bytes per second")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_qos_write_bps_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired limit of write bytes per second")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_qos_iops_burst", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired burst limit of IO operations")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_qos_bps_burst", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired burst limit of IO bytes")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_qos_read_iops_burst", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired burst limit of read operations")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_qos_write_iops_burst", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired burst limit of write operations")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_qos_read_bps_burst", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired burst limit of read bytes")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_qos_write_bps_burst", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired burst limit of write bytes")
    .set_default(0)
    .add_service("rbd"),

    Option("rbd_qos_iops_burst_seconds", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired burst duration in seconds of IO operations")
    .set_default(1)
    .set_min(1)
    .add_service("rbd"),

    Option("rbd_qos_bps_burst_seconds", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired burst duration in seconds of IO bytes")
    .set_default(1)
    .set_min(1)
    .add_service("rbd"),

    Option("rbd_qos_read_iops_burst_seconds", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired burst duration in seconds of read operations")
    .set_default(1)
    .set_min(1)
    .add_service("rbd"),

    Option("rbd_qos_write_iops_burst_seconds", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired burst duration in seconds of write operations")
    .set_default(1)
    .set_min(1)
    .add_service("rbd"),

    Option("rbd_qos_read_bps_burst_seconds", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired burst duration in seconds of read bytes")
    .set_default(1)
    .set_min(1)
    .add_service("rbd"),

    Option("rbd_qos_write_bps_burst_seconds", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired burst duration in seconds of write bytes")
    .set_default(1)
    .set_min(1)
    .add_service("rbd"),

    Option("rbd_qos_schedule_tick_min", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("minimum schedule tick (in milliseconds) for QoS")
    .set_long_description("This determines the minimum time (in milliseconds) at which I/Os can become unblocked if the limit of a throttle is hit. In terms of the token bucket algorithm, this is the minimum interval at which tokens are added to the bucket.")
    .set_default(50)
    .set_min(1)
    .add_service("rbd"),

    Option("rbd_qos_exclude_ops", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("optionally exclude ops from QoS")
    .set_long_description("Optionally exclude ops from QoS. This setting accepts either an integer bitmask value or comma-delimited string of op names. This setting is always internally stored as an integer bitmask value. The mapping between op bitmask value and op name is as follows: +1 -> read, +2 -> write, +4 -> discard, +8 -> write_same, +16 -> compare_and_write")
    .set_validator([](std::string *value, std::string *error_message) {
        std::ostringstream ss;
        uint64_t exclude_ops = librbd::io::rbd_io_operations_from_string(*value, &ss);
        // Leave this in integer form to avoid breaking Cinder.  Someday
        // we would like to present this in string form instead...
        *value = stringify(exclude_ops);
        if (ss.str().size()) {
          return -EINVAL;
        }
        return 0;
    })
    .set_flag(Option::FLAG_RUNTIME)
    .add_service("rbd"),

    Option("rbd_discard_on_zeroed_write_same", Option::TYPE_BOOL, Option::LEVEL_ADVANCED)
    .set_description("discard data on zeroed write same instead of writing zero")
    .set_default(true)
    .add_service("rbd"),

    Option("rbd_mtime_update_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("RBD Image modify timestamp refresh interval. Set to 0 to disable modify timestamp update.")
    .set_default(60)
    .set_min(0)
    .add_service("rbd"),

    Option("rbd_atime_update_interval", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("RBD Image access timestamp refresh interval. Set to 0 to disable access timestamp update.")
    .set_default(60)
    .set_min(0)
    .add_service("rbd"),

    Option("rbd_io_scheduler", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("RBD IO scheduler")
    .set_default("simple")
    .set_enum_allowed({"none", "simple"})
    .add_service("rbd"),

    Option("rbd_io_scheduler_simple_max_delay", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum io delay (in milliseconds) for simple io scheduler (if set to 0 dalay is calculated based on latency stats)")
    .set_default(0)
    .set_min(0)
    .add_service("rbd"),

    Option("rbd_persistent_cache_mode", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("enable persistent write back cache for this volume")
    .set_default("disabled")
    .set_enum_allowed({"disabled", "rwl", "ssd"})
    .add_service("rbd"),

    Option("rbd_persistent_cache_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("size of the persistent write back cache for this volume")
    .set_default(1_G)
    .set_min(1_G)
    .add_service("rbd"),

    Option("rbd_persistent_cache_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("location of the persistent write back cache in a DAX-enabled filesystem on persistent memory")
    .set_default("/tmp")
    .add_service("rbd"),

    Option("rbd_quiesce_notification_attempts", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("the number of quiesce notification attempts")
    .set_default(10)
    .set_min(1)
    .add_service("rbd"),

    Option("rbd_default_snapshot_quiesce_mode", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("default snapshot quiesce mode")
    .set_default("required")
    .set_enum_allowed({"required", "ignore-error", "skip"})
    .add_service("rbd"),

    Option("rbd_plugins", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("comma-delimited list of librbd plugins to enable")
    .add_service("rbd"),

    Option("rbd_config_pool_override_update_timestamp", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("timestamp of last update to pool-level config overrides")
    .set_default(0)
    .add_service("rbd"),


  });
}
