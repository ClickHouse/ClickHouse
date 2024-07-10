#include "common/options.h"


std::vector<Option> get_immutable_object_cache_options() {
  return std::vector<Option>({
    Option("immutable_object_cache_path", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("immutable object cache data dir")
    .set_default("/tmp/ceph_immutable_object_cache")
    .add_service("immutable-object-cache"),

    Option("immutable_object_cache_sock", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("immutable object cache domain socket")
    .set_default("/var/run/ceph/immutable_object_cache_sock")
    .add_service("immutable-object-cache"),

    Option("immutable_object_cache_max_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("max immutable object cache data size")
    .set_default(1_G)
    .add_service("immutable-object-cache"),

    Option("immutable_object_cache_max_inflight_ops", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("max inflight promoting requests for immutable object cache daemon")
    .set_default(128)
    .add_service("immutable-object-cache"),

    Option("immutable_object_cache_client_dedicated_thread_num", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("immutable object cache client dedicated thread number")
    .set_default(2)
    .add_service("immutable-object-cache"),

    Option("immutable_object_cache_watermark", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("immutable object cache water mark")
    .set_default(0.9)
    .add_service("immutable-object-cache"),

    Option("immutable_object_cache_qos_schedule_tick_min", Option::TYPE_MILLISECS, Option::LEVEL_ADVANCED)
    .set_description("minimum schedule tick for immutable object cache")
    .set_default(50)
    .set_min(1)
    .add_service("immutable-object-cache"),

    Option("immutable_object_cache_qos_iops_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired immutable object cache IO operations limit per second")
    .set_default(0)
    .add_service("immutable-object-cache"),

    Option("immutable_object_cache_qos_iops_burst", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired burst limit of immutable object cache IO operations")
    .set_default(0)
    .add_service("immutable-object-cache"),

    Option("immutable_object_cache_qos_iops_burst_seconds", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("the desired burst duration in seconds of immutable object cache IO operations")
    .set_default(1)
    .set_min(1)
    .add_service("immutable-object-cache"),

    Option("immutable_object_cache_qos_bps_limit", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired immutable object cache IO bytes limit per second")
    .set_default(0)
    .add_service("immutable-object-cache"),

    Option("immutable_object_cache_qos_bps_burst", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("the desired burst limit of immutable object cache IO bytes")
    .set_default(0)
    .add_service("immutable-object-cache"),

    Option("immutable_object_cache_qos_bps_burst_seconds", Option::TYPE_SECS, Option::LEVEL_ADVANCED)
    .set_description("the desired burst duration in seconds of immutable object cache IO bytes")
    .set_default(1)
    .set_min(1)
    .add_service("immutable-object-cache"),


  });
}
