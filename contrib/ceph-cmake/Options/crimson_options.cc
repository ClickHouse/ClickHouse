#include "common/options.h"


std::vector<Option> get_crimson_options() {
  return std::vector<Option>({
    Option("crimson_osd_obc_lru_size", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of obcs to cache")
    .set_default(10),

    Option("crimson_osd_scheduler_concurrency", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("The maximum number concurrent IO operations, 0 for unlimited")
    .set_default(0),

    Option("crimson_alien_op_num_threads", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("The number of threads for serving alienized ObjectStore")
    .set_default(6)
    .set_flag(Option::FLAG_STARTUP),

    Option("crimson_seastar_smp", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("Number of seastar reactor threads to use for the osd")
    .set_default(1)
    .set_flag(Option::FLAG_STARTUP),

    Option("crimson_alien_thread_cpu_cores", Option::TYPE_STR, Option::LEVEL_ADVANCED)
    .set_description("CPU cores on which alienstore threads will run in cpuset(7) format"),

    Option("seastore_segment_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Segment size to use for SegmentManager")
    .set_default(64_M),

    Option("seastore_device_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("Total size to use for SegmentManager block file if created")
    .set_default(50_G),

    Option("seastore_block_create", Option::TYPE_BOOL, Option::LEVEL_DEV)
    .set_description("Create SegmentManager file if it doesn't exist")
    .set_default(true)
    .add_see_also({"seastore_device_size"}),

    Option("seastore_journal_batch_capacity", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("The number limit of records in a journal batch")
    .set_default(16),

    Option("seastore_journal_batch_flush_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("The size threshold to force flush a journal batch")
    .set_default(16_M),

    Option("seastore_journal_iodepth_limit", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("The io depth limit to submit journal records")
    .set_default(5),

    Option("seastore_journal_batch_preferred_fullness", Option::TYPE_FLOAT, Option::LEVEL_DEV)
    .set_description("The record fullness threshold to flush a journal batch")
    .set_default(0.95),

    Option("seastore_default_max_object_size", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("default logical address space reservation for seastore objects' data")
    .set_default(16777216),

    Option("seastore_default_object_metadata_reservation", Option::TYPE_UINT, Option::LEVEL_DEV)
    .set_description("default logical address space reservation for seastore objects' metadata")
    .set_default(16777216),

    Option("seastore_cache_lru_size", Option::TYPE_SIZE, Option::LEVEL_ADVANCED)
    .set_description("Size in bytes of extents to keep in cache.")
    .set_default(64_M),

    Option("seastore_obj_data_write_amplification", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("split extent if ratio of total extent size to write size exceeds this value")
    .set_default(1.25),

    Option("seastore_max_concurrent_transactions", Option::TYPE_UINT, Option::LEVEL_ADVANCED)
    .set_description("maximum concurrent transactions that seastore allows")
    .set_default(8),

    Option("seastore_main_device_type", Option::TYPE_STR, Option::LEVEL_DEV)
    .set_description("The main device type seastore uses (SSD or RANDOM_BLOCK_SSD)")
    .set_default("SSD"),

    Option("seastore_cbjournal_size", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("Total size to use for CircularBoundedJournal if created, it is valid only if seastore_main_device_type is RANDOM_BLOCK")
    .set_default(5_G),

    Option("seastore_multiple_tiers_stop_evict_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("When the used ratio of main tier is less than this value, then stop evict cold data to the cold tier.")
    .set_default(0.5),

    Option("seastore_multiple_tiers_default_evict_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Begin evicting cold data to the cold tier when the used ratio of the main tier reaches this value.")
    .set_default(0.6),

    Option("seastore_multiple_tiers_fast_evict_ratio", Option::TYPE_FLOAT, Option::LEVEL_ADVANCED)
    .set_description("Begin fast eviction when the used ratio of the main tier reaches this value.")
    .set_default(0.7),

    Option("seastore_data_delta_based_overwrite", Option::TYPE_SIZE, Option::LEVEL_DEV)
    .set_description("overwrite the existing data block based on delta if the original size is smaller than the value, otherwise do overwrite based on remapping, set to 0 to enforce the remap-based overwrite.")
    .set_default(0),


  });
}
