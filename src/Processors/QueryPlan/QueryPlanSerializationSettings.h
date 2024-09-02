#pragma once

#include <Core/BaseSettings.h>
#include <Core/SettingsEnums.h>
#include <Core/Defines.h>


namespace DB
{

#define PLAN_SERIALIZATION_SETTINGS(M, ALIAS) \
    M(UInt64, max_block_size, DEFAULT_BLOCK_SIZE, "Maximum block size in rows for reading", 0) \
    \
    M(UInt64, max_rows_in_distinct, 0, "Maximum number of elements during execution of DISTINCT.", 0) \
    M(UInt64, max_bytes_in_distinct, 0, "Maximum total size of state (in uncompressed bytes) in memory for the execution of DISTINCT.", 0) \
    M(OverflowMode, distinct_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    \
    M(UInt64, max_rows_to_sort, 0, "If more than the specified amount of records have to be processed for ORDER BY operation, the behavior will be determined by the 'sort_overflow_mode' which by default is - throw an exception", 0) \
    M(UInt64, max_bytes_to_sort, 0, "If more than the specified amount of (uncompressed) bytes have to be processed for ORDER BY operation, the behavior will be determined by the 'sort_overflow_mode' which by default is - throw an exception", 0) \
    M(OverflowMode, sort_overflow_mode, OverflowMode::THROW, "What to do when the limit is exceeded.", 0) \
    \
    M(UInt64, prefer_external_sort_block_bytes, DEFAULT_BLOCK_SIZE * 256, "Prefer maximum block bytes for external sort, reduce the memory usage during merging.", 0) \
    M(UInt64, max_bytes_before_external_sort, 0, "If memory usage during ORDER BY operation is exceeding this threshold in bytes, activate the 'external sorting' mode (spill data to disk). Recommended value is half of available system memory.", 0) \
    M(UInt64, max_bytes_before_remerge_sort, 1000000000, "In case of ORDER BY with LIMIT, when memory usage is higher than specified threshold, perform additional steps of merging blocks before final merge to keep just top LIMIT rows.", 0) \
    M(Float, remerge_sort_lowered_memory_bytes_ratio, 2., "If memory usage after remerge does not reduced by this ratio, remerge will be disabled.", 0) \
    M(UInt64, min_free_disk_space_for_temporary_data, 0, "The minimum disk space to keep while writing temporary data used in external sorting and aggregation.", 0) \
    \


DECLARE_SETTINGS_TRAITS(QueryPlanSerializationSettingsTraits, PLAN_SERIALIZATION_SETTINGS)

struct QueryPlanSerializationSettings : public BaseSettings<QueryPlanSerializationSettingsTraits>
{
    QueryPlanSerializationSettings() = default;
};

}
