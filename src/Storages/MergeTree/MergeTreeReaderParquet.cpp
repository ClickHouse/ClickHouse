#include <Storages/MergeTree/MergeTreeReaderParquet.h>

#if USE_PARQUET

namespace DB
{

MergeTreeReaderParquet::MergeTreeReaderParquet(
    MergeTreeDataPartInfoForReaderPtr data_part_info_for_read_,
    NamesAndTypesList columns_,
    const VirtualFields & virtual_fields_,
    const StorageSnapshotPtr & storage_snapshot_,
    const MergeTreeSettingsPtr & storage_settings_,
    UncompressedCache * uncompressed_cache_,
    MarkCache * mark_cache_,
    DeserializationPrefixesCache * /*deserialization_prefixes_cache_*/,
    MarkRanges mark_ranges_,
    MergeTreeReaderSettings settings_,
    ValueSizeMap avg_value_size_hints_,
    const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
    clockid_t clock_type_)
    : IMergeTreeReader(
        data_part_info_for_read_,
        columns_,
        virtual_fields_,
        storage_snapshot_,
        storage_settings_,
        uncompressed_cache_,
        mark_cache_,
        mark_ranges_,
        settings_,
        avg_value_size_hints_)
    , profile_callback(profile_callback_)
    , clock_type(clock_type_)
{
}

}

#endif
