#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <DataTypes/Serializations/ISerialization.h>

#include <Processors/Formats/Impl/Parquet/Decoding.h>

#include <Processors/Formats/IInputFormat.h>

#if USE_PARQUET

#include <parquet/encoding.h>
#include <parquet/schema.h>
#include <parquet/metadata.h>
#include <parquet/page_index.h>

namespace DB
{

class MergeTreeDataPartCompact;
using DataPartCompactPtr = std::shared_ptr<const MergeTreeDataPartCompact>;

class IMergeTreeDataPart;
using DataPartPtr = std::shared_ptr<const IMergeTreeDataPart>;

class MergeTreeReaderParquet : public IMergeTreeReader
{
public:
    MergeTreeReaderParquet(
        MergeTreeDataPartInfoForReaderPtr data_part_info_for_read_,
        NamesAndTypesList columns_,
        const VirtualFields & virtual_fields_,
        const StorageSnapshotPtr & storage_snapshot_,
        const MergeTreeSettingsPtr & storage_settings_,
        UncompressedCache * uncompressed_cache_,
        MarkCache * mark_cache_,
        DeserializationPrefixesCache * deserialization_prefixes_cache_,
        MarkRanges mark_ranges_,
        MergeTreeReaderSettings settings_,
        ValueSizeMap avg_value_size_hints_,
        const ReadBufferFromFileBase::ProfileCallback & profile_callback_,
        clockid_t clock_type_);

    bool canReadIncompleteGranules() const final { return false; }

protected:
    ReadBufferFromFileBase::ProfileCallback profile_callback;
    clockid_t clock_type;

    size_t next_mark = 0;
};

}

#endif
