#pragma once

#include <Core/NamesAndTypes.h>
#include <Storages/MergeTree/IMergeTreeReader.h>
#include <IO/ReadBufferFromFileBase.h>
#include <DataTypes/Serializations/ISerialization.h>

#include <Processors/Formats/Impl/Parquet/Decoding.h>

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

/// Base class of readers for compact parts.
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
    //void fillColumnPositions();   // columns_to_read[i] -> индекс leaf-колонки в parquet (по имени / field-id)

    struct ColumnStream
    {
        parquet::format::ColumnChunk chunk;
        parquet::format::OffsetIndex offset_index;
        Parquet::PageDecoderInfo decoder_info;
        Parquet::Dictionary dictionary;
    };

    parquet::FileMetaData footer;                        // распарсен 1 раз (или из ParquetMetadataCache)
    std::vector<std::optional<size_t>> column_positions; // nullopt = колонки нет в парте
    std::vector<ColumnStream> streams;

    ReadBufferFromFileBase::ProfileCallback profile_callback;
    clockid_t clock_type;

    size_t next_mark = 0;                     // для continue_reading, как в Compact
};

}

#endif
