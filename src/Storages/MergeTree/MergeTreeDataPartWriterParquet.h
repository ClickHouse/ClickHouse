#pragma once

#include "config.h"

#include <map>

#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterCompact.h>
#include <Storages/MergeTree/ColumnsSubstreams.h>

#include <Processors/Formats/Impl/Parquet/Decoding.h>
#include <Processors/Formats/Impl/Parquet/Write.h>

#if USE_PARQUET

#include <parquet/metadata.h>

namespace DB
{

/// Writes data part in compact format.
class MergeTreeDataPartWriterParquet : public MergeTreeDataPartWriterOnDisk
{
    using Base = MergeTreeDataPartWriterOnDisk;

public:
    MergeTreeDataPartWriterParquet(
        const String & data_part_name_,
        const String & logger_name_,
        const SerializationByName & serializations_,
        MutableDataPartStoragePtr data_part_storage_,
        const MergeTreeIndexGranularityInfo & index_granularity_info_,
        const MergeTreeSettingsPtr & storage_settings_,
        const NamesAndTypesList & columns_list,
        const StorageMetadataPtr & metadata_snapshot_,
        const std::vector<MergeTreeIndexPtr> & indices_to_recalc,
        const CompressionCodecPtr & default_codec,
        const MergeTreeWriterSettings & settings,
        MergeTreeIndexGranularityPtr index_granularity_);

    void write(const Block & block, const IColumnPermutation * permutation, Block * permuted_columns_cache) override;

    void finalizeIndexGranularity() final;
    void fillChecksums(MergeTreeDataPartChecksums & checksums, NameSet & checksums_to_remove) final;
    void finish(bool sync) override;
    void cancel() noexcept override;

    size_t getNumberOfOpenStreams() const override { return 1; }

    static constexpr const char * checksums_key = "checksums";

private:
    void addStreams(const NameAndTypePair &, const ASTPtr &) override {}   // ← ключевое: НЕТ колоночных стримов
    void fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block) override;
    ISerialization::SerializeBinaryBulkSettings getSerializationSettings() const override { return {}; }

    void flushRowGroup();

    Parquet::WriteOptions options;
    Parquet::SchemaElements schema;
    Parquet::FileWriteState file_state;
    FormatSettings format_settings;
    Block header;

    std::unique_ptr<WriteBufferFromFileBase> parquet_plain_file;
    std::unique_ptr<HashingWriteBuffer> data_hashing;
    bool file_header_written = false;

    MergeTreeDataPartWriterCompact::ColumnsBuffer columns_buffer;
    /// Row group is flushed once the buffer reaches this many rows; a whole number of granules
    /// so a data page (= one granule) never straddles a row-group boundary.
    size_t row_group_size_rows = 0;
    parquet::KeyValueMetadata key_value_metadata;
};

}

#endif
