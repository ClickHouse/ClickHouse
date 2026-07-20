#pragma once

#include "config.h"

#include <Storages/MergeTree/MergeTreeDataPartWriterOnDisk.h>
#include <Storages/MergeTree/MergeTreeDataPartWriterCompact.h>
#include <Processors/Formats/Impl/ParquetBlockOutputFormat.h>

#if USE_PARQUET

namespace DB
{

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

private:
    void addStreams(const NameAndTypePair &, const ASTPtr &) override {}   /// No per-column streams.
    void fillIndexGranularity(size_t index_granularity_for_block, size_t rows_in_block) override;
    ISerialization::SerializeBinaryBulkSettings getSerializationSettings() const override { return {}; }

    Block header;

    std::unique_ptr<WriteBufferFromFileBase> parquet_plain_file;
    std::unique_ptr<HashingWriteBuffer> data_hashing;
    std::shared_ptr<ParquetBlockOutputFormat> output_format;

    size_t row_group_size_rows = 0;
    Block primary_index_block;
    Block skip_indices_block;
};

}

#endif
