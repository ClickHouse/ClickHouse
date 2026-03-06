#include <Storages/MergeTree/IMergedBlockOutputStream.h>
#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Storages/MergeTree/MergeTreeIndexGranularityConstant.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <IO/HashingWriteBuffer.h>
#include <Interpreters/Context.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Core/Settings.h>
#include <Storages/MergeTree/StatisticsSerialization.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace MergeTreeSetting
{
    extern const MergeTreeSettingsBool enable_index_granularity_compression;
}

MergedBlockOutputStream::MergedBlockOutputStream(
    const MergeTreeMutableDataPartPtr & data_part,
    MergeTreeSettingsPtr data_settings,
    const StorageMetadataPtr & metadata_snapshot_,
    const NamesAndTypesList & columns_list_,
    const MergeTreeIndices & skip_indices,
    CompressionCodecPtr default_codec_,
    MergeTreeIndexGranularityPtr index_granularity_ptr,
    TransactionID tid,
    size_t part_uncompressed_bytes,
    bool reset_columns_,
    bool blocks_are_granules_size,
    const WriteSettings & write_settings_,
    WrittenOffsetSubstreams * written_offset_substreams)
    : IMergedBlockOutputStream(
          std::move(data_settings), data_part->getDataPartStoragePtr(), metadata_snapshot_, columns_list_, reset_columns_)
    , columns_list(columns_list_)
    , default_codec(default_codec_)
{
    /// Save marks in memory if prewarm is enabled to avoid re-reading marks file.
    bool save_marks_in_cache = data_part->storage.getMarkCacheToPrewarm(part_uncompressed_bytes) != nullptr;
    /// Save primary index in memory if cache is disabled or is enabled with prewarm to avoid re-reading primary index file.
    bool save_primary_index_in_memory = !data_part->storage.getPrimaryIndexCache() || data_part->storage.getPrimaryIndexCacheToPrewarm(part_uncompressed_bytes);

    writer_settings = MergeTreeWriterSettings(
        data_part->storage.getContext()->getSettingsRef(),
        write_settings_,
        storage_settings,
        data_part,
        data_part->index_granularity_info.mark_type.adaptive,
        /* rewrite_primary_key = */ true,
        save_marks_in_cache,
        save_primary_index_in_memory,
        blocks_are_granules_size);

    data_part_storage->createDirectories();

    /// NOTE do not pass context for writing to system.transactions_info_log,
    /// because part may have temporary name (with temporary block numbers). Will write it later.
    data_part->version.setCreationTID(tid, nullptr);
    data_part->storeVersionMetadata();

    writer = createMergeTreeDataPartWriter(data_part->getType(),
        data_part->name,
        data_part->storage.getLogName(),
        data_part->getSerializations(),
        data_part_storage,
        data_part->index_granularity_info,
        storage_settings,
        columns_list,
        data_part->getColumnPositions(),
        metadata_snapshot,
        data_part->storage.getVirtualsPtr(),
        skip_indices,
        data_part->getMarksFileExtension(),
        default_codec,
        writer_settings,
        std::move(index_granularity_ptr),
        written_offset_substreams);
}

/// If data is pre-sorted.
void MergedBlockOutputStream::write(const Block & block)
{
    writeImpl(block, nullptr);
}

void MergedBlockOutputStream::cancel() noexcept
{
    if (writer)
        writer->cancel();
}


/** If the data is not sorted, but we pre-calculated the permutation, after which they will be sorted.
    * This method is used to save RAM, since you do not need to keep two blocks at once - the source and the sorted.
    */
void MergedBlockOutputStream::writeWithPermutation(const Block & block, const IColumn::Permutation * permutation)
{
    writeImpl(block, permutation);
}

struct MergedBlockOutputStream::Finalizer::Impl
{
    IMergeTreeDataPartWriter & writer;
    MergeTreeData::MutableDataPartPtr part;
    NameSet files_to_remove_after_finish;
    std::vector<std::unique_ptr<WriteBufferFromFileBase>> written_files;
    bool sync;

    Impl(IMergeTreeDataPartWriter & writer_, MergeTreeData::MutableDataPartPtr part_, const NameSet & files_to_remove_after_finish_, bool sync_)
        : writer(writer_)
        , part(std::move(part_))
        , files_to_remove_after_finish(files_to_remove_after_finish_)
        , sync(sync_)
    {
    }

    void finish();
    void cancel() noexcept;
};

void MergedBlockOutputStream::Finalizer::finish()
{
    std::unique_ptr<Impl> to_finish = std::move(impl);
    impl.reset();
    if (to_finish)
        to_finish->finish();
}

void MergedBlockOutputStream::Finalizer::cancel() noexcept
{
    std::unique_ptr<Impl> to_cancel = std::move(impl);
    impl.reset();
    if (to_cancel)
        to_cancel->cancel();
}

void MergedBlockOutputStream::Finalizer::Impl::finish()
{
    writer.finish(sync);

    for (auto & file : written_files)
    {
        file->finalize();
        if (sync)
            file->sync();
    }

    for (const auto & file_name : files_to_remove_after_finish)
        part->getDataPartStorage().removeFile(file_name);
}

void MergedBlockOutputStream::Finalizer::Impl::cancel() noexcept
{
    writer.cancel();

    for (auto & file : written_files)
    {
        file->cancel();
    }
}

MergedBlockOutputStream::Finalizer::Finalizer(Finalizer &&) noexcept = default;
MergedBlockOutputStream::Finalizer & MergedBlockOutputStream::Finalizer::operator=(Finalizer &&) noexcept = default;
MergedBlockOutputStream::Finalizer::Finalizer(std::unique_ptr<Impl> impl_) : impl(std::move(impl_)) {}

MergedBlockOutputStream::Finalizer::~Finalizer()
{
    if (impl)
        cancel();
}


void MergedBlockOutputStream::finalizePart(
    const MergeTreeMutableDataPartPtr & new_part,
    const GatheredData & gathered_data,
    bool sync,
    const NamesAndTypesList * total_columns_list)
{
    finalizePartAsync(new_part, gathered_data, sync, total_columns_list).finish();
}

void MergedBlockOutputStream::finalizeIndexGranularity()
{
    writer->finalizeIndexGranularity();
}

MergedBlockOutputStream::Finalizer MergedBlockOutputStream::finalizePartAsync(
    const MergeTreeMutableDataPartPtr & new_part,
    const GatheredData & gathered_data,
    bool sync,
    const NamesAndTypesList * total_columns_list)
{
    /// Finish write and get checksums.
    MergeTreeData::DataPart::Checksums checksums = gathered_data.checksums;
    NameSet checksums_to_remove;

    /// Finish columns serialization.
    writer->fillChecksums(checksums, checksums_to_remove);

    for (const auto & name : checksums_to_remove)
        checksums.files.erase(name);

    LOG_TRACE(getLogger("MergedBlockOutputStream"), "filled checksums {}", new_part->getNameWithState());

    for (const auto & [projection_name, projection_part] : new_part->getProjectionParts())
    {
        checksums.addFile(
            projection_name + ".proj",
            projection_part->checksums.getTotalSizeOnDisk(),
            projection_part->checksums.getTotalChecksumUInt128());
    }

    NameSet files_to_remove_after_sync;
    if (reset_columns)
    {
        auto part_columns = total_columns_list ? *total_columns_list : columns_list;
        auto serialization_infos = new_part->getSerializationInfos();

        serialization_infos.replaceData(new_serialization_infos);
        files_to_remove_after_sync
            = removeEmptyColumnsFromPart(new_part, part_columns, new_part->expired_columns, serialization_infos, checksums);

        new_part->setColumns(part_columns, serialization_infos, metadata_snapshot->getMetadataVersion());
    }

    std::vector<std::unique_ptr<WriteBufferFromFileBase>> written_files;
    written_files = finalizePartOnDisk(new_part, checksums, gathered_data);

    new_part->rows_count = rows_count;
    new_part->modification_time = time(nullptr);

    new_part->checksums = checksums;
    new_part->setBytesOnDisk(checksums.getTotalSizeOnDisk());
    new_part->setBytesUncompressedOnDisk(checksums.getTotalSizeUncompressedOnDisk());
    new_part->index_granularity = writer->getIndexGranularity();
    new_part->calculateColumnsAndSecondaryIndicesSizesOnDisk();

    if ((*new_part->storage.getSettings())[MergeTreeSetting::enable_index_granularity_compression])
    {
        if (auto new_index_granularity = new_part->index_granularity->optimize())
            new_part->index_granularity = std::move(new_index_granularity);
    }

    /// For constant granularity parts (non-adaptive marks), the writer's in-memory
    /// granularity has all marks at the constant value. Fix the last mark granularity
    /// to match the actual row count, same as done when loading parts from disk.
    /// Note: the granularity pointer may be shared with the source part (mutations reuse it),
    /// so we use fixedFromRowsCount which returns a new object to avoid racing with concurrent readers.
    if (const auto * constant_granularity = dynamic_cast<const MergeTreeIndexGranularityConstant *>(new_part->index_granularity.get()))
        new_part->index_granularity = constant_granularity->fixedFromRowsCount(rows_count);

    /// It's important to set index after index granularity.
    if (auto computed_index = writer->releaseIndexColumns())
        new_part->setIndex(std::move(*computed_index));

    /// In mutation, existing_rows_count is already calculated in PartMergerWriter
    /// In merge situation, lightweight deleted rows was physically deleted, existing_rows_count equals rows_count
    if (!new_part->existing_rows_count.has_value())
        new_part->existing_rows_count = rows_count;

    if (default_codec != nullptr)
        new_part->default_codec = default_codec;

    auto finalizer = std::make_unique<Finalizer::Impl>(*writer, new_part, files_to_remove_after_sync, sync);
    finalizer->written_files = std::move(written_files);
    return Finalizer(std::move(finalizer));
}

MergedBlockOutputStream::WrittenFiles MergedBlockOutputStream::finalizePartOnDisk(
    const MergeTreeMutableDataPartPtr & new_part,
    MergeTreeData::DataPart::Checksums & checksums,
    const GatheredData & gathered_data)
{
    /// NOTE: You do not need to call fsync here, since it will be called later for the all written_files.
    WrittenFiles written_files;

    auto write_hashed_file = [&](const auto & filename, auto && writer)
    {
        auto out = new_part->getDataPartStorage().writeFile(filename, 4096, writer_settings.query_write_settings);
        HashingWriteBuffer out_hashing(*out);
        writer(out_hashing);
        out_hashing.finalize();
        checksums.files[filename].file_size = out_hashing.count();
        checksums.files[filename].file_hash = out_hashing.getHash();
        out->preFinalize();
        written_files.emplace_back(std::move(out));
    };

    auto write_plain_file = [&](const auto & filename, auto && writer)
    {
        auto out = new_part->getDataPartStorage().writeFile(filename, 4096, writer_settings.query_write_settings);
        writer(*out);
        out->preFinalize();
        written_files.emplace_back(std::move(out));
    };

    if (!new_part->isProjectionPart())
    {
        if (new_part->uuid != UUIDHelpers::Nil)
        {
            write_hashed_file(IMergeTreeDataPart::UUID_FILE_NAME, [&](auto & buffer)
            {
                writeUUIDText(new_part->uuid, buffer);
            });
        }

        if (new_part->storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        {
            if (auto file = new_part->partition.store(metadata_snapshot, new_part->storage.getContext(), new_part->getDataPartStorage(), checksums))
            {
                written_files.emplace_back(std::move(file));
            }

            if (new_part->minmax_idx->initialized)
            {
                auto files = new_part->minmax_idx->store(metadata_snapshot, new_part->getDataPartStorage(), checksums, storage_settings);
                for (auto & file : files)
                    written_files.emplace_back(std::move(file));
            }
            else if (rows_count)
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "MinMax index was not initialized for new non-empty part {}", new_part->name);
            }

            const auto & source_parts = new_part->getSourcePartsSet();
            if (!source_parts.empty())
            {
                write_hashed_file(SourcePartsSetForPatch::FILENAME, [&](auto & buffer)
                {
                    source_parts.writeBinary(buffer);
                });
            }
        }
    }

    write_hashed_file("count.txt", [&](auto & buffer)
    {
        writeIntText(rows_count, buffer);
    });

    if (!new_part->ttl_infos.empty())
    {
        write_hashed_file("ttl.txt", [&](auto & buffer)
        {
            new_part->ttl_infos.write(buffer);
        });
    }

    const auto & serialization_infos = new_part->getSerializationInfos();
    if (serialization_infos.needsPersistence())
    {
        write_hashed_file(IMergeTreeDataPart::SERIALIZATION_FILE_NAME, [&](auto & buffer)
        {
            serialization_infos.writeJSON(buffer);
        });
    }

    const auto & statistics = gathered_data.statistics;
    new_part->setEstimates(statistics.getEstimates());

    if (!statistics.empty())
    {
        if (isFullPartStorage(new_part->getDataPartStorage()))
        {
            auto out = serializeStatisticsPacked(new_part->getDataPartStorage(), checksums, statistics, default_codec, writer_settings.query_write_settings);
            written_files.emplace_back(std::move(out));
        }
        /// Write statistics as separate compressed files in packed parts to avoid double buffering.
        else
        {
            auto files = serializeStatisticsWide(new_part->getDataPartStorage(), checksums, statistics, default_codec, writer_settings.query_write_settings);
            std::move(files.begin(), files.end(), std::back_inserter(written_files));
        }
    }

    write_plain_file("columns.txt", [&](auto & buffer)
    {
        new_part->getColumns().writeText(buffer);
    });

    /// Merge columns substreams from current writer and additional columns substreams
    /// from other writers (that could be used during vertical merge).
    /// If there are no additional columns substreams we still need to call merge
    /// so we will keep only columns that are present in new_part->getColumns().
    /// It may happen that new_part->getColumns() has less columns then columns substreams
    /// from writer because of expired TTL.
    auto columns_substreams = ColumnsSubstreams::merge(
        writer->getColumnsSubstreams(),
        gathered_data.columns_substreams,
        new_part->getColumns().getNames());

    if (!columns_substreams.empty())
    {
        write_plain_file(IMergeTreeDataPart::COLUMNS_SUBSTREAMS_FILE_NAME, [&](auto & buffer)
        {
            columns_substreams.writeText(buffer);
        });

        new_part->setColumnsSubstreams(columns_substreams);
    }

    write_plain_file(IMergeTreeDataPart::METADATA_VERSION_FILE_NAME, [&](auto & buffer)
    {
        writeIntText(new_part->getMetadataVersion(), buffer);
    });

    if (default_codec != nullptr)
    {
        write_plain_file(IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME, [&](auto & buffer)
        {
            writeText(default_codec->getFullCodecDesc()->formatWithSecretsOneLine(), buffer);
        });
    }
    else
    {
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Compression codec have to be specified for part on disk, empty for {}", new_part->name);
    }

    write_plain_file("checksums.txt", [&](auto & buffer)
    {
        checksums.write(buffer);
    });

    return written_files;
}

void MergedBlockOutputStream::writeImpl(const Block & block, const IColumn::Permutation * permutation)
{
    block.checkNumberOfRows();
    size_t rows = block.rows();
    if (!rows)
        return;

    writer->write(block, permutation);
    if (reset_columns)
        new_serialization_infos.add(block);

    rows_count += rows;
}

}
