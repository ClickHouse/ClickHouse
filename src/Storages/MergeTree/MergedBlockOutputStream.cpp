#include <Storages/MergeTree/MergedBlockOutputStream.h>
#include <Interpreters/Context.h>
#include <Parsers/queryToString.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}


MergedBlockOutputStream::MergedBlockOutputStream(
    const MergeTreeDataPartPtr & data_part,
    const StorageMetadataPtr & metadata_snapshot_,
    const NamesAndTypesList & columns_list_,
    const MergeTreeIndices & skip_indices,
    CompressionCodecPtr default_codec_,
    bool reset_columns_,
    bool blocks_are_granules_size)
    : IMergedBlockOutputStream(data_part, metadata_snapshot_, columns_list_, reset_columns_)
    , columns_list(columns_list_)
    , default_codec(default_codec_)
{
    MergeTreeWriterSettings writer_settings(
        storage.getContext()->getSettings(),
        storage.getSettings(),
        data_part->index_granularity_info.is_adaptive,
        /* rewrite_primary_key = */ true,
        blocks_are_granules_size);

    if (!part_path.empty())
        volume->getDisk()->createDirectories(part_path);

    writer = data_part->getWriter(columns_list, metadata_snapshot, skip_indices, default_codec, writer_settings);
}

/// If data is pre-sorted.
void MergedBlockOutputStream::write(const Block & block)
{
    writeImpl(block, nullptr);
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
        , sync(sync_) {}

    void finish();
};

void MergedBlockOutputStream::Finalizer::finish()
{
    std::unique_ptr<Impl> to_finish = std::move(impl);
    if (to_finish)
        to_finish->finish();
}

void MergedBlockOutputStream::Finalizer::Impl::finish()
{
    writer.finish(sync);

    auto disk = part->volume->getDisk();
    for (const auto & file_name: files_to_remove_after_finish)
        disk->removeFile(part->getFullRelativePath() + file_name);

    for (auto & file : written_files)
    {
        file->finalize();
        if (sync)
            file->sync();
    }

    part->storage.lockSharedData(*part);
}

MergedBlockOutputStream::Finalizer::~Finalizer()
{
    try
    {
        finish();
    }
    catch (...)
    {
        tryLogCurrentException("MergedBlockOutputStream");
    }
}

MergedBlockOutputStream::Finalizer::Finalizer(Finalizer &&) noexcept = default;
MergedBlockOutputStream::Finalizer & MergedBlockOutputStream::Finalizer::operator=(Finalizer &&) noexcept = default;
MergedBlockOutputStream::Finalizer::Finalizer(std::unique_ptr<Impl> impl_) : impl(std::move(impl_)) {}

void MergedBlockOutputStream::finalizePart(
        MergeTreeData::MutableDataPartPtr & new_part,
        bool sync,
        const NamesAndTypesList * total_columns_list,
        MergeTreeData::DataPart::Checksums * additional_column_checksums)
{
    finalizePartAsync(new_part, sync, total_columns_list, additional_column_checksums).finish();
}

MergedBlockOutputStream::Finalizer MergedBlockOutputStream::finalizePartAsync(
        MergeTreeData::MutableDataPartPtr & new_part,
        bool sync,
        const NamesAndTypesList * total_columns_list,
        MergeTreeData::DataPart::Checksums * additional_column_checksums)
{
    /// Finish write and get checksums.
    MergeTreeData::DataPart::Checksums checksums;

    if (additional_column_checksums)
        checksums = std::move(*additional_column_checksums);

    /// Finish columns serialization.
    writer->fillChecksums(checksums);

    LOG_TRACE(&Poco::Logger::get("MergedBlockOutputStream"), "filled checksums {}", new_part->getNameWithState());

    for (const auto & [projection_name, projection_part] : new_part->getProjectionParts())
        checksums.addFile(
            projection_name + ".proj",
            projection_part->checksums.getTotalSizeOnDisk(),
            projection_part->checksums.getTotalChecksumUInt128());

    NameSet files_to_remove_after_sync;
    if (reset_columns)
    {
        auto part_columns = total_columns_list ? *total_columns_list : columns_list;
        auto serialization_infos = new_part->getSerializationInfos();

        serialization_infos.replaceData(new_serialization_infos);
        files_to_remove_after_sync = removeEmptyColumnsFromPart(new_part, part_columns, serialization_infos, checksums);

        new_part->setColumns(part_columns);
        new_part->setSerializationInfos(serialization_infos);
    }

    auto finalizer = std::make_unique<Finalizer::Impl>(*writer, new_part, files_to_remove_after_sync, sync);
    if (new_part->isStoredOnDisk())
       finalizer->written_files = finalizePartOnDisk(new_part, checksums);

    new_part->rows_count = rows_count;
    new_part->modification_time = time(nullptr);
    new_part->index = writer->releaseIndexColumns();
    new_part->checksums = checksums;
    new_part->setBytesOnDisk(checksums.getTotalSizeOnDisk());
    new_part->index_granularity = writer->getIndexGranularity();
    new_part->calculateColumnsAndSecondaryIndicesSizesOnDisk();

    if (default_codec != nullptr)
        new_part->default_codec = default_codec;

    return Finalizer(std::move(finalizer));
}

MergedBlockOutputStream::WrittenFiles MergedBlockOutputStream::finalizePartOnDisk(
    const MergeTreeData::DataPartPtr & new_part,
    MergeTreeData::DataPart::Checksums & checksums)
{
    WrittenFiles written_files;
    if (new_part->isProjectionPart())
    {
        if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING || isCompactPart(new_part))
        {
            auto count_out = volume->getDisk()->writeFile(part_path + "count.txt", 4096);
            HashingWriteBuffer count_out_hashing(*count_out);
            writeIntText(rows_count, count_out_hashing);
            count_out_hashing.next();
            checksums.files["count.txt"].file_size = count_out_hashing.count();
            checksums.files["count.txt"].file_hash = count_out_hashing.getHash();
            count_out->preFinalize();
            written_files.emplace_back(std::move(count_out));
        }
    }
    else
    {
        if (new_part->uuid != UUIDHelpers::Nil)
        {
            auto out = volume->getDisk()->writeFile(fs::path(part_path) / IMergeTreeDataPart::UUID_FILE_NAME, 4096);
            HashingWriteBuffer out_hashing(*out);
            writeUUIDText(new_part->uuid, out_hashing);
            checksums.files[IMergeTreeDataPart::UUID_FILE_NAME].file_size = out_hashing.count();
            checksums.files[IMergeTreeDataPart::UUID_FILE_NAME].file_hash = out_hashing.getHash();
            out->preFinalize();
            written_files.emplace_back(std::move(out));
        }

        if (storage.format_version >= MERGE_TREE_DATA_MIN_FORMAT_VERSION_WITH_CUSTOM_PARTITIONING)
        {
            if (auto file = new_part->partition.store(storage, volume->getDisk(), part_path, checksums))
                written_files.emplace_back(std::move(file));

            if (new_part->minmax_idx->initialized)
            {
                auto files = new_part->minmax_idx->store(storage, volume->getDisk(), part_path, checksums);
                for (auto & file : files)
                    written_files.emplace_back(std::move(file));
            }
            else if (rows_count)
                throw Exception("MinMax index was not initialized for new non-empty part " + new_part->name
                    + ". It is a bug.", ErrorCodes::LOGICAL_ERROR);
        }

        {
            auto count_out = volume->getDisk()->writeFile(fs::path(part_path) / "count.txt", 4096);
            HashingWriteBuffer count_out_hashing(*count_out);
            writeIntText(rows_count, count_out_hashing);
            count_out_hashing.next();
            checksums.files["count.txt"].file_size = count_out_hashing.count();
            checksums.files["count.txt"].file_hash = count_out_hashing.getHash();
            count_out->preFinalize();
            written_files.emplace_back(std::move(count_out));
        }
    }

    if (!new_part->ttl_infos.empty())
    {
        /// Write a file with ttl infos in json format.
        auto out = volume->getDisk()->writeFile(fs::path(part_path) / "ttl.txt", 4096);
        HashingWriteBuffer out_hashing(*out);
        new_part->ttl_infos.write(out_hashing);
        checksums.files["ttl.txt"].file_size = out_hashing.count();
        checksums.files["ttl.txt"].file_hash = out_hashing.getHash();
        out->preFinalize();
        written_files.emplace_back(std::move(out));
    }

    if (!new_part->getSerializationInfos().empty())
    {
        auto out = volume->getDisk()->writeFile(part_path + IMergeTreeDataPart::SERIALIZATION_FILE_NAME, 4096);
        HashingWriteBuffer out_hashing(*out);
        new_part->getSerializationInfos().writeJSON(out_hashing);
        checksums.files[IMergeTreeDataPart::SERIALIZATION_FILE_NAME].file_size = out_hashing.count();
        checksums.files[IMergeTreeDataPart::SERIALIZATION_FILE_NAME].file_hash = out_hashing.getHash();
        out->preFinalize();
        written_files.emplace_back(std::move(out));
    }

    {
        /// Write a file with a description of columns.
        auto out = volume->getDisk()->writeFile(fs::path(part_path) / "columns.txt", 4096);
        new_part->getColumns().writeText(*out);
        out->preFinalize();
        written_files.emplace_back(std::move(out));
    }

    if (default_codec != nullptr)
    {
        auto out = volume->getDisk()->writeFile(part_path + IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME, 4096);
        DB::writeText(queryToString(default_codec->getFullCodecDesc()), *out);
        out->preFinalize();
        written_files.emplace_back(std::move(out));
    }
    else
    {
        throw Exception("Compression codec have to be specified for part on disk, empty for" + new_part->name
                + ". It is a bug.", ErrorCodes::LOGICAL_ERROR);
    }

    {
        /// Write file with checksums.
        auto out = volume->getDisk()->writeFile(fs::path(part_path) / "checksums.txt", 4096);
        checksums.write(*out);
        out->preFinalize();
        written_files.emplace_back(std::move(out));
    }

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
