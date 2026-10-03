#include <Storages/MergeTree/MergeTreeIndexClearFiles.h>

#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeData.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/StorageInMemoryMetadata.h>

#include <array>

namespace DB
{

bool isIndexExpiredByTTL(
    const std::shared_ptr<const StorageInMemoryMetadata> & metadata_snapshot,
    const MergeTreeDataPartTTLInfos & ttl_infos,
    const String & index_name,
    time_t current_time,
    bool ttl_merges_allowed)
{
    if (!ttl_merges_allowed || !metadata_snapshot->hasAnyIndexClearTTL())
        return false;

    for (const auto & ttl : metadata_snapshot->getIndexClearTTLs())
    {
        if (ttl.index_name != index_name)
            continue;

        auto ttl_info_it = ttl_infos.index_clear_ttl.find(ttl.result_column);
        if (ttl_info_it == ttl_infos.index_clear_ttl.end())
            continue;

        const time_t max_ttl = ttl_info_it->second.max;
        if (max_ttl && max_ttl <= current_time)
            return true;
    }

    return false;
}

std::set<MergeTreeIndexPtr> getIndexesExpiredByClearTTL(
    const std::shared_ptr<const StorageInMemoryMetadata> & metadata_snapshot,
    const MergeTreeSettings & settings,
    const MergeTreeDataPartTTLInfos & ttl_infos,
    time_t current_time,
    bool ttl_merges_allowed)
{
    std::set<MergeTreeIndexPtr> result;
    if (!ttl_merges_allowed || !metadata_snapshot->hasAnyIndexClearTTL())
        return result;

    const auto & index_factory = MergeTreeIndexFactory::instance();
    for (const auto & index : metadata_snapshot->getSecondaryIndices())
    {
        if (isIndexExpiredByTTL(metadata_snapshot, ttl_infos, index.name, current_time, ttl_merges_allowed))
            result.insert(index_factory.get(metadata_snapshot, index, settings));
    }

    return result;
}

bool markExpiredIndexClearTTLsFinished(
    const std::shared_ptr<const StorageInMemoryMetadata> & metadata_snapshot,
    MergeTreeDataPartTTLInfos & ttl_infos,
    time_t current_time)
{
    bool updated = false;
    for (const auto & ttl : metadata_snapshot->getIndexClearTTLs())
    {
        const auto it = ttl_infos.index_clear_ttl.find(ttl.result_column);
        if (it != ttl_infos.index_clear_ttl.end()
            && !it->second.finished()
            && it->second.max
            && it->second.max <= current_time)
        {
            it->second.ttl_finished = true;
            updated = true;
        }
    }
    return updated;
}

SkipIndexClearFiles getClearIndexFilesToClear(
    const std::shared_ptr<const IMergeTreeDataPart> & part,
    const std::shared_ptr<const StorageInMemoryMetadata> & metadata_snapshot,
    time_t current_time,
    bool ttl_merges_allowed)
{
    const auto indexes = getIndexesExpiredByClearTTL(
        metadata_snapshot,
        *part->storage.getSettings(),
        part->ttl_infos,
        current_time,
        ttl_merges_allowed);

    return collectSkipIndexClearFiles(
        indexes,
        part->getMarksFileExtension(),
        part->checksums,
        part->getDataPartStorage());
}

NameSet getSkipIndexSubstreamFileNames(
    const std::set<MergeTreeIndexPtr> & indexes,
    const String & mrk_extension,
    const MergeTreeDataPartChecksums & checksums,
    const IDataPartStorage * storage)
{
    NameSet result;
    for (const auto & index : indexes)
    {
        const String file_name = index->getFileName();
        auto add_file = [&](const String & stream_name, const String & extension)
        {
            result.insert(stream_name + extension);
            if (auto actual = IMergeTreeDataPart::getStreamNameOrHash(stream_name, extension, checksums))
                result.insert(*actual + extension);
            if (storage)
                if (auto actual = IMergeTreeDataPart::getStreamNameOrHash(stream_name, extension, *storage))
                    result.insert(*actual + extension);
        };

        for (const auto & substream : index->getAllSubstreamsInPart(checksums, file_name, storage))
        {
            const String stream_name = file_name + substream.suffix;
            add_file(stream_name, substream.extension);
            add_file(stream_name, mrk_extension);
        }

        /// Corrupted parts may contain standalone index files omitted from `checksums.txt`.
        /// Probe every declared substream and both historical data-file extensions so cleanup
        /// can remove those orphans. This runs only after a cleanup has been selected.
        for (const auto & substream : index->getSubstreams())
        {
            const String stream_name = file_name + substream.suffix;
            add_file(stream_name, substream.extension);
            add_file(stream_name, mrk_extension);
            add_file(stream_name, ".idx");
            add_file(stream_name, ".idx2");
        }
    }
    return result;
}

SkipIndexClearFiles collectSkipIndexClearFiles(
    const std::set<MergeTreeIndexPtr> & indexes,
    const String & mrk_extension,
    const MergeTreeDataPartChecksums & checksums,
    const IDataPartStorage & storage)
{
    SkipIndexClearFiles result;
    if (indexes.empty())
        return result;

    result.files = getSkipIndexSubstreamFileNames(indexes, mrk_extension, checksums, &storage);
    const auto * disk_storage = dynamic_cast<const DataPartStorageOnDiskBase *>(&storage);

    for (const auto & file : result.files)
    {
        const bool is_in_packed_archive = disk_storage && disk_storage->isFileInPackedSkipIndicesArchive(file);
        result.packed_archive_dirty |= is_in_packed_archive;
        result.has_existing_files |= checksums.has(file) || storage.existsFile(file) || is_in_packed_archive;
    }

    return result;
}

bool partHasSkipIndexFiles(const IMergeTreeDataPart & part, const MergeTreeIndexPtr & index)
{
    const auto & checksums = part.checksums;
    const auto & storage = part.getDataPartStorage();
    const auto checksum_candidates = getSkipIndexSubstreamFileNames(
        {index},
        part.getMarksFileExtension(),
        checksums);

    for (const auto & file : checksum_candidates)
        if (checksums.has(file))
            return true;

    if (checksums.has(String(SKIP_INDICES_PACKED_FILENAME)))
    {
        const auto * disk_storage = dynamic_cast<const DataPartStorageOnDiskBase *>(&storage);
        if (skipIndexHasFilesInPackedArchive(*index, disk_storage, part.getMarksFileExtension()))
            return true;
    }

    /// Older mutations could preserve a full Wide part while omitting standalone index files
    /// from checksums. Other full parts stay on the checksum-only selector path so unmaterialized
    /// indexes do not cause repeated storage probes on every selection pass.
    const bool may_have_orphaned_standalone_files
        = storage.getType() == MergeTreeDataPartStorageType::Full
        && part.getType() == MergeTreeDataPartType::Wide
        && part.info.mutation != 0;
    if (storage.getType() == MergeTreeDataPartStorageType::Full && !may_have_orphaned_standalone_files)
        return false;

    return skipIndexHasStandaloneFiles(*index, storage, part.getMarksFileExtension());
}

bool skipIndexHasStandaloneFiles(
    const IMergeTreeIndex & index,
    const IDataPartStorage & storage,
    const String & mrk_extension)
{
    const String file_name = index.getFileName();
    for (const auto & substream : index.getSubstreams())
    {
        const String stream_name = file_name + substream.suffix;
        if (IMergeTreeDataPart::getStreamNameOrHash(stream_name, substream.extension, storage))
            return true;
        if (substream.extension == ".idx2"
            && IMergeTreeDataPart::getStreamNameOrHash(stream_name, ".idx", storage))
        {
            return true;
        }
        if (IMergeTreeDataPart::getStreamNameOrHash(stream_name, mrk_extension, storage))
            return true;
    }

    return false;
}

bool skipIndexHasFilesInPackedArchive(
    const IMergeTreeIndex & index,
    const DataPartStorageOnDiskBase * storage,
    const String & mrk_extension)
{
    if (!storage)
        return false;

    const String file_name = index.getFileName();
    for (const auto & substream : index.getSubstreams())
    {
        const String stream_name = file_name + substream.suffix;
        if (storage->isFileInPackedSkipIndicesArchive(stream_name + substream.extension))
            return true;
        if (storage->isFileInPackedSkipIndicesArchive(stream_name + mrk_extension))
            return true;
        if (storage->isFileInPackedSkipIndicesArchive(stream_name + ".idx"))
            return true;
        if (storage->isFileInPackedSkipIndicesArchive(stream_name + ".idx2"))
            return true;
    }

    return false;
}

NameSet getDroppedSkipIndexArchiveFileNames(
    const NameSet & dropped_index_names,
    const std::set<MergeTreeIndexPtr> & surviving_indexes,
    bool escape_index_filenames,
    const String & mrk_extension,
    const IMergeTreeDataPart & part,
    const DataPartStorageOnDiskBase & storage)
{
    NameSet surviving_index_owned_files;
    for (const auto & index : surviving_indexes)
    {
        if (!part.isSkipIndexInPackedArchive(*index))
            continue;

        const String file_name = index->getFileName();
        for (const auto & substream : index->getAllSubstreamsInPart(part.checksums, file_name, &part.getDataPartStorage()))
        {
            surviving_index_owned_files.insert(file_name + substream.suffix + substream.extension);
            surviving_index_owned_files.insert(file_name + substream.suffix + mrk_extension);
        }
    }

    NameSet result;
    static constexpr std::array<const char *, 4> known_substream_suffixes = {"", ".dct", ".pst", ".pos"};
    static constexpr std::array<const char *, 2> known_index_extensions = {".idx2", ".idx"};

    for (const auto & index_name : dropped_index_names)
    {
        const String index_file_name = getIndexFileName(index_name, escape_index_filenames);
        for (const auto * substream_suffix : known_substream_suffixes)
        {
            for (const auto * extension : known_index_extensions)
            {
                const String candidate = index_file_name + substream_suffix + extension;
                if (!surviving_index_owned_files.contains(candidate)
                    && storage.isFileInPackedSkipIndicesArchive(candidate))
                    result.insert(candidate);
            }

            const String mark_candidate = index_file_name + substream_suffix + mrk_extension;
            if (!surviving_index_owned_files.contains(mark_candidate)
                && storage.isFileInPackedSkipIndicesArchive(mark_candidate))
                result.insert(mark_candidate);
        }
    }

    return result;
}

}
