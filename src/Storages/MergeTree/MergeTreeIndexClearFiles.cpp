#include <Storages/MergeTree/MergeTreeIndexClearFiles.h>

#include <Storages/MergeTree/DataPartStorageOnDiskBase.h>
#include <Storages/MergeTree/IMergeTreeDataPart.h>
#include <Storages/MergeTree/IDataPartStorage.h>
#include <Storages/MergeTree/MergeTreeDataPartChecksum.h>

#include <array>
#include <filesystem>
#include <Common/StringUtils.h>

namespace DB
{

NameSet getSkipIndexSubstreamFileNames(
    const std::set<MergeTreeIndexPtr> & indexes,
    const String & mrk_extension,
    const MergeTreeDataPartChecksums & checksums,
    const IDataPartStorage * storage)
{
    NameSet result;
    for (const auto & index : indexes)
    {
        for (const auto & substream : index->getSubstreams())
        {
            const String stream_name = index->getFileName() + substream.suffix;

            /// Current logical names.
            result.insert(stream_name + substream.extension);
            result.insert(stream_name + mrk_extension);

            /// Legacy / compatible data-file names. Minmax moved from `.idx` to `.idx2`;
            /// DROP/CLEAR INDEX must probe both when removing inherited files/checksums.
            result.insert(stream_name + ".idx");
            result.insert(stream_name + ".idx2");

            auto add_actual_name = [&](const String & extension)
            {
                if (auto actual = IMergeTreeDataPart::getStreamNameOrHash(stream_name, extension, checksums))
                    result.insert(*actual + extension);
                if (storage)
                    if (auto actual = IMergeTreeDataPart::getStreamNameOrHash(stream_name, extension, *storage))
                        result.insert(*actual + extension);
            };

            add_actual_name(substream.extension);
            add_actual_name(".idx");
            add_actual_name(".idx2");
            add_actual_name(mrk_extension);
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
    bool escape_index_filenames,
    const String & mrk_extension,
    const DataPartStorageOnDiskBase & storage)
{
    NameSet result;

    static constexpr std::array<const char *, 3> known_substream_suffixes = {"", ".dct", ".pst"};
    static constexpr std::array<const char *, 2> known_index_extensions = {".idx2", ".idx"};

    for (const auto & index_name : dropped_index_names)
    {
        const String index_file_name = getIndexFileName(index_name, escape_index_filenames);
        for (const auto * substream_suffix : known_substream_suffixes)
        {
            for (const auto * extension : known_index_extensions)
            {
                const String candidate = index_file_name + substream_suffix + extension;
                if (storage.isFileInPackedSkipIndicesArchive(candidate))
                    result.insert(candidate);
            }

            const String mark_candidate = index_file_name + substream_suffix + mrk_extension;
            if (storage.isFileInPackedSkipIndicesArchive(mark_candidate))
                result.insert(mark_candidate);
        }
    }

    return result;
}


namespace
{

bool shouldCopyPartFileEntry(const String & name, const PartFileCopyOptions & options)
{
    if (options.files_to_copy)
        return options.files_to_copy->contains(name);
    return !(options.files_to_skip && options.files_to_skip->contains(name));
}

}

bool canCopyPartFilesWithSkip(
    const IDataPartStorage & source_storage,
    const PartFileCopyOptions & options)
{
    for (auto it = source_storage.iterate(); it->isValid(); it->next())
    {
        const auto name = it->name();
        if (!shouldCopyPartFileEntry(name, options) || it->isFile())
            continue;

        if (endsWith(name, ".tmp_proj"))
            return !options.fail_on_temporary_projection_directories;

        auto projection_src = source_storage.getProjection(name);
        for (auto projection_it = projection_src->iterate(); projection_it->isValid(); projection_it->next())
            if (!projection_it->isFile() && options.fail_on_projection_subdirectories)
                return false;
    }

    return true;
}

std::optional<NameSet> copyPartFilesWithSkip(
    const IDataPartStorage & source_storage,
    IDataPartStorage & destination_storage,
    const PartFileCopyOptions & options)
{
    if (!canCopyPartFilesWithSkip(source_storage, options))
        return std::nullopt;

    NameSet hardlinked_files;

    for (auto it = source_storage.iterate(); it->isValid(); it->next())
    {
        const auto name = it->name();
        if (!shouldCopyPartFileEntry(name, options))
            continue;

        if (it->isFile())
        {
            const bool copy_file = options.copy_instead_of_hardlinks
                || source_storage.getDiskName() != destination_storage.getDiskName();
            if (copy_file)
                destination_storage.copyFileFrom(source_storage, name, name);
            else
            {
                destination_storage.createHardLinkFrom(source_storage, name, name);
                hardlinked_files.insert(name);
            }
            continue;
        }

        if (endsWith(name, ".tmp_proj"))
            continue;

        destination_storage.createProjection(name);
        auto projection_src = source_storage.getProjection(name);
        auto projection_dst = destination_storage.getProjection(name);
        const bool copy_projection_file = options.copy_instead_of_hardlinks
            || projection_src->getDiskName() != projection_dst->getDiskName();

        for (auto projection_it = projection_src->iterate(); projection_it->isValid(); projection_it->next())
        {
            if (!projection_it->isFile())
                continue;

            const auto projection_file = projection_it->name();
            if (copy_projection_file)
                projection_dst->copyFileFrom(*projection_src, projection_file, projection_file);
            else
            {
                projection_dst->createHardLinkFrom(*projection_src, projection_file, projection_file);
                hardlinked_files.insert((std::filesystem::path(projection_src->getPartDirectory()) / projection_file).string());
            }
        }
    }

    return hardlinked_files;
}

}
