#include <Storages/MergeTree/PartFileCopy.h>

#include <Storages/MergeTree/IDataPartStorage.h>

#include <IO/copyData.h>

#include <Common/StringUtils.h>

#include <filesystem>

namespace DB
{

namespace
{

bool shouldCopyPartFileEntry(const String & name, const PartFileCopyOptions & options)
{
    if (options.files_to_copy)
        return options.files_to_copy->contains(name);
    return !(options.files_to_skip && options.files_to_skip->contains(name));
}

void copyPartFile(
    const IDataPartStorage & source_storage,
    IDataPartStorage & destination_storage,
    const String & name,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings,
    bool sync,
    const std::function<void()> & cancellation_callback)
{
    auto source = source_storage.readFile(name, read_settings, std::nullopt);
    auto destination = destination_storage.writeFile(name, DBMS_DEFAULT_BUFFER_SIZE, write_settings);
    try
    {
        if (cancellation_callback)
            copyData(*source, *destination, cancellation_callback);
        else
            copyData(*source, *destination);
        destination->finalize();
        if (sync)
            destination->sync();
    }
    catch (...)
    {
        destination->cancel();
        throw;
    }
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
        {
            if (options.fail_on_temporary_projection_directories)
                return false;
            continue;
        }

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
    const PartFileCopyOptions & options,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings)
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
                copyPartFile(
                    source_storage,
                    destination_storage,
                    name,
                    read_settings,
                    write_settings,
                    options.sync_copied_files,
                    options.cancellation_callback);
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
                copyPartFile(
                    *projection_src,
                    *projection_dst,
                    projection_file,
                    read_settings,
                    write_settings,
                    options.sync_copied_files,
                    options.cancellation_callback);
            else
            {
                projection_dst->createHardLinkFrom(*projection_src, projection_file, projection_file);
                hardlinked_files.insert((std::filesystem::path(projection_src->getPartDirectory()) / projection_file).string());
            }
        }

        if (options.checkpoint_after_projection)
            destination_storage.checkpointTransaction();
    }

    return hardlinked_files;
}

}
