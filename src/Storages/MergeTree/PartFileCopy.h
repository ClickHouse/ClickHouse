#pragma once

#include <Core/Names.h>

#include <functional>
#include <optional>

namespace DB
{

class IDataPartStorage;
struct ReadSettings;
struct WriteSettings;

struct PartFileCopyOptions
{
    const NameSet * files_to_skip = nullptr;
    const NameSet * files_to_copy = nullptr;
    bool copy_instead_of_hardlinks = false;
    bool fail_on_temporary_projection_directories = false;
    bool fail_on_projection_subdirectories = false;
    bool checkpoint_after_projection = false;
    bool sync_copied_files = false;
    std::function<void()> cancellation_callback;
};

/// Return false if `copyPartFilesWithSkip` would reject the source part before copying anything.
bool canCopyPartFilesWithSkip(
    const IDataPartStorage & source_storage,
    const PartFileCopyOptions & options);

/// Copy or hardlink source part files into destination according to the skip/include sets.
/// Projection files are copied one level deep; nested directories are rejected or skipped according
/// to the options. Returned names are the source files that were hardlinked, using projection-prefixed
/// names for projection files to match mutation tracking.
std::optional<NameSet> copyPartFilesWithSkip(
    const IDataPartStorage & source_storage,
    IDataPartStorage & destination_storage,
    const PartFileCopyOptions & options,
    const ReadSettings & read_settings,
    const WriteSettings & write_settings);

}
