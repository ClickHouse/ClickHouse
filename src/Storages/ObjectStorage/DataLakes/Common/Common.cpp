#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/ProfileEvents.h>
#include <Common/filesystemHelpers.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <filesystem>

#include <fmt/ranges.h>

namespace ProfileEvents
{
    extern const Event DeltaLakeDeltaLogExistenceChecks;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int PATH_ACCESS_DENIED;
}

PrefixListing listPrefix(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix, const String & suffix)
{
    return listPrefix(
        object_storage,
        path,
        prefix,
        [&suffix](const RelativePathWithMetadata & files_with_metadata) { return files_with_metadata.relative_path.ends_with(suffix); });
}


PrefixListing listPrefix(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need)
{
    PrefixListing listing;
    listing.listed_prefix = std::filesystem::path(path) / prefix;
    object_storage.listObjects(listing.listed_prefix, listing.entries, 0);
    for (const auto & file_with_metadata : listing.entries)
    {
        if (check_need(*file_with_metadata))
            listing.matched.push_back(file_with_metadata->relative_path);
    }
    LOG_TRACE(
        getLogger("DataLakeCommon"),
        "Listed {} of {} files ({})",
        listing.matched.size(),
        listing.entries.size(),
        fmt::join(listing.matched, ", "));
    return listing;
}

std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix, const String & suffix)
{
    return listPrefix(object_storage, path, prefix, suffix).matched;
}


std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need)
{
    return listPrefix(object_storage, path, prefix, check_need).matched;
}

namespace
{

String relativeToPrefix(const String & key, const String & listed_prefix)
{
    if (!key.starts_with(listed_prefix) || key.size() == listed_prefix.size())
        return key;
    return key.substr(listed_prefix.size());
}

String joinCapped(const std::vector<String> & names, size_t total)
{
    String res = fmt::format("{}", fmt::join(names, ", "));
    if (total > names.size())
        res += fmt::format("{}and {} more", names.empty() ? "" : ", ", total - names.size());
    return res;
}

}

String describeListedObjects(const RelativePathsWithMetadata & entries, const String & listed_prefix, size_t max_entries)
{
    if (entries.empty())
        return "0 entries";

    const size_t shown = std::min(entries.size(), max_entries);
    std::vector<String> names;
    names.reserve(shown);
    for (size_t i = 0; i < shown; ++i)
    {
        const auto & entry = entries[i];
        String name = relativeToPrefix(entry->relative_path, listed_prefix);
        const auto & metadata = entry->metadata;
        if (metadata && metadata->is_last_modified_known)
        {
            WriteBufferFromOwnString last_modified;
            writeDateTimeTextISO(metadata->last_modified.epochTime(), last_modified, DateLUT::instance("UTC"));
            name += fmt::format(" ({})", last_modified.str());
        }
        names.push_back(std::move(name));
    }
    return fmt::format(
        "{} {}: {}", entries.size(), entries.size() == 1 ? "entry" : "entries", joinCapped(names, entries.size()));
}

String describeObjectKeys(const std::vector<String> & keys, const String & listed_prefix, size_t max_entries)
{
    const size_t shown = std::min(keys.size(), max_entries);
    std::vector<String> names;
    names.reserve(shown);
    for (size_t i = 0; i < shown; ++i)
        names.push_back(relativeToPrefix(keys[i], listed_prefix));
    return joinCapped(names, keys.size());
}

bool deltaLogExists(const IObjectStorage & object_storage, const String & path)
{
    ProfileEvents::increment(ProfileEvents::DeltaLakeDeltaLogExistenceChecks);
    const auto delta_log_dir = (std::filesystem::path(path) / "_delta_log").string() + "/";
    RelativePathsWithMetadata files;
    object_storage.listObjects(delta_log_dir, files, /* max_keys */ 1);
    return !files.empty();
}

String resolvePathInsideTable(const String & table_path, const String & relative_path)
{
    auto base = std::filesystem::path(table_path);
    auto combined = base / relative_path;

    if (!pathStartsWith(combined, base))
        throw Exception(
            ErrorCodes::PATH_ACCESS_DENIED,
            "Data lake path `{}` should be inside the table directory `{}`",
            relative_path,
            table_path);

    return combined.string();
}
}
