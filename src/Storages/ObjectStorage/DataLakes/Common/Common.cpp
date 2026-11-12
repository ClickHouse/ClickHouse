#include <base/pathToString.h>
#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage.h>
#include <Disks/DiskObjectStorage/ObjectStorages/Local/LocalObjectStorage.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Storages/ObjectStorage/DataLakes/Common/Common.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>
#include <Common/DateLUT.h>
#include <Common/Exception.h>
#include <Common/ObjectStorageKey.h>
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

namespace
{

/// `path`/`prefix`/, joined in object storage key space: the `std::filesystem` spelling `path / prefix / ""`
/// would append `\` on Windows, where it is an ordinary character in a key.
String keyDirectory(const String & path, const String & prefix)
{
    String key = appendObjectStorageKeySegment(path, prefix);
    if (!key.empty() && !key.ends_with('/'))
        key += '/';
    return key;
}

/// Whether `key`, returned by listing `directory`, is an object directly inside it. A storage may return the keys
/// without leading separators and with the directory in normal form (HDFS canonicalizes it).
bool isDirectlyInside(std::string_view key, const String & directory)
{
    while (key.starts_with('/'))
        key.remove_prefix(1);
    for (const String & spelling : {directory, pathToGenericString(pathFromString(directory).lexically_normal())})
    {
        std::string_view dir = spelling;
        while (dir.starts_with('/'))
            dir.remove_prefix(1);
        while (dir.ends_with('/'))
            dir.remove_suffix(1);
        std::string_view rest = key;
        if (!dir.empty())
        {
            if (!rest.starts_with(dir) || rest.size() == dir.size() || rest[dir.size()] != '/')
                continue;
            rest.remove_prefix(dir.size() + 1);
        }
        return !rest.empty() && !rest.contains('/');
    }
    return false;
}

/// Objects at any depth inside the directory `path`/`prefix` whose names inside it start with `name_prefix`.
PrefixListing listPrefixImpl(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const String & name_prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need)
{
    PrefixListing listing;
    /// An object storage key, not a filesystem path, so it is joined in key string space.
    /// Without the trailing separator the key also matches sibling directories such as `metadata_old/`.
    listing.listed_prefix = keyDirectory(path, prefix) + name_prefix;
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

std::function<bool(const RelativePathWithMetadata &)> endsWith(const String & suffix)
{
    return [suffix](const RelativePathWithMetadata & file_with_metadata) { return file_with_metadata.relative_path.ends_with(suffix); };
}

}

PrefixListing listPrefix(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix, const String & suffix)
{
    return listPrefixImpl(object_storage, path, prefix, "", endsWith(suffix));
}

PrefixListing listPrefix(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need)
{
    return listPrefixImpl(object_storage, path, prefix, "", check_need);
}

PrefixListing listPrefixNonRecursive(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const String & name_prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need)
{
    auto directory = keyDirectory(path, prefix);
    /// The local storage lists a relative directory under its root, the same way it resolves any other key.
    if (object_storage.getType() == ObjectStorageType::Local)
        directory = pathToGenericString(resolvePathRelativelyToBase(directory, object_storage.getCommonKeyPrefix()));
    return listPrefixImpl(
        object_storage,
        path,
        prefix,
        name_prefix,
        [&](const RelativePathWithMetadata & file_with_metadata)
        { return check_need(file_with_metadata) && isDirectlyInside(file_with_metadata.relative_path, directory); });
}

PrefixListing listPrefixNonRecursive(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const String & name_prefix,
    const String & suffix)
{
    return listPrefixNonRecursive(object_storage, path, prefix, name_prefix, endsWith(suffix));
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

std::vector<String> listFilesNonRecursive(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix, const String & suffix)
{
    return listPrefixNonRecursive(object_storage, path, prefix, "", suffix).matched;
}

std::vector<String> listFilesNonRecursive(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const String & name_prefix,
    const String & suffix)
{
    return listPrefixNonRecursive(object_storage, path, prefix, name_prefix, suffix).matched;
}

std::vector<String> listFilesNonRecursive(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need)
{
    return listPrefixNonRecursive(object_storage, path, prefix, "", check_need).matched;
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
    const auto delta_log_dir = appendObjectStorageKeySegment(path, "_delta_log") + "/";
    RelativePathsWithMetadata files;
    object_storage.listObjects(delta_log_dir, files, /* max_keys */ 1);
    return !files.empty();
}

String resolvePathInsideTable(const String & table_path, const String & relative_path)
{
    /// The containment test is what defines "inside" here, and it is `std::filesystem`'s notion of
    /// it, so the two keys are converted into paths explicitly rather than through the narrow
    /// constructor. The result is read back generically: `path::string()` would hand back the `\`
    /// that `operator/` appends on Windows, where it is an ordinary character in a key.
    const auto base = pathFromString(table_path);
    const auto combined = base / pathFromString(relative_path);

    if (!pathStartsWith(combined, base))
        throw Exception(
            ErrorCodes::PATH_ACCESS_DENIED,
            "Data lake path `{}` should be inside the table directory `{}`",
            relative_path,
            table_path);

    return pathToGenericString(combined);
}
}
