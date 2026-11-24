#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/PlainRewritableLayout.h>
#include <Disks/DiskObjectStorage/MetadataStorages/NormalizedPath.h>

#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <Common/Exception.h>
#include <Common/getRandomASCIIString.h>
#include <base/find_symbols.h>

#include <fmt/format.h>

#include <algorithm>
#include <vector>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

PlainRewritableLayout::PlainRewritableLayout(std::string object_storage_common_key_prefix_)
    : object_storage_common_key_prefix(object_storage_common_key_prefix_)
{
}

std::string PlainRewritableLayout::constructMetadataDirectoryKey() const
{
    return object_storage_common_key_prefix / METADATA_DIRECTORY_TOKEN;
}

std::string PlainRewritableLayout::constructSnapshotObjectKey() const
{
    return object_storage_common_key_prefix / METADATA_DIRECTORY_TOKEN / SNAPSHOT_FILE_NAME;
}

std::string PlainRewritableLayout::constructRootFilesDirectoryKey() const
{
    return object_storage_common_key_prefix / ROOT_DIRECTORY_TOKEN;
}

std::string PlainRewritableLayout::constructFilesDirectoryKey(const std::string & directory_remote_path) const
{
    return object_storage_common_key_prefix / directory_remote_path;
}

std::string PlainRewritableLayout::constructFileObjectKey(const std::string & directory_remote_path, const std::string & file_name) const
{
    return object_storage_common_key_prefix / directory_remote_path / file_name;
}

std::string PlainRewritableLayout::constructBlobObjectKey(const std::string & blob_key) const
{
    return object_storage_common_key_prefix / blob_key;
}

std::string PlainRewritableLayout::constructDirectoryObjectKey(const std::string & directory_remote_path) const
{
    return object_storage_common_key_prefix / METADATA_DIRECTORY_TOKEN / directory_remote_path / PREFIX_PATH_FILE_NAME;
}

std::string PlainRewritableLayout::generateRemovedName()
{
    return REMOVED_NAME_PREFIX + getRandomASCIIString(REMOVED_NAME_RANDOM_PART_SIZE);
}

bool PlainRewritableLayout::isRemovedName(std::string_view name)
{
    if (name.size() != REMOVED_NAME_PREFIX.size() + REMOVED_NAME_RANDOM_PART_SIZE)
        return false;

    if (!name.starts_with(REMOVED_NAME_PREFIX))
        return false;

    /// The alphabet of `getRandomASCIIString`.
    return std::all_of(name.begin() + REMOVED_NAME_PREFIX.size(), name.end(), [](char c) { return 'a' <= c && c <= 'z'; });
}

bool PlainRewritableLayout::isRemovedLocalPath(const std::string & local_path)
{
    return getRemovedNameOfLocalPath(local_path).has_value();
}

std::optional<std::string> PlainRewritableLayout::getRemovedNameOfLocalPath(const std::string & local_path)
{
    const auto normalized_path = normalizePath(local_path);
    if (normalized_path.empty())
        return std::nullopt;

    auto name = normalized_path.begin()->string();
    if (!isRemovedName(name))
        return std::nullopt;

    return name;
}

std::string PlainRewritableLayout::restoreLocalPathOfPendingRemoval(const std::string & local_path, const std::string & original_local_path)
{
    const auto normalized_path = normalizePath(local_path);
    std::filesystem::path relative_path;
    for (auto it = std::next(normalized_path.begin()); it != normalized_path.end(); ++it)
        relative_path /= *it;

    /// The same shape as the paths `MoveDirectoryOperation` writes: `path_from / subdir / ""`.
    return (std::filesystem::path(original_local_path) / relative_path / "").string();
}

std::string PlainRewritableLayout::makeCommittedTombstoneContent(const std::string & removed_name)
{
    return removed_name;
}

std::string PlainRewritableLayout::makePendingTombstoneContent(const std::string & original_local_path)
{
    /// The line feed terminates the content, so it cannot be a part of the path.
    if (original_local_path.contains('\n'))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot remove '{}' on a plain_rewritable disk: the path contains a line feed", original_local_path);

    return fmt::format("{}{}\n", PENDING_TOMBSTONE_PREFIX, original_local_path);
}

std::string PlainRewritableLayout::makePendingReplaceTombstoneContent(const PendingReplace & pending_replace)
{
    /// The remote path of a directory is generated, so it never contains a line feed.
    if (pending_replace.directory_remote_path.contains('\n'))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot replace '{}' on a plain_rewritable disk: the remote path of its directory contains a line feed", pending_replace.file_name);

    /// The name of a file may contain a line feed, which separates the fields. Only such a name is written escaped,
    /// under another prefix: the content for any other name stays the one that older versions write and read.
    if (!pending_replace.file_name.contains('\n'))
        return fmt::format("{}{}\n{}\n{}\n", PENDING_REPLACE_TOMBSTONE_PREFIX, pending_replace.directory_remote_path, pending_replace.file_name, pending_replace.size);

    WriteBufferFromOwnString out;
    writeString(PENDING_REPLACE_ESCAPED_TOMBSTONE_PREFIX, out);
    writeString(pending_replace.directory_remote_path, out);
    writeChar('\n', out);
    writeEscapedString(pending_replace.file_name, out);
    writeChar('\n', out);
    writeIntText(pending_replace.size, out);
    writeChar('\n', out);
    return out.str();
}

std::string PlainRewritableLayout::makeBlobRemovalTombstoneContent(const std::string & blob_key)
{
    /// The key is escaped the same way as in the file list of `prefix.path`: the name of a file may contain a line
    /// feed, and so may the key of its blob, while a line feed terminates the content.
    WriteBufferFromOwnString out;
    writeString(BLOB_REMOVAL_TOMBSTONE_PREFIX, out);
    writeEscapedString(blob_key, out);
    writeChar('\n', out);
    return out.str();
}

bool PlainRewritableLayout::isValidBlobKey(std::string_view blob_key)
{
    if (blob_key.empty() || blob_key.starts_with('/') || blob_key.contains('\0'))
        return false;

    std::vector<std::string> parts;
    splitInto<'/'>(parts, blob_key);
    if (parts.front() == METADATA_DIRECTORY_TOKEN)
        return false;

    return std::none_of(parts.begin(), parts.end(), [](const std::string & part) { return part == "." || part == ".."; });
}

PlainRewritableLayout::TombstoneMarker PlainRewritableLayout::parseTombstoneMarkerContent(std::string_view content, std::string_view removed_name)
{
    using Kind = TombstoneMarker::Kind;

    if (content == removed_name)
        return {.kind = Kind::Committed, .pending_original_path = {}, .pending_replace = {}, .blob_key = {}};

    if (content.starts_with(PENDING_TOMBSTONE_PREFIX))
    {
        auto path = content.substr(PENDING_TOMBSTONE_PREFIX.size());
        if (!path.ends_with('\n'))
            return {};
        path.remove_suffix(1);

        /// `RemoveRecursiveOperation` writes the path of a directory, which ends with a slash.
        if (path.empty() || path.contains('\n') || !path.ends_with('/'))
            return {};

        return {.kind = Kind::PendingRemoval, .pending_original_path = std::string(path), .pending_replace = {}, .blob_key = {}};
    }

    const bool is_pending_replace_escaped = content.starts_with(PENDING_REPLACE_ESCAPED_TOMBSTONE_PREFIX);
    if (is_pending_replace_escaped || content.starts_with(PENDING_REPLACE_TOMBSTONE_PREFIX))
    {
        auto rest = content.substr(is_pending_replace_escaped ? PENDING_REPLACE_ESCAPED_TOMBSTONE_PREFIX.size() : PENDING_REPLACE_TOMBSTONE_PREFIX.size());
        if (!rest.ends_with('\n'))
            return {};
        rest.remove_suffix(1);

        std::vector<std::string> lines;
        splitInto<'\n'>(lines, rest);

        PendingReplace pending_replace;
        if (lines.size() != 3 || lines[0].empty() || lines[1].empty() || !tryParse(pending_replace.size, lines[2]))
            return {};

        if (is_pending_replace_escaped)
        {
            /// An escaped name has no line feeds and no tabs, so the whole line must be read as one string, see
            /// `makePendingReplaceTombstoneContent`. Anything else was not written by it.
            std::string file_name;
            ReadBufferFromString in(lines[1]);
            try
            {
                readEscapedString(file_name, in);
            }
            catch (const Exception &)
            {
                return {};
            }
            if (!in.eof() || file_name.empty())
                return {};

            lines[1] = std::move(file_name);
        }

        /// The backup is copied over the target, so the target must be a file of this disk, see `isValidBlobKey`.
        if (lines[1].contains('/') || !isValidBlobKey(lines[0] + "/" + lines[1]))
            return {};

        pending_replace.directory_remote_path = std::move(lines[0]);
        pending_replace.file_name = std::move(lines[1]);
        return {.kind = Kind::PendingReplace, .pending_original_path = {}, .pending_replace = std::move(pending_replace), .blob_key = {}};
    }

    if (content.starts_with(BLOB_REMOVAL_TOMBSTONE_PREFIX))
    {
        auto escaped_blob_key = content.substr(BLOB_REMOVAL_TOMBSTONE_PREFIX.size());
        if (!escaped_blob_key.ends_with('\n'))
            return {};
        escaped_blob_key.remove_suffix(1);

        /// The key is escaped, see `makeBlobRemovalTombstoneContent`. An escaped key has no line feeds and no tabs, so
        /// the whole content must be read as one string. Anything else was not written by it.
        std::string blob_key;
        ReadBufferFromString in(escaped_blob_key);
        try
        {
            readEscapedString(blob_key, in);
        }
        catch (const Exception &)
        {
            return {};
        }
        if (!in.eof())
            return {};

        /// The blob of a marker is deleted, so a key that would name an object outside of the blobs of this disk
        /// (in another prefix, or in `__meta`) is never taken from a damaged or forged marker.
        if (!isValidBlobKey(blob_key))
            return {};

        return {.kind = Kind::BlobRemoval, .pending_original_path = {}, .pending_replace = {}, .blob_key = std::move(blob_key)};
    }

    return {};
}

std::string PlainRewritableLayout::constructTombstoneDirectoryKey() const
{
    return object_storage_common_key_prefix / METADATA_DIRECTORY_TOKEN / TOMBSTONE_DIRECTORY_TOKEN;
}

std::string PlainRewritableLayout::constructTombstoneMarkerKey(const std::string & removed_name) const
{
    return object_storage_common_key_prefix / METADATA_DIRECTORY_TOKEN / TOMBSTONE_DIRECTORY_TOKEN / removed_name;
}

std::optional<std::string> PlainRewritableLayout::parseTombstoneMarkerKey(const std::string & key) const
{
    std::vector<std::string> key_parts;
    splitInto<'/'>(key_parts, key);

    if (key_parts.size() < 3)
        return std::nullopt;

    const size_t size = key_parts.size();
    if (key_parts[size - 3] != METADATA_DIRECTORY_TOKEN || key_parts[size - 2] != TOMBSTONE_DIRECTORY_TOKEN)
        return std::nullopt;

    if (!isRemovedName(key_parts[size - 1]))
        return std::nullopt;

    return std::move(key_parts[size - 1]);
}

std::optional<std::pair<std::string, std::string>> PlainRewritableLayout::parseFileObjectKey(const std::string & key) const
{
    std::vector<std::string> key_parts;
    splitInto<'/'>(key_parts, key);

    if (key_parts.size() < 2)
        return std::nullopt;

    const size_t size = key_parts.size();
    return std::make_pair(std::move(key_parts[size - 2]), std::move(key_parts[size - 1]));
}

std::optional<std::string> PlainRewritableLayout::parseDirectoryObjectKey(const std::string & key) const
{
    std::vector<std::string> key_parts;
    splitInto<'/'>(key_parts, key);

    if (key_parts.size() < 3)
        return std::nullopt;

    const size_t size = key_parts.size();
    if (key_parts[size - 3] != METADATA_DIRECTORY_TOKEN)
        return std::nullopt;
    else if (key_parts[size - 1] != PREFIX_PATH_FILE_NAME)
        return std::nullopt;

    return std::move(key_parts[size - 2]);
}

}
