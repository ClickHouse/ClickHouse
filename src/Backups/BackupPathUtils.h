#pragma once

#include <base/types.h>

#include <string_view>

namespace DB
{

/// Joins a path inside a backup with a child name, with exactly one `/` between them.
///
/// A path inside a backup is a logical entry name, not a filesystem path: `IBackup::listFiles`,
/// `IBackup::hasFiles` and the keys of `BackupEntries` are matched as strings against exactly
/// what the backup side wrote, whatever the backup is stored on - a directory, an object store or
/// a `.zip` archive.
///
/// Spelled out rather than delegated to `std::filesystem::path::operator/`, which is what these
/// joins used to do: on Windows that appends a backslash and reads the UTF-8 bytes through the
/// process's active code page, so an entry could be written under one name and looked for under
/// another - and the restore would find nothing.
String joinBackupPath(std::string_view parent, std::string_view child);

/// The same for more than two segments, joined left to right.
template <typename... Rest>
String joinBackupPath(std::string_view parent, std::string_view child, Rest... rest)
{
    const String joined = joinBackupPath(parent, child);
    return joinBackupPath(std::string_view(joined), rest...);
}

/// The name of the entry a path inside a backup refers to: everything after its last `/`.
String backupPathBaseName(std::string_view path);

/// Everything before the last `/` of a path inside a backup, without the separator itself.
/// A path with no `/` at all has no parent and gives the empty string.
String backupPathParent(std::string_view path);

}
