#pragma once

#include <Common/Logger.h>
#include <base/types.h>

#include <string>
#include <string_view>

/// Pure path-string manipulation, independent of the Keeper client itself.
/// Include this instead of the (very heavy) `Common/ZooKeeper/ZooKeeper.h` when only paths are handled.
namespace zkutil
{

/// Path "default:/foo" refers to znode "/foo" in the default zookeeper,
/// path "other:/foo" refers to znode "/foo" in auxiliary zookeeper named "other".
constexpr std::string_view DEFAULT_ZOOKEEPER_NAME = "default";

String normalizeZooKeeperPath(std::string zookeeper_path, bool check_starts_with_slash, LoggerPtr log = nullptr);

String extractZooKeeperName(const String & path);

/// Joins a ZooKeeper path with a child name, with exactly one `/` between them.
///
/// Spelled out rather than delegated to `std::filesystem::path::operator/`, which is what much of
/// this used to do: a ZooKeeper path is not a filesystem path, and `operator/` appends the host's
/// preferred separator - a backslash on Windows, where in a znode name it is an ordinary
/// character.
String joinZooKeeperPath(std::string_view parent, std::string_view child);

/// The same for more than two segments, joined left to right.
template <typename... Rest>
String joinZooKeeperPath(std::string_view parent, std::string_view child, Rest... rest)
{
    const String joined = joinZooKeeperPath(parent, child);
    return joinZooKeeperPath(std::string_view(joined), rest...);
}

/// The parent of a ZooKeeper path: everything before its last `/`. `/a/b` gives `/a` and `/a`
/// gives `/`; a path with no `/` at all has no parent and gives the empty string.
///
/// Spelled out rather than delegated to `std::filesystem::path::parent_path` for the same reason
/// as `joinZooKeeperPath`: a znode path is not a filesystem path, and on Windows the filesystem
/// layer also treats `\` as a separator and reinterprets the bytes through the active code page.
/// A znode path never ends in a separator, so no trailing-`/` case is defined here.
String parentZooKeeperPath(std::string_view path);

/// The name of the znode a ZooKeeper path refers to: everything after its last `/`.
String zooKeeperNodeName(std::string_view path);

/// Collapses `.` and `..` components and runs of `/` in a ZooKeeper path, purely lexically.
/// A `..` at the root of an absolute path is dropped. The result never ends in `/` unless it is
/// the root itself.
///
/// This is what `std::filesystem::path::lexically_normal` would do, minus the filesystem: for a
/// znode path `std::filesystem` is wrong on Windows, where it treats `\` as a separator and
/// reads the bytes through the active code page.
String lexicallyNormalizeZooKeeperPath(std::string_view path);

String extractZooKeeperPath(const String & path, bool check_starts_with_slash, LoggerPtr log = nullptr);

/// Like extractZooKeeperPath, but collapses ALL trailing slashes (not just one) into a canonical form,
/// so that "/a", "/a/" and "/a//" compare equal. Use when comparing keeper paths for equality.
String extractZooKeeperPathAndCollapseTrailingSlashes(const String & path, bool check_starts_with_slash, LoggerPtr log = nullptr);

}
