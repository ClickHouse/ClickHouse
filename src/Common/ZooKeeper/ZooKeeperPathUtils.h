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

String extractZooKeeperPath(const String & path, bool check_starts_with_slash, LoggerPtr log = nullptr);

/// Like extractZooKeeperPath, but collapses ALL trailing slashes (not just one) into a canonical form,
/// so that "/a", "/a/" and "/a//" compare equal. Use when comparing keeper paths for equality.
String extractZooKeeperPathAndCollapseTrailingSlashes(const String & path, bool check_starts_with_slash, LoggerPtr log = nullptr);

}
