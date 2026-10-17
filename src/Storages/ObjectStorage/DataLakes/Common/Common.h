#pragma once
#include <functional>
#include <Core/Types.h>
#include <Storages/ObjectStorage/StorageObjectStorage.h>

namespace DB
{

class IObjectStorage;

/// One listing: the raw entries the object storage returned for `listed_prefix`, and the ones a filter kept.
struct PrefixListing
{
    String listed_prefix;
    RelativePathsWithMetadata entries;
    std::vector<String> matched;
};

PrefixListing listPrefix(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix, const String & suffix);

PrefixListing listPrefix(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need);

std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix, const String & suffix);

std::vector<String> listFiles(
    const IObjectStorage & object_storage,
    const String & path,
    const String & prefix,
    const std::function<bool(const RelativePathWithMetadata &)> & check_need);

constexpr size_t MAX_REPORTED_LISTING_ENTRIES = 10;

/// `N entries: name (modification time), ...` for an error message that reports absence, or `0 entries`.
/// A name is the part of the key after `listed_prefix`, or the whole key when there is none, and a modification
/// time is shown only where the storage reports one.
String describeListedObjects(const RelativePathsWithMetadata & entries, const String & listed_prefix, size_t max_entries);

/// `name, name, ... and K more`, relative to `listed_prefix`.
String describeObjectKeys(const std::vector<String> & keys, const String & listed_prefix, size_t max_entries);

/// True if a `_delta_log/` with any entry (not just `*.json`) exists at `path`, so a checkpoint-only log still counts as an existing table.
bool deltaLogExists(const IObjectStorage & object_storage, const String & path);

String resolvePathInsideTable(const String & table_path, const String & relative_path);
}
