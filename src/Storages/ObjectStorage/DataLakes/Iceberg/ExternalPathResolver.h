#pragma once

#include "config.h"

#include <Disks/DiskObjectStorage/ObjectStorages/IObjectStorage_fwd.h>
#include <Interpreters/Context_fwd.h>

#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <utility>

namespace DB
{

struct ObjectInfo;
using ObjectInfoPtr = std::shared_ptr<ObjectInfo>;

/// A URI split into components
///  s3://bucket/a/b -> scheme="s3", authority="bucket", key="a/b"
///  file:///var/x   -> scheme="file", authority="",     key="/var/x"
///  /abs/p          -> scheme="",     authority="",     key="/abs/p"
/// Text before a colon counts as a scheme only when it is shaped like one (RFC 3986).
struct SchemeAuthorityKey
{
    explicit SchemeAuthorityKey(const std::string & uri);

    std::string scheme;
    std::string authority;
    std::string key;
};

#if USE_AVRO

namespace Iceberg { class IcebergPathResolver; }

/// The object storages built on demand for the files an Iceberg table places outside its own storage,
/// keyed by the identity `tryResolveObjectStorageForPath` derives (endpoint, bucket and, when
/// credentials are propagated, their generation). The table's own storage is absent: it is passed
/// alongside as `base_storage`, and a path that resolves to it needs no entry.
struct ExternalStorageCache
{
    mutable std::mutex mutex;
    std::map<std::string, ObjectStoragePtr> storages;
};

/// Resolve an absolute metadata path directly to its (object storage, key) by parsing the URI. Returns
/// std::nullopt for paths that must go through `path_resolver`: relative ones, and absolute ones under
/// the table's declared location.
std::optional<std::pair<ObjectStoragePtr, std::string>> tryResolveObjectStorageForPath(
    const std::string & table_location,
    const std::string & path,
    const ObjectStoragePtr & base_storage,
    ExternalStorageCache & external_storages,
    const ContextPtr & context);

/// Whether the user's grant covers what `path` names, decided as for a read but without building the
/// storage. True for a path the table's own grant covers, and for one that names no target to authorize.
/// For `system.iceberg_files`, which reports every manifest entry, including files no read could open.
bool isPathReadGranted(
    const std::string & table_location,
    const std::string & path,
    const ObjectStoragePtr & base_storage,
    const ContextPtr & context);

/// Whether a read of `path` would reach the file: the grant covers it and nothing rejects the path
/// itself. For the metadata-only answers that stand in for a scan, which must not answer for a file the
/// scan would refuse to open.
bool isPathReadable(
    const std::string & table_location,
    const std::string & path,
    const ObjectStoragePtr & base_storage,
    const ContextPtr & context);

/// Resolve a metadata path to (object storage, key) for reading. Paths outside the table's declared
/// location resolve directly via `tryResolveObjectStorageForPath`; the rest are mapped by
/// `path_resolver`, which re-roots them onto the directory the table is really read from.
std::pair<ObjectStoragePtr, std::string> resolveObjectStorageForPath(
    const std::string & table_location,
    const std::string & path,
    const ObjectStoragePtr & base_storage,
    ExternalStorageCache & external_storages,
    const ContextPtr & context,
    const Iceberg::IcebergPathResolver & path_resolver);

/// Give `object` back the storage it is read from, on the node that will read it: a worker receives only
/// the path the metadata spells, since an object storage cannot be sent over the wire. Does nothing for
/// an object that is not a data lake object, or whose storage is already known.
void resolveObjectStorageFromDataLakeMetadata(
    const ObjectInfoPtr & object,
    const std::string & table_location,
    const ObjectStoragePtr & base_storage,
    ExternalStorageCache & external_storages,
    const ContextPtr & context);

#endif

}
