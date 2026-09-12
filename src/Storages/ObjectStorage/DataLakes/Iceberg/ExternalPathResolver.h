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
/// Text before a colon counts as a scheme only when it is shaped like one (RFC 3986), so an object
/// key that merely contains a colon keeps its whole spelling.
struct SchemeAuthorityKey
{
    explicit SchemeAuthorityKey(const std::string & uri);

    std::string scheme;
    std::string authority;
    std::string key;
};

#if USE_AVRO

namespace Iceberg { class IcebergPathResolver; }

/// The object storages built on demand for the files an Iceberg table places outside its own
/// storage, keyed by the identity `tryResolveObjectStorageForPath` derives for each target
/// (endpoint, bucket and, when credentials are propagated, their generation).
///
/// The table's own storage is deliberately absent: it is not built here but passed alongside as
/// `base_storage`, and a path that resolves to it needs no entry. So this holds exactly the
/// storages that would otherwise be rebuilt for every file that names one of them.
struct ExternalStorageCache
{
    mutable std::mutex mutex;
    std::map<std::string, ObjectStoragePtr> storages;
};

/// Resolve an absolute metadata path directly to its (object storage, key) by parsing the URI.
/// The storage may be `base_storage` or one built for an external location. Returns std::nullopt for
/// paths that must instead go through `path_resolver`: relative paths, and absolute paths that lie
/// under the table's declared location.
std::optional<std::pair<ObjectStoragePtr, std::string>> tryResolveObjectStorageForPath(
    const std::string & table_location,
    const std::string & path,
    const ObjectStoragePtr & base_storage,
    ExternalStorageCache & external_storages,
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

/// Give `object` back the storage it is read from, on the node that will read it. A worker receives
/// only the path the table's metadata spells, because an object storage cannot be sent over the wire,
/// so a file the metadata places outside the table's own storage has to be resolved again here.
/// Does nothing for an object that is not a data lake object, or whose storage is already known.
void resolveObjectStorageFromDataLakeMetadata(
    const ObjectInfoPtr & object,
    const std::string & table_location,
    const ObjectStoragePtr & base_storage,
    ExternalStorageCache & external_storages,
    const ContextPtr & context);

#endif

}
