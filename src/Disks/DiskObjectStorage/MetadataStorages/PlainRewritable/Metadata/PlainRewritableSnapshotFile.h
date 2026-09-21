#pragma once

#include <Disks/DiskObjectStorage/MetadataStorages/PlainRewritable/Metadata/FsSnapshot.h>

#include <string>
#include <unordered_map>

namespace DB
{

class ReadBuffer;
class WriteBuffer;

/// The whole state of a `plain_rewritable` disk: every directory with its remote (random) name mapped to
/// the logical path, and the files in it. It is what the metadata storage keeps in memory, in the flat
/// form that is loaded from the object storage and applied through `FsMetadata::applyLayout`.
using PlainRewritableRemoteLayout = std::unordered_map<std::string, DirectoryRemoteInfo>;

/** The snapshot file (`__meta/snapshot.bin`) is a compact serialized copy of `PlainRewritableRemoteLayout`.
  *
  * Building the state from scratch requires listing the `__meta` directory, reading every `prefix.path`
  * object in it and listing every directory, which is tens of thousands of requests even for a disk with
  * a single table. The snapshot is written by the disk that owns the data after its state changes and lets
  * a starting server (or a read-only replica of the disk) obtain the whole state in a single request.
  *
  * The format is:
  * - a `VarUInt` format version (`SNAPSHOT_FORMAT_VERSION`), written as is;
  * - the rest of the file is compressed with `ZSTD` in the native ClickHouse framing
  *   (`CompressedWriteBuffer`, checksummed blocks) and contains:
  *   - `VarUInt` number of directories, then for every directory, ordered by path:
  *     - `String` logical path (normalized, `""` for the root), `String` remote path, `String` ETag of `prefix.path`,
  *       `Int64` last modification time;
  *     - `VarUInt` number of files, then for every file, ordered by name:
  *       - `String` name, `VarUInt` size in bytes, `Int64` last modification time.
  *
  * The snapshot describes the state at some moment in the past and is not guaranteed to be consistent with
  * the objects present in the storage at the moment of reading. This is acceptable for the way `MergeTree`
  * tables use the disk, see the comments in `MetadataStorageFromPlainRewritableObjectStorage::load`.
  */
static constexpr UInt64 SNAPSHOT_FORMAT_VERSION = 1;

void writePlainRewritableSnapshot(const PlainRewritableRemoteLayout & layout, WriteBuffer & out);

/// Throws `UNKNOWN_FORMAT_VERSION` if the file was written by a newer version that changed the format,
/// and other exceptions on malformed content.
PlainRewritableRemoteLayout readPlainRewritableSnapshot(ReadBuffer & in);

}
