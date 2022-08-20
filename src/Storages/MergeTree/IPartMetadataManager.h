#pragma once

#include <unordered_map>
#include <city.h>
#include <base/types.h>

namespace DB
{

class IMergeTreeDataPart;

class SeekableReadBuffer;

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

/// Interface for managing metadata of merge tree part.
/// IPartMetadataManager has two implementations:
/// - PartMetadataManagerOrdinary: manage metadata from disk directly. deleteAll/assertAllDeleted/updateAll/check
///   are all empty implementations because they are not needed for PartMetadataManagerOrdinary(those operations
///   are done implicitly when removing or renaming part directory).
/// - PartMetadataManagerWithCache: manage metadata from RocksDB cache and disk.
class IPartMetadataManager
{
public:
    using uint128 = CityHash_v1_0_2::uint128;

    explicit IPartMetadataManager(const IMergeTreeDataPart * part_);

    virtual ~IPartMetadataManager() = default;

    /// Read metadata content and return SeekableReadBuffer object.
    virtual std::unique_ptr<SeekableReadBuffer> read(const String & file_name) const = 0;

    /// Return true if metadata exists in part.
    virtual bool exists(const String & file_name) const = 0;

    /// Delete all metadatas in part.
    /// If include_projection is true, also delete metadatas in projection parts.
    virtual void deleteAll(bool include_projection) = 0;

    /// Assert that all metadatas in part are deleted.
    /// If include_projection is true, also assert that all metadatas in projection parts are deleted.
    virtual void assertAllDeleted(bool include_projection) const = 0;

    /// Update all metadatas in part.
    /// If include_projection is true, also update metadatas in projection parts.
    virtual void updateAll(bool include_projection) = 0;

    /// Check all metadatas in part.
    virtual std::unordered_map<String, uint128> check() const = 0;

protected:
    const IMergeTreeDataPart * part;
};

using PartMetadataManagerPtr = std::shared_ptr<IPartMetadataManager>;
}
