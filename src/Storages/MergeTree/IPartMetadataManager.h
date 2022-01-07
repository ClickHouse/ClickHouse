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


class IPartMetadataManager
{
public:
    using uint128 = CityHash_v1_0_2::uint128;

    explicit IPartMetadataManager(const IMergeTreeDataPart * part_);

    virtual ~IPartMetadataManager() = default;

    virtual std::unique_ptr<SeekableReadBuffer> read(const String & file_name) const = 0;

    virtual bool exists(const String & file_name) const = 0;

    virtual void deleteAll(bool include_projection) = 0;

    virtual void assertAllDeleted(bool include_projection) const = 0;

    virtual void updateAll(bool include_projection) = 0;

    virtual std::unordered_map<String, uint128> check() const = 0;

protected:
    const IMergeTreeDataPart * part;
    const DiskPtr disk;
};

using PartMetadataManagerPtr = std::shared_ptr<IPartMetadataManager>;
}
