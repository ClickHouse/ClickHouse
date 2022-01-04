#pragma once

#include "config_core.h"

#if USE_ROCKSDB
#include <city.h>
#include <Core/Types.h>


namespace DB
{

class SeekableReadBuffer;
class IMergeTreeDataPart;

class MergeTreeMetadataCache;
using MergeTreeMetadataCachePtr = std::shared_ptr<MergeTreeMetadataCache>;

class IDisk;
using DiskPtr = std::shared_ptr<IDisk>;

class PartMetadataCache
{
public:
    using uint128 = CityHash_v1_0_2::uint128;

    PartMetadataCache(
        const MergeTreeMetadataCachePtr & cache_,
        const String & disk_name_,
        const String & relative_data_path_,
        const String & relative_path_,
        const IMergeTreeDataPart * parent_part_)
        : cache(cache_)
        , disk_name(disk_name_)
        , relative_data_path(relative_data_path_)
        , relative_path(relative_path_)
        , parent_part(parent_part_)
    {
    }

    std::unique_ptr<SeekableReadBuffer>
    readOrSet(const DiskPtr & disk, const String & file_name, String & value);
    void batchSet(const DiskPtr & disk, const Strings & file_names);
    void batchDelete(const Strings & file_names);
    void set(const String & file_name, const String & value);
    void getFilesAndCheckSums(Strings & files, std::vector<uint128> & checksums) const;

private:
    String getFullRelativePath() const;
    String getKey(const String & file_path) const;

    MergeTreeMetadataCachePtr cache;
    const String & disk_name;
    const String & relative_data_path; /// Relative path of table to disk
    const String & relative_path; /// Relative path of part to table
    const IMergeTreeDataPart * parent_part;
};

using PartMetadataCachePtr = std::shared_ptr<PartMetadataCache>;

}
#endif
