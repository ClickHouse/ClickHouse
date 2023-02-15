#pragma once

#include <unordered_map>
#include <vector>
#include <Storages/MergeTree/MergeTreePartInfo.h>

namespace DB
{
class DeleteBuffer : public std::unordered_map<MergeTreePartInfo, std::vector<std::string>, MergeTreePartInfoHash>
{
public:
    void insertKeysByInfo(const MergeTreePartInfo & info, const std::vector<std::string> & keys)
    {
        (*this)[info].insert((*this)[info].end(), keys.begin(), keys.end());
    }
};

}
