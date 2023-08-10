#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Common/SipHash.h>

namespace DB
{

class PhysicalProperties
{
public:
    enum DistributionType : int8_t
    {
        Any = 1,
        Singleton = 2,
        Replicated = 3,
        Hashed = 4,
    };


    struct Distribution
    {
        DistributionType type;
        Names keys; /// keys for partition
    };

    bool operator==(const PhysicalProperties & other) const
    {
        bool sort_same = sort_description.size() == commonPrefix(sort_description, other.sort_description).size();
        bool distribution_same = distribution.type == other.distribution.type && distribution.keys == other.distribution.keys;
        return sort_same && distribution_same;
    }

    struct HashFunction
    {
        size_t operator()(const PhysicalProperties & properties) const
        {
            SipHash hash;
            hash.update(int8_t(properties.distribution.type));
            for (auto key : properties.distribution.keys)
            {
                hash.update(key);
            }

            for (auto sort : properties.sort_description)
            {
                hash.update(sort.dump());
            }
            return hash.get64();
        }
    };

    Distribution distribution;

    SortDescription sort_description;

};

}
