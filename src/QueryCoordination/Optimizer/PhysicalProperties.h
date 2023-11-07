#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <QueryCoordination/Optimizer/SortProp.h>
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

    static String distributionType(DistributionType type)
    {
        switch (type)
        {
            case DistributionType::Any:
                return "Any";
            case DistributionType::Singleton:
                return "Singleton";
            case DistributionType::Replicated:
                return "Replicated";
            case DistributionType::Hashed:
                return "Hashed";
        }
    }

    struct Distribution
    {
        DistributionType type;
        Names keys; /// keys for Hashed

        bool distribution_by_buket_num = false;
    };

    bool operator==(const PhysicalProperties & other) const;

    struct HashFunction
    {
        size_t operator()(const PhysicalProperties & properties) const
        {
            SipHash hash;
            hash.update(int8_t(properties.distribution.type));
            for (const auto & key : properties.distribution.keys)
                hash.update(key);

            for (const auto & sort : properties.sort_prop.sort_description)
                hash.update(sort.dump());
            return hash.get64();
        }
    };

    bool satisfy(const PhysicalProperties & required) const;
    bool satisfySort(const PhysicalProperties & required) const;
    bool satisfyDistribute(const PhysicalProperties & required) const;

    String toString() const;

    Distribution distribution;
    SortProp sort_prop;
};

}
