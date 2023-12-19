#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Processors/QueryPlan/IQueryPlanStep.h>
#include <Common/SipHash.h>

namespace DB
{

struct Distribution
{
    enum Type : int8_t
    {
        Any = 1,
        Singleton = 2,
        Replicated = 3,
        Hashed = 4,
    };

    String toString() const;

    Type type;
    Names keys; /// keys for Hashed
    bool distributed_by_bucket_num = false;
};

struct SortProp
{
    String toString() const;

    SortDescription sort_description = {};
    DataStream::SortScope sort_scope = DataStream::SortScope::None;
};

class PhysicalProperties
{
public:
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
    bool satisfySorting(const PhysicalProperties & required) const;
    bool satisfyDistribution(const PhysicalProperties & required) const;

    String toString() const;

    Distribution distribution;
    SortProp sort_prop;
};

String toString(std::vector<PhysicalProperties> properties_list);

}
