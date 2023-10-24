#include <QueryCoordination/Optimizer/PhysicalProperties.h>

namespace DB
{

bool PhysicalProperties::operator==(const PhysicalProperties & other) const
{
    if (sort_description.size() != other.sort_description.size())
        return false;

    if (sort_description.size() != commonPrefix(sort_description, other.sort_description).size())
        return false;

    if (other.distribution.keys.size() != distribution.keys.size())
        return false;

    if (other.distribution.distribution_by_buket_num != distribution.distribution_by_buket_num)
        return false;

    for (const auto & key : distribution.keys)
    {
        if (std::count(other.distribution.keys.begin(), other.distribution.keys.end(), key) != 1)
            return false;
    }

    return distribution.type == other.distribution.type;
}

bool PhysicalProperties::satisfy(const PhysicalProperties & required) const
{
    bool sort_satisfy = required.sort_description.size() == commonPrefix(sort_description, required.sort_description).size();

    if (!sort_satisfy)
        return false;

    if (required.distribution.type == DistributionType::Any)
        return true;

    if (required.distribution.distribution_by_buket_num != distribution.distribution_by_buket_num)
        return false;

    for (const auto & key : distribution.keys)
    {
        if (std::count(required.distribution.keys.begin(), required.distribution.keys.end(), key) != 1)
            return false;
    }

    return distribution.type == required.distribution.type;
}

String PhysicalProperties::toString() const
{
    return distributionType(distribution.type);
}

}
