#include <QueryCoordination/Optimizer/PhysicalProperties.h>

namespace DB
{

bool PhysicalProperties::operator==(const PhysicalProperties & other) const
{
    bool sort_same = sort_description.size() == commonPrefix(sort_description, other.sort_description).size();
    bool distribution_same = distribution.type == other.distribution.type && distribution.keys == other.distribution.keys;
    return sort_same && distribution_same;
}

bool PhysicalProperties::satisfy(const PhysicalProperties & required) const
{
    if (required.distribution.type == DistributionType::Any)
        return true;

    return *this == required;
}

String PhysicalProperties::toString() const
{
    return "distribution type: " + distributionType(distribution.type);
}

}
