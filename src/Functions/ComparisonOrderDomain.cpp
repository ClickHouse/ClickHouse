#include <Functions/ComparisonOrderDomain.h>

#include <DataTypes/IDataType.h>

namespace DB
{

bool ComparisonOrderDomain::operator==(const ComparisonOrderDomain & other) const
{
    if (kind != other.kind || scale != other.scale)
        return false;
    if (kind != Kind::ExactType)
        return true;
    return exact_type && other.exact_type && exact_type->equals(*other.exact_type);
}

}
