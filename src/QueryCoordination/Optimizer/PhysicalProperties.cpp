#include <QueryCoordination/Optimizer/PhysicalProperties.h>

namespace DB
{

String SortProp::toString() const
{
    String res;
    for (const auto & sort_column : sort_description)
        res += sort_column.column_name + ", ";

    switch (sort_scope)
    {
        case DataStream::SortScope::None:
            res += "None";
            break;
        case DataStream::SortScope::Stream:
            res += "Stream";
            break;
        case DataStream::SortScope::Global:
            res += "Global";
            break;
        case DataStream::SortScope::Chunk:
            res += "Chunk";
            break;
    }

    return res;
}

String Distribution::toString() const
{
    switch (this->type)
    {
        case Any:
            return "Any";
        case Singleton:
            return "Singleton";
        case Replicated:
            return "Replicated";
        case Hashed:
            return "Hashed";
    }
}

bool PhysicalProperties::operator==(const PhysicalProperties & other) const
{
    if (sort_prop.sort_description.size() != other.sort_prop.sort_description.size())
        return false;

    if (sort_prop.sort_description.size() != commonPrefix(sort_prop.sort_description, other.sort_prop.sort_description).size())
        return false;

    if (sort_prop.sort_scope != other.sort_prop.sort_scope)
        return false;

    if (other.distribution.keys.size() != distribution.keys.size())
        return false;

    if (other.distribution.distributed_by_bucket_num != distribution.distributed_by_bucket_num)
        return false;

    for (const auto & key : distribution.keys)
        if (std::count(other.distribution.keys.begin(), other.distribution.keys.end(), key) != 1)
            return false;

    return distribution.type == other.distribution.type;
}

bool PhysicalProperties::satisfy(const PhysicalProperties & required) const
{
    bool satisfy_sort = satisfySorting(required);
    bool satisfy_distribute = satisfyDistribution(required);

    return satisfy_sort && satisfy_distribute;
}

bool PhysicalProperties::satisfySorting(const PhysicalProperties & required) const
{
    bool sort_description_satisfy = required.sort_prop.sort_description.size()
        == commonPrefix(sort_prop.sort_description, required.sort_prop.sort_description).size();

    if (!sort_description_satisfy)
        return false;

    bool sort_scope_satisfy = sort_prop.sort_scope >= required.sort_prop.sort_scope;

    if (!sort_scope_satisfy)
        return false;

    return true;
}

bool PhysicalProperties::satisfyDistribution(const PhysicalProperties & required) const
{
    if (required.distribution.type == Distribution::Any)
        return true;

    if (required.distribution.distributed_by_bucket_num != distribution.distributed_by_bucket_num)
        return false;

    for (const auto & key : distribution.keys)
        if (std::count(required.distribution.keys.begin(), required.distribution.keys.end(), key) != 1)
            return false;

    return distribution.type == required.distribution.type;
}

String PhysicalProperties::toString() const
{
    return distribution.toString();
}

}
