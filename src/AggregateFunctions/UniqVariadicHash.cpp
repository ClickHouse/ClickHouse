#include <AggregateFunctions/UniqVariadicHash.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>
#include <Common/assert_cast.h>
#include <Common/typeid_cast.h>


namespace DB
{
struct Settings;

/// If some arguments are not contiguous, we cannot use simple hash function,
///  because it requires method IColumn::getDataAt to work.
/// Note that we treat single tuple argument in the same way as multiple arguments.
bool isAllArgumentsContiguousInMemory(const DataTypes & argument_types)
{
    auto check_all_arguments_are_contiguous_in_memory = [](const DataTypes & types)
    {
        for (const auto & type : types)
            if (!type->isValueUnambiguouslyRepresentedInContiguousMemoryRegion())
                return false;
        return true;
    };

    const DataTypeTuple * single_argument_as_tuple = nullptr;
    if (argument_types.size() == 1)
        single_argument_as_tuple = typeid_cast<const DataTypeTuple *>(argument_types[0].get());

    if (single_argument_as_tuple)
        return check_all_arguments_are_contiguous_in_memory(single_argument_as_tuple->getElements());
    return check_all_arguments_are_contiguous_in_memory(argument_types);
}

namespace
{

/// The types of the columns that `withNormalizedFloatValue` and `rawFloatValueWidth` look into.
/// `IColumn::updateHashWithValue` normalizes negative zeros by itself in all other columns.
bool hasNegativeZeroNormalizedByUniqVariadicHash(const IDataType & type)
{
    switch (type.getTypeId())
    {
        case TypeIndex::Float64:
        case TypeIndex::Float32:
        case TypeIndex::BFloat16:
            return true;
        case TypeIndex::Array:
            return hasNegativeZeroNormalizedByUniqVariadicHash(*assert_cast<const DataTypeArray &>(type).getNestedType());
        case TypeIndex::Nullable:
            return hasNegativeZeroNormalizedByUniqVariadicHash(*assert_cast<const DataTypeNullable &>(type).getNestedType());
        case TypeIndex::LowCardinality:
            return hasNegativeZeroNormalizedByUniqVariadicHash(*assert_cast<const DataTypeLowCardinality &>(type).getDictionaryType());
        default:
            return false;
    }
}

}

bool isNegativeZeroNormalizationNeeded(const DataTypes & argument_types)
{
    const DataTypes * types = &argument_types;
    if (argument_types.size() == 1)
        if (const auto * single_argument_as_tuple = typeid_cast<const DataTypeTuple *>(argument_types[0].get()))
            types = &single_argument_as_tuple->getElements();

    for (const auto & type : *types)
        if (hasNegativeZeroNormalizedByUniqVariadicHash(*type))
            return true;
    return false;
}

}
