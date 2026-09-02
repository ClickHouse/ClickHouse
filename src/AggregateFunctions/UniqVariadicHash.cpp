#include <AggregateFunctions/UniqVariadicHash.h>
#include <DataTypes/DataTypeTuple.h>
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

}
