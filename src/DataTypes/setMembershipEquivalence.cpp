#include <DataTypes/setMembershipEquivalence.h>

#include <Common/NaNUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeTuple.h>

namespace DB
{

bool hasFloat(const DataTypePtr & type)
{
    if (isFloat(removeNullable(removeLowCardinality(type))))
        return true;

    bool found = false;
    type->forEachChild([&](const IDataType & child)
    {
        if (WhichDataType(child).isFloat())
            found = true;
    });

    return found;
}

bool constantMayHoldFloatNaNOrZero(const Field & constant_value)
{
    switch (constant_value.getType())
    {
        case Field::Types::Float64:
        {
            const Float64 value = constant_value.safeGet<Float64>();
            return isNaN(value) || value == 0.0;
        }
        case Field::Types::UInt64:
            return constant_value.safeGet<UInt64>() == 0;
        case Field::Types::Int64:
            return constant_value.safeGet<Int64>() == 0;
        case Field::Types::Tuple:
        {
            for (const auto & element : constant_value.safeGet<Tuple>())
                if (constantMayHoldFloatNaNOrZero(element))
                    return true;
            return false;
        }
        case Field::Types::Array:
        {
            for (const auto & element : constant_value.safeGet<Array>())
                if (constantMayHoldFloatNaNOrZero(element))
                    return true;
            return false;
        }
        default:
            /// Not provably a non-zero number.
            return true;
    }
}

bool comparisonWithConstantMatchesSetMembership(const DataTypePtr & expression_type, const Field & constant_value)
{
    const auto type = removeNullable(removeLowCardinality(expression_type));

    if (isFloat(type))
        return !constantMayHoldFloatNaNOrZero(constant_value);

    /// Without a floating-point value under it, the two relations coincide.
    if (!hasFloat(type))
        return true;

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get()))
    {
        if (constant_value.getType() != Field::Types::Tuple)
            return false;

        const auto & elements = tuple_type->getElements();
        const auto & tuple = constant_value.safeGet<Tuple>();
        if (tuple.size() != elements.size())
            return false;

        for (size_t i = 0; i < elements.size(); ++i)
            if (!comparisonWithConstantMatchesSetMembership(elements[i], tuple[i]))
                return false;

        return true;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(type.get()))
    {
        if (constant_value.getType() != Field::Types::Array)
            return false;

        for (const auto & element : constant_value.safeGet<Array>())
            if (!comparisonWithConstantMatchesSetMembership(array_type->getNestedType(), element))
                return false;

        return true;
    }

    /// Any other carrier of floating-point values (`Map`, `Variant`, `Dynamic`, ...) is not analyzed.
    return false;
}

}
