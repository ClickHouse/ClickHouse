#include <DataTypes/Utils.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeTuple.h>

namespace DB
{

bool canBeSafelyCast(const DataTypePtr & from_type, const DataTypePtr & to_type)
{
    auto from_which_type = WhichDataType(from_type->getTypeId());
    bool to_type_was_nullable = isNullableOrLowCardinalityNullable(to_type);
    auto to_type_unwrapped = removeNullable(removeLowCardinality(to_type));

    if (from_type->equals(*to_type_unwrapped))
        return true;

    auto to_which_type = WhichDataType(to_type_unwrapped->getTypeId());

    switch (from_which_type.idx)
    {
        case TypeIndex::UInt8:
        case TypeIndex::UInt16:
        case TypeIndex::UInt32:
        case TypeIndex::UInt64:
        case TypeIndex::UInt128:
        case TypeIndex::UInt256:
        {
            if (to_which_type.isUInt() &&
                to_type_unwrapped->getSizeOfValueInMemory() >= from_type->getSizeOfValueInMemory())
                return true;

            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::Int8:
        case TypeIndex::Int16:
        case TypeIndex::Int32:
        case TypeIndex::Int64:
        case TypeIndex::Int128:
        case TypeIndex::Int256:
        {
            if (to_which_type.isInt() &&
                to_type_unwrapped->getSizeOfValueInMemory() >= from_type->getSizeOfValueInMemory())
                return true;

            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::BFloat16:
        {
            if (to_which_type.isFloat32() || to_which_type.isFloat64() || to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::Float32:
        {
            if (to_which_type.isFloat64() || to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::Float64:
        case TypeIndex::Date:
        case TypeIndex::Date32:
        case TypeIndex::DateTime:
        case TypeIndex::DateTime64:
        case TypeIndex::Time:
        case TypeIndex::Time64:
        case TypeIndex::FixedString:
        case TypeIndex::Enum8:
        case TypeIndex::Enum16:
        case TypeIndex::IPv6:
        {
            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::Decimal32:
        case TypeIndex::Decimal64:
        case TypeIndex::Decimal128:
        case TypeIndex::Decimal256:
        {
            if (to_which_type.isDecimal())
            {
                auto from_type_decimal_precision = getDecimalPrecision(*from_type);
                auto to_type_decimal_precision = getDecimalPrecision(*to_type_unwrapped);
                if (from_type_decimal_precision > to_type_decimal_precision)
                    return false;

                auto from_type_decimal_scale = getDecimalScale(*from_type);
                auto to_type_decimal_scale = getDecimalScale(*to_type_unwrapped);
                if (from_type_decimal_scale > to_type_decimal_scale)
                    return false;

                return true;
            }

            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::UUID:
        {
            if (to_which_type.isUInt128() || to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::IPv4:
        {
            if (to_which_type.isUInt32() || to_which_type.isUInt64() || to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::Nullable:
        {
            if (to_type_was_nullable)
            {
                const auto & from_type_nullable = assert_cast<const DataTypeNullable &>(*from_type);
                return canBeSafelyCast(from_type_nullable.getNestedType(), to_type_unwrapped);
            }

            return false;
        }
        case TypeIndex::LowCardinality:
        {
            /// The target keeps its nullability here, because a Nullable dictionary type needs a target
            /// that can hold a NULL. Stripping only LowCardinality leaves the unwrapped target the same.
            const auto & from_type_low_cardinality = assert_cast<const DataTypeLowCardinality &>(*from_type);
            return canBeSafelyCast(from_type_low_cardinality.getDictionaryType(), removeLowCardinality(to_type));
        }
        case TypeIndex::Array:
        {
            if (to_which_type.isArray())
            {
                const auto & from_type_array = assert_cast<const DataTypeArray &>(*from_type);
                const auto & to_type_array = assert_cast<const DataTypeArray &>(*to_type_unwrapped);
                return canBeSafelyCast(from_type_array.getNestedType(), to_type_array.getNestedType());
            }

            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::Map:
        {
            if (to_which_type.isMap())
            {
                const auto & from_type_map = assert_cast<const DataTypeMap &>(*from_type);
                const auto & to_type_map = assert_cast<const DataTypeMap &>(*to_type_unwrapped);
                if (!canBeSafelyCast(from_type_map.getKeyType(), to_type_map.getKeyType()))
                    return false;

                if (!canBeSafelyCast(from_type_map.getValueType(), to_type_map.getValueType()))
                    return false;

                return true;
            }

            if (to_which_type.isArray())
            {
                // Map nested type is Array(Tuple(key_type, value_type))
                const auto & from_type_map = assert_cast<const DataTypeMap &>(*from_type);
                const auto & to_type_array = assert_cast<const DataTypeArray &>(*to_type_unwrapped);
                const auto * to_type_nested_tuple_type = typeid_cast<const DataTypeTuple *>(to_type_array.getNestedType().get());
                if (!to_type_nested_tuple_type)
                    return false;

                const auto & to_type_tuple_elements = to_type_nested_tuple_type->getElements();
                if (to_type_tuple_elements.size() != 2)
                    return false;

                if (!canBeSafelyCast(from_type_map.getKeyType(), to_type_tuple_elements[0]))
                    return false;

                if (!canBeSafelyCast(from_type_map.getValueType(), to_type_tuple_elements[1]))
                    return false;

                return true;
            }

            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::Tuple:
        {
            if (to_which_type.isTuple())
            {
                const auto & from_type_tuple = assert_cast<const DataTypeTuple &>(*from_type);
                const auto & to_type_tuple = assert_cast<const DataTypeTuple &>(*to_type_unwrapped);

                const auto & from_tuple_type_elements = from_type_tuple.getElements();
                const auto & to_tuple_type_elements = to_type_tuple.getElements();

                size_t lhs_type_elements_size = from_tuple_type_elements.size();
                if (lhs_type_elements_size != to_tuple_type_elements.size())
                    return false;

                for (size_t i = 0; i < lhs_type_elements_size; ++i)
                    if (!canBeSafelyCast(from_tuple_type_elements[i], to_tuple_type_elements[i]))
                        return false;

                return true;
            }

            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::QBit:
            return to_which_type.isQBit();
        case TypeIndex::Object:
        {
            if (to_which_type.isString())
                return true;

            return false;
        }
        case TypeIndex::Variant:
        case TypeIndex::Dynamic:
            /// Both encode a NULL via NULL_DISCRIMINATOR, so only a target that can hold one is safe.
            return to_type_was_nullable && to_which_type.isString();
        case TypeIndex::String:
        case TypeIndex::Set:
        case TypeIndex::Interval:
        case TypeIndex::Function:
        case TypeIndex::AggregateFunction:
        case TypeIndex::Nothing:
        case TypeIndex::JSONPaths:
            return false;
    }

    return true;
}

bool conversionPreservesOrder(const IDataType & from, const IDataType & to)
{
    if (from.equals(to))
        return true;

    const WhichDataType which_from(from);
    const WhichDataType which_to(to);

    /// An `Enum` is `static_cast` to the target's field type, so the order survives only when that
    /// mapping is the identity: the target must agree on the values AND be wide enough not to
    /// truncate, which `contains` does not check. An unmatched `to` falls through to the unwrapping.
    if (const auto * from_enum = dynamic_cast<const IDataTypeEnum *>(&from))
    {
        if (const auto * to_enum = dynamic_cast<const IDataTypeEnum *>(&to))
        {
            if (from.getSizeOfValueInMemory() <= to.getSizeOfValueInMemory() && to_enum->contains(*from_enum))
                return true;
        }
        else if (which_to.isInt() && from.getSizeOfValueInMemory() <= to.getSizeOfValueInMemory())
            return true;
    }

    /// Widening an integer keeps the order when the signedness is preserved or the target is
    /// signed, mirroring `ToNumberMonotonicity`'s expansion branch. An equal width can flip the
    /// sign bit and a narrowing wraps, so both stay refused. `isInteger` covers the wide types as
    /// well: `getLeastSupertype` derives `Int128`/`UInt128`/`Int256`/`UInt256` for an ordinary
    /// column-list-less `Merge` over mixed integer widths, and those casts are just as injective.
    if (which_from.isInteger() && which_to.isInteger()
        && from.getSizeOfValueInMemory() < to.getSizeOfValueInMemory()
        && (from.isValueRepresentedByUnsignedInteger() == to.isValueRepresentedByUnsignedInteger()
            || !to.isValueRepresentedByUnsignedInteger()))
        return true;

    /// Exact widenings: every source value is representable in the target and the mapping is strictly
    /// monotonic, so both the order and the distinctness survive.
    ///   - `Float32` to `Float64`: every `Float32` is a `Float64`.
    ///   - `Date` to `Date32`: the same day number in a wider integer.
    ///   - `DateTime` to `DateTime64`: the seconds are multiplied by `10^scale`; the largest `DateTime`
    ///     (2106) at the largest scale (9) is about 4.3e18 and fits an `Int64`.
    ///   - `Decimal(P1, S1)` to `Decimal(P2, S2)` with `S2 >= S1` and `P2 - S2 >= P1 - S1`: the value is
    ///     multiplied by `10^(S2 - S1)` and the integer part is not narrowed, so nothing overflows.
    ///     A smaller target scale rounds, which collapses distinct values.
    ///   - `FixedString(N)` to `String`: the cast trims the trailing zero bytes, which is still strictly
    ///     monotonic on a fixed length. Two distinct fixed strings differ at some byte; the one holding
    ///     the smaller byte there is the smaller value, and trimming only removes zero bytes - the
    ///     minimum - from the end, so it can neither change that first difference nor make the trimmed
    ///     value the prefix of the other unless it was already the smaller one.
    if (which_from.isFloat32() && which_to.isFloat64())
        return true;
    if (which_from.isDate() && which_to.isDate32())
        return true;
    if (which_from.isDateTime() && which_to.isDateTime64())
        return true;
    if (which_from.isDecimal() && which_to.isDecimal())
        return getDecimalScale(from) <= getDecimalScale(to)
            && getDecimalPrecision(from) - getDecimalScale(from) <= getDecimalPrecision(to) - getDecimalScale(to);
    if (which_from.isFixedString() && which_to.isString())
        return true;

    /// `ColumnLowCardinality::compareAt` compares through the dictionary, so a `LowCardinality`
    /// column orders exactly like its nested type. The wrapper is therefore stripped from either
    /// side; it never nests, so the stripped side is not `LowCardinality` again.
    const auto * from_lc = typeid_cast<const DataTypeLowCardinality *>(&from);
    const auto * to_lc = typeid_cast<const DataTypeLowCardinality *>(&to);
    if (from_lc || to_lc)
        return conversionPreservesOrder(
            from_lc ? *from_lc->getDictionaryType() : from, to_lc ? *to_lc->getDictionaryType() : to);

    /// Keeping or adding nullability moves no value: no NULL appears and every non-NULL keeps its
    /// place, so only the nested pair matters. Removing it falls through, because a nullable value
    /// then has to become a concrete one and NULL placement changes.
    if (const auto * to_nullable = typeid_cast<const DataTypeNullable *>(&to))
    {
        const auto * from_nullable = typeid_cast<const DataTypeNullable *>(&from);
        return conversionPreservesOrder(from_nullable ? *from_nullable->getNestedType() : from, *to_nullable->getNestedType());
    }

    /// `ColumnArray::compareAt` compares elementwise then by length, so a strictly monotonic element
    /// conversion orders arrays the same way. Both sides must be `Array`: wrapping or unwrapping one
    /// changes what is compared. `Tuple` and `Map` need their own analysis and stay refused.
    const auto * from_array = typeid_cast<const DataTypeArray *>(&from);
    const auto * to_array = typeid_cast<const DataTypeArray *>(&to);
    if (from_array && to_array)
        return conversionPreservesOrder(*from_array->getNestedType(), *to_array->getNestedType());

    return false;
}

}
