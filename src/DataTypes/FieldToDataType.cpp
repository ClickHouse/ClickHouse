#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypesDecimal.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeFactory.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int EMPTY_DATA_PASSED;
    extern const int NOT_IMPLEMENTED;
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const Null &) const
{
    return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const UInt64 & x) const
{
    if (x <= std::numeric_limits<UInt8>::max()) return std::make_shared<DataTypeUInt8>();
    if (x <= std::numeric_limits<UInt16>::max()) return std::make_shared<DataTypeUInt16>();
    if (x <= std::numeric_limits<UInt32>::max()) return std::make_shared<DataTypeUInt32>();
    return std::make_shared<DataTypeUInt64>();
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const Int64 & x) const
{
    if (x <= std::numeric_limits<Int8>::max() && x >= std::numeric_limits<Int8>::min()) return std::make_shared<DataTypeInt8>();
    if (x <= std::numeric_limits<Int16>::max() && x >= std::numeric_limits<Int16>::min()) return std::make_shared<DataTypeInt16>();
    if (x <= std::numeric_limits<Int32>::max() && x >= std::numeric_limits<Int32>::min()) return std::make_shared<DataTypeInt32>();
    return std::make_shared<DataTypeInt64>();
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const Float64 &) const
{
    return std::make_shared<DataTypeFloat64>();
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const UInt128 &) const
{
    return std::make_shared<DataTypeUInt128>();
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const Int128 &) const
{
    return std::make_shared<DataTypeInt128>();
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const UInt256 &) const
{
    return std::make_shared<DataTypeUInt256>();
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const Int256 &) const
{
    return std::make_shared<DataTypeInt256>();
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const UUID &) const
{
    return std::make_shared<DataTypeUUID>();
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const IPv4 &) const
{
    return std::make_shared<DataTypeIPv4>();
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const IPv6 &) const
{
    return std::make_shared<DataTypeIPv6>();
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const String &) const
{
    return std::make_shared<DataTypeString>();
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const DecimalField<Decimal32> & x) const
{
    using Type = DataTypeDecimal<Decimal32>;
    return std::make_shared<Type>(Type::maxPrecision(), x.getScale());
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const DecimalField<Decimal64> & x) const
{
    using Type = DataTypeDecimal<Decimal64>;
    return std::make_shared<Type>(Type::maxPrecision(), x.getScale());
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const DecimalField<Decimal128> & x) const
{
    using Type = DataTypeDecimal<Decimal128>;
    return std::make_shared<Type>(Type::maxPrecision(), x.getScale());
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const DecimalField<Decimal256> & x) const
{
    using Type = DataTypeDecimal<Decimal256>;
    return std::make_shared<Type>(Type::maxPrecision(), x.getScale());
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const Array & x) const
{
    DataTypes element_types;
    element_types.reserve(x.size());

    auto checkIfConversionSigned = [&](bool& has_signed_int, bool& has_uint64, bool& uint64_could_opposite,
                                       const DataTypePtr & type, const Field & elem)
    {
        if (uint64_could_opposite)
        {
            has_signed_int |= WhichDataType(type).isNativeInt();

            if (type->getTypeId() == TypeIndex::UInt64)
            {
                has_uint64 = true;
                uint64_could_opposite &= (elem.template get<UInt64>() <= std::numeric_limits<Int64>::max());
            }
        }
    };

    bool has_signed_int = false;
    bool has_uint64 = false;
    bool uint64_could_opposite = true;
    for (const Field & elem : x)
    {
        DataTypePtr type = applyVisitor(*this, elem);
        element_types.emplace_back(type);
        checkIfConversionSigned(has_signed_int, has_uint64, uint64_could_opposite, type, elem);
    }

    // Convert the UInt64 type to Int64 in order to cover other signed_integer types
    // and obtain the least super type of all ints.
    if (has_signed_int && has_uint64 && uint64_could_opposite)
    {
        for (auto & type : element_types)
            if (type->getTypeId() == TypeIndex::UInt64)
                type = std::make_shared<DataTypeInt64>();
    }

    return std::make_shared<DataTypeArray>(getLeastSupertype<on_error>(element_types));
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const Tuple & tuple) const
{
    if (tuple.empty())
        throw Exception(ErrorCodes::EMPTY_DATA_PASSED, "Cannot infer type of an empty tuple");

    DataTypes element_types;
    element_types.reserve(tuple.size());

    for (const auto & element : tuple)
        element_types.push_back(applyVisitor(*this, element));

    return std::make_shared<DataTypeTuple>(element_types);
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const Map & map) const
{
    DataTypes key_types;
    DataTypes value_types;
    key_types.reserve(map.size());
    value_types.reserve(map.size());

    auto checkIfConversionSigned = [&](bool& has_signed_int, bool& has_uint64, bool& uint64_could_opposite,
                                       const DataTypePtr & type, const Field & elem)
    {
        if (uint64_could_opposite)
        {
            has_signed_int |= WhichDataType(type).isNativeInt();

            if (type->getTypeId() == TypeIndex::UInt64)
            {
                has_uint64 = true;
                uint64_could_opposite &= (elem.template get<UInt64>() <= std::numeric_limits<Int64>::max());
            }
        }
    };

    auto updateUInt64Types = [](DataTypes types)
    {
        for (auto& type : types)
            if (type->getTypeId() == TypeIndex::UInt64)
                type = std::make_shared<DataTypeInt64>();
    };

    bool k_has_signed_int = false;
    bool k_has_uint64 = false;
    bool k_uint64_could_opposite = true;
    bool v_has_signed_int = false;
    bool v_has_uint64 = false;
    bool v_uint64_could_opposite = true;
    for (const auto & elem : map)
    {
        const auto & tuple = elem.safeGet<const Tuple &>();
        assert(tuple.size() == 2);
        DataTypePtr k_type = applyVisitor(*this, tuple[0]);
        key_types.push_back(k_type);
        checkIfConversionSigned(k_has_signed_int, k_has_uint64, k_uint64_could_opposite, k_type, tuple[0]);
        DataTypePtr v_type = applyVisitor(*this, tuple[1]);
        value_types.push_back(v_type);
        checkIfConversionSigned(v_has_signed_int, v_has_uint64, v_uint64_could_opposite, v_type, tuple[1]);
    }

    // Convert the UInt64 type to Int64 in order to cover other signed_integer types
    // and obtain the least super type of all ints.
    if (k_has_signed_int && k_has_uint64 && k_uint64_could_opposite)
        updateUInt64Types(key_types);

    if (v_has_signed_int && v_has_uint64 && v_uint64_could_opposite)
        updateUInt64Types(value_types);

    return std::make_shared<DataTypeMap>(
        getLeastSupertype<on_error>(key_types),
        getLeastSupertype<on_error>(value_types));
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const Object &) const
{
    /// TODO: Do we need different parameters for type Object?
    return std::make_shared<DataTypeObject>("json", false);
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const AggregateFunctionStateData & x) const
{
    const auto & name = static_cast<const AggregateFunctionStateData &>(x).name;
    return DataTypeFactory::instance().get(name);
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const CustomType &) const
{
    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Not implemented");
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator()(const bool &) const
{
    return DataTypeFactory::instance().get("Bool");
}

template class FieldToDataType<LeastSupertypeOnError::Throw>;
template class FieldToDataType<LeastSupertypeOnError::String>;
template class FieldToDataType<LeastSupertypeOnError::Null>;

}
