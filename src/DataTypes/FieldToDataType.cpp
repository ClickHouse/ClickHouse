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
    if (x <= std::numeric_limits<Int64>::max()) return std::make_shared<DataTypeUInt64>(/*unsigned_can_be_signed=*/true);
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

    for (const Field & elem : x)
        element_types.emplace_back(applyVisitor(*this, elem));

    return std::make_shared<DataTypeArray>(getLeastSupertype<on_error>(element_types));
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const Tuple & tuple) const
{
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

    for (const auto & elem : map)
    {
        const auto & tuple = elem.safeGet<const Tuple &>();
        assert(tuple.size() == 2);
        key_types.push_back(applyVisitor(*this, tuple[0]));
        value_types.push_back(applyVisitor(*this, tuple[1]));
    }

    return std::make_shared<DataTypeMap>(
        getLeastSupertype<on_error>(key_types),
        getLeastSupertype<on_error>(value_types));
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const Object &) const
{
    return std::make_shared<DataTypeObject>(DataTypeObject::SchemaFormat::JSON);
}

template <LeastSupertypeOnError on_error>
DataTypePtr FieldToDataType<on_error>::operator() (const AggregateFunctionStateData & x) const
{
    return DataTypeFactory::instance().get(x.name);
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
template class FieldToDataType<LeastSupertypeOnError::Variant>;

}
