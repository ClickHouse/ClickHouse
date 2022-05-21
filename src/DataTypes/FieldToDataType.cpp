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
#include <DataTypes/getLeastSupertype.h>
#include <DataTypes/DataTypeFactory.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int EMPTY_DATA_PASSED;
}


DataTypePtr FieldToDataType::operator() (const Null &) const
{
    return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeNothing>());
}

DataTypePtr FieldToDataType::operator() (const UInt64 & x) const
{
    if (x <= std::numeric_limits<UInt8>::max()) return std::make_shared<DataTypeUInt8>();
    if (x <= std::numeric_limits<UInt16>::max()) return std::make_shared<DataTypeUInt16>();
    if (x <= std::numeric_limits<UInt32>::max()) return std::make_shared<DataTypeUInt32>();
    return std::make_shared<DataTypeUInt64>();
}

DataTypePtr FieldToDataType::operator() (const Int64 & x) const
{
    if (x <= std::numeric_limits<Int8>::max() && x >= std::numeric_limits<Int8>::min()) return std::make_shared<DataTypeInt8>();
    if (x <= std::numeric_limits<Int16>::max() && x >= std::numeric_limits<Int16>::min()) return std::make_shared<DataTypeInt16>();
    if (x <= std::numeric_limits<Int32>::max() && x >= std::numeric_limits<Int32>::min()) return std::make_shared<DataTypeInt32>();
    return std::make_shared<DataTypeInt64>();
}

DataTypePtr FieldToDataType::operator() (const Float64 &) const
{
    return std::make_shared<DataTypeFloat64>();
}

DataTypePtr FieldToDataType::operator() (const UInt128 &) const
{
    return std::make_shared<DataTypeUInt128>();
}

DataTypePtr FieldToDataType::operator() (const Int128 &) const
{
    return std::make_shared<DataTypeInt128>();
}

DataTypePtr FieldToDataType::operator() (const UInt256 &) const
{
    return std::make_shared<DataTypeUInt256>();
}

DataTypePtr FieldToDataType::operator() (const Int256 &) const
{
    return std::make_shared<DataTypeInt256>();
}

DataTypePtr FieldToDataType::operator() (const UUID &) const
{
    return std::make_shared<DataTypeUUID>();
}

DataTypePtr FieldToDataType::operator() (const String &) const
{
    return std::make_shared<DataTypeString>();
}

DataTypePtr FieldToDataType::operator() (const DecimalField<Decimal32> & x) const
{
    using Type = DataTypeDecimal<Decimal32>;
    return std::make_shared<Type>(Type::maxPrecision(), x.getScale());
}

DataTypePtr FieldToDataType::operator() (const DecimalField<Decimal64> & x) const
{
    using Type = DataTypeDecimal<Decimal64>;
    return std::make_shared<Type>(Type::maxPrecision(), x.getScale());
}

DataTypePtr FieldToDataType::operator() (const DecimalField<Decimal128> & x) const
{
    using Type = DataTypeDecimal<Decimal128>;
    return std::make_shared<Type>(Type::maxPrecision(), x.getScale());
}

DataTypePtr FieldToDataType::operator() (const DecimalField<Decimal256> & x) const
{
    using Type = DataTypeDecimal<Decimal256>;
    return std::make_shared<Type>(Type::maxPrecision(), x.getScale());
}

DataTypePtr FieldToDataType::operator() (const Array & x) const
{
    DataTypes element_types;
    element_types.reserve(x.size());

    for (const Field & elem : x)
        element_types.emplace_back(applyVisitor(FieldToDataType(allow_convertion_to_string), elem));

    return std::make_shared<DataTypeArray>(getLeastSupertype(element_types, allow_convertion_to_string));
}

DataTypePtr FieldToDataType::operator() (const Tuple & tuple) const
{
    if (tuple.empty())
        throw Exception("Cannot infer type of an empty tuple", ErrorCodes::EMPTY_DATA_PASSED);

    DataTypes element_types;
    element_types.reserve(tuple.size());

    for (const auto & element : tuple)
        element_types.push_back(applyVisitor(FieldToDataType(allow_convertion_to_string), element));

    return std::make_shared<DataTypeTuple>(element_types);
}

DataTypePtr FieldToDataType::operator() (const Map & map) const
{
    DataTypes key_types;
    DataTypes value_types;
    key_types.reserve(map.size());
    value_types.reserve(map.size());

    for (const auto & elem : map)
    {
        const auto & tuple = elem.safeGet<const Tuple &>();
        assert(tuple.size() == 2);
        key_types.push_back(applyVisitor(FieldToDataType(allow_convertion_to_string), tuple[0]));
        value_types.push_back(applyVisitor(FieldToDataType(allow_convertion_to_string), tuple[1]));
    }

    return std::make_shared<DataTypeMap>(
        getLeastSupertype(key_types, allow_convertion_to_string),
        getLeastSupertype(value_types, allow_convertion_to_string));
}

DataTypePtr FieldToDataType::operator() (const Object &) const
{
    /// TODO: Do we need different parameters for type Object?
    return std::make_shared<DataTypeObject>("json", false);
}

DataTypePtr FieldToDataType::operator() (const AggregateFunctionStateData & x) const
{
    const auto & name = static_cast<const AggregateFunctionStateData &>(x).name;
    return DataTypeFactory::instance().get(name);
}

DataTypePtr FieldToDataType::operator()(const bool &) const
{
    return DataTypeFactory::instance().get("Bool");
}

}
