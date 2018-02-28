#include <Common/FieldVisitors.h>
#include <DataTypes/FieldToDataType.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/convertFieldToType.h>
#include <Common/Exception.h>
#include <ext/size.h>


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
    if (x <= std::numeric_limits<UInt8>::max())  return std::make_shared<DataTypeUInt8>();
    if (x <= std::numeric_limits<UInt16>::max()) return std::make_shared<DataTypeUInt16>();
    if (x <= std::numeric_limits<UInt32>::max()) return std::make_shared<DataTypeUInt32>();
    return std::make_shared<DataTypeUInt64>();
}

DataTypePtr FieldToDataType::operator() (const Int64 & x) const
{
    if (x <= std::numeric_limits<Int8>::max() && x >= std::numeric_limits<Int8>::min())   return std::make_shared<DataTypeInt8>();
    if (x <= std::numeric_limits<Int16>::max() && x >= std::numeric_limits<Int16>::min()) return std::make_shared<DataTypeInt16>();
    if (x <= std::numeric_limits<Int32>::max() && x >= std::numeric_limits<Int32>::min()) return std::make_shared<DataTypeInt32>();
    return std::make_shared<DataTypeInt64>();
}

DataTypePtr FieldToDataType::operator() (const Float64 &) const
{
    return std::make_shared<DataTypeFloat64>();
}

DataTypePtr FieldToDataType::operator() (const String &) const
{
    return std::make_shared<DataTypeString>();
}


DataTypePtr FieldToDataType::operator() (const Array & x) const
{
    DataTypes element_types;
    element_types.reserve(x.size());

    for (const Field & elem : x)
        element_types.emplace_back(applyVisitor(FieldToDataType(), elem));

    return std::make_shared<DataTypeArray>(getLeastSupertype(element_types));
}


DataTypePtr FieldToDataType::operator() (const Tuple & x) const
{
    auto & tuple = static_cast<const TupleBackend &>(x);
    if (tuple.empty())
        throw Exception("Cannot infer type of an empty tuple", ErrorCodes::EMPTY_DATA_PASSED);

    DataTypes element_types;
    element_types.reserve(ext::size(tuple));

    for (const auto & element : tuple)
        element_types.push_back(applyVisitor(FieldToDataType(), element));

    return std::make_shared<DataTypeTuple>(element_types);
}


}
