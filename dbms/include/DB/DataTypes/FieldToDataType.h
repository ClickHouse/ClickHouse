#pragma once

#include <DB/Core/FieldVisitors.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeArray.h>
#include <DB/DataTypes/DataTypeNull.h>

#include <DB/Common/Exception.h>


namespace DB
{

/** Для заданного значения Field возвращает минимальный тип данных, позволяющий хранить значение этого типа.
  * В случае, если Field - массив, конвертирует все элементы к общему типу.
  */
class FieldToDataType : public StaticVisitor<DataTypePtr>
{
public:
	DataTypePtr operator() (Null 	& x) const
	{
		return std::make_shared<DataTypeNull>();
	}

	DataTypePtr operator() (UInt64 	& x) const
	{
		if (x <= std::numeric_limits<UInt8>::max())		return std::make_shared<DataTypeUInt8>();
		if (x <= std::numeric_limits<UInt16>::max())	return std::make_shared<DataTypeUInt16>();
		if (x <= std::numeric_limits<UInt32>::max())	return std::make_shared<DataTypeUInt32>();
		return std::make_shared<DataTypeUInt64>();
	}

	DataTypePtr operator() (Int64 	& x) const
	{
		if (x <= std::numeric_limits<Int8>::max() && x >= std::numeric_limits<Int8>::min())		return std::make_shared<DataTypeInt8>();
		if (x <= std::numeric_limits<Int16>::max() && x >= std::numeric_limits<Int16>::min())	return std::make_shared<DataTypeInt16>();
		if (x <= std::numeric_limits<Int32>::max() && x >= std::numeric_limits<Int32>::min())	return std::make_shared<DataTypeInt32>();
		return std::make_shared<DataTypeInt64>();
	}

	DataTypePtr operator() (Float64 & x) const
	{
		return std::make_shared<DataTypeFloat64>();
	}

	DataTypePtr operator() (String 	& x) const
	{
		return std::make_shared<DataTypeString>();
	}

	DataTypePtr operator() (Array 	& x) const;

	DataTypePtr operator() (Tuple 	& x) const;
};

}

