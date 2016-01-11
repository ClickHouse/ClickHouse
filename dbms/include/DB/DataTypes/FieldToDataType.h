#pragma once

#include <DB/Core/FieldVisitors.h>

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeArray.h>

#include <DB/Common/Exception.h>
#include <DB/Core/ErrorCodes.h>


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
		throw Exception("NULL literals are not implemented yet", ErrorCodes::NOT_IMPLEMENTED);
	}

	DataTypePtr operator() (UInt64 	& x) const
	{
		if (x <= std::numeric_limits<UInt8>::max())		return new DataTypeUInt8;
		if (x <= std::numeric_limits<UInt16>::max())	return new DataTypeUInt16;
		if (x <= std::numeric_limits<UInt32>::max())	return new DataTypeUInt32;
		return new DataTypeUInt64;
	}

	DataTypePtr operator() (Int64 	& x) const
	{
		if (x <= std::numeric_limits<Int8>::max() && x >= std::numeric_limits<Int8>::min())		return new DataTypeInt8;
		if (x <= std::numeric_limits<Int16>::max() && x >= std::numeric_limits<Int16>::min())	return new DataTypeInt16;
		if (x <= std::numeric_limits<Int32>::max() && x >= std::numeric_limits<Int32>::min())	return new DataTypeInt32;
		return new DataTypeInt64;
	}

	DataTypePtr operator() (Float64 & x) const
	{
		return new DataTypeFloat64;
	}

	DataTypePtr operator() (String 	& x) const
	{
		return new DataTypeString;
	}

	DataTypePtr operator() (Array 	& x) const;

	DataTypePtr operator() (Tuple 	& x) const;
};

}

