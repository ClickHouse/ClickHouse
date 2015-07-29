#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeArray.h>


namespace DB
{

/// Для заданного значения Field возвращает минимальный тип данных, позволяющий хранить значение этого типа.
class FieldToDataType : public StaticVisitor<DataTypePtr>
{
public:
	DataTypePtr operator() (const Null 		& x) const
	{
		throw Exception("NULL literals is not implemented yet", ErrorCodes::NOT_IMPLEMENTED);
	}
	
	DataTypePtr operator() (const UInt64 	& x) const
	{
		if (x <= std::numeric_limits<UInt8>::max())		return new DataTypeUInt8;
		if (x <= std::numeric_limits<UInt16>::max())	return new DataTypeUInt16;
		if (x <= std::numeric_limits<UInt32>::max())	return new DataTypeUInt32;
		return new DataTypeUInt64;
	}
	
	DataTypePtr operator() (const Int64 	& x) const
	{
		if (x <= std::numeric_limits<Int8>::max() && x >= std::numeric_limits<Int8>::min())		return new DataTypeInt8;
		if (x <= std::numeric_limits<Int16>::max() && x >= std::numeric_limits<Int16>::min())	return new DataTypeInt16;
		if (x <= std::numeric_limits<Int32>::max() && x >= std::numeric_limits<Int32>::min())	return new DataTypeInt32;
		return new DataTypeInt64;
	}
	
	DataTypePtr operator() (const Float64 	& x) const
	{
		return new DataTypeFloat64;
	}
	
	DataTypePtr operator() (const String 	& x) const
	{
		return new DataTypeString;
	}

	DataTypePtr operator() (const Array 	& x) const
	{
		return new DataTypeArray(apply_visitor(FieldToDataType(), x.at(0)));
	}
};


}

