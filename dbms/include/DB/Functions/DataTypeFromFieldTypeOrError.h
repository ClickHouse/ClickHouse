#pragma once

#include <DB/DataTypes/DataTypesNumberFixed.h>

namespace DB
{

template <typename T>
struct DataTypeFromFieldTypeOrError
{
	static DataTypePtr getDataType()
	{
		return new typename DataTypeFromFieldType<T>::Type;
	}
};

template <>
struct DataTypeFromFieldTypeOrError<NumberTraits::Error>
{
	static DataTypePtr getDataType()
	{
		return nullptr;
	}
};

}
