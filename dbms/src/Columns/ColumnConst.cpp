#include <DB/DataTypes/DataTypeString.h>
#include <DB/DataTypes/DataTypeFixedString.h>
#include <DB/DataTypes/DataTypeArray.h>

#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnConst.h>
#include <DB/Columns/ColumnArray.h>
#include <DB/Columns/ColumnTuple.h>
#include <DB/Columns/ColumnFixedString.h>
#include <DB/DataTypes/DataTypeTuple.h>
#include <ext/enumerate.hpp>


namespace DB
{


template <> ColumnPtr ColumnConst<String>::convertToFullColumn() const
{
	if (!data_type || typeid_cast<const DataTypeString *>(&*data_type))
	{
		ColumnString * res = new ColumnString;
		ColumnPtr res_ptr = res;
		ColumnString::Offsets_t & offsets = res->getOffsets();
		ColumnString::Chars_t & vec = res->getChars();

		size_t string_size = data.size() + 1;
		size_t offset = 0;
		offsets.resize(s);
		vec.resize(s * string_size);

		for (size_t i = 0; i < s; ++i)
		{
			memcpy(&vec[offset], data.data(), string_size);
			offset += string_size;
			offsets[i] = offset;
		}

		return res_ptr;
	}
	else if (const DataTypeFixedString * type = typeid_cast<const DataTypeFixedString *>(&*data_type))
	{
		size_t n = type->getN();

		if (data.size() > n)
			throw Exception("Too long value for " + type->getName(), ErrorCodes::TOO_LARGE_STRING_SIZE);

		ColumnFixedString * res = new ColumnFixedString(n);
		ColumnPtr res_ptr = res;
		ColumnFixedString::Chars_t & vec = res->getChars();

		vec.resize_fill(n * s);
		size_t offset = 0;

		for (size_t i = 0; i < s; ++i)
		{
			memcpy(&vec[offset], data.data(), data.size());
			offset += n;
		}

		return res_ptr;
	}
	else
		throw Exception("Invalid data type in ColumnConstString: " + data_type->getName(), ErrorCodes::LOGICAL_ERROR);
}


ColumnPtr ColumnConst<Array>::convertToFullColumn() const
{
	if (!data_type)
		throw Exception("No data type specified for ColumnConstArray", ErrorCodes::LOGICAL_ERROR);

	const DataTypeArray * type = typeid_cast<const DataTypeArray *>(&*data_type);
	if (!type)
		throw Exception("Non-array data type specified for ColumnConstArray", ErrorCodes::LOGICAL_ERROR);

	const Array & array = getDataFromHolderImpl();
	size_t array_size = array.size();
	ColumnPtr nested_column = type->getNestedType()->createColumn();

	ColumnArray * res = new ColumnArray(nested_column);
	ColumnArray::Offsets_t & offsets = res->getOffsets();

	offsets.resize(s);
	for (size_t i = 0; i < s; ++i)
	{
		offsets[i] = (i + 1) * array_size;
		for (size_t j = 0; j < array_size; ++j)
			nested_column->insert(array[j]);
	}

	return res;
}


StringRef ColumnConst<Array>::getDataAt(size_t n) const
{
	throw Exception("Method getDataAt is not supported for " + this->getName(), ErrorCodes::NOT_IMPLEMENTED);
}

UInt64 ColumnConst<Array>::get64(size_t n) const
{
	throw Exception("Method get64 is not supported for " + this->getName(), ErrorCodes::NOT_IMPLEMENTED);
}

StringRef ColumnConst<Array>::getDataAtWithTerminatingZero(size_t n) const
{
	throw Exception("Method getDataAt is not supported for " + this->getName(), ErrorCodes::NOT_IMPLEMENTED);
}


ColumnPtr ColumnConst<Tuple>::convertToFullColumn() const
{
	if (!data_type)
		throw Exception("No data type specified for ColumnConstTuple", ErrorCodes::LOGICAL_ERROR);

	const DataTypeTuple * type = typeid_cast<const DataTypeTuple *>(&*data_type);
	if (!type)
		throw Exception("Non-Tuple data type specified for ColumnConstTuple", ErrorCodes::LOGICAL_ERROR);

	/// Ask data_type to create ColumnTuple of corresponding type
	ColumnPtr res = type->createColumn();
	static_cast<ColumnTuple &>(*res).insert(getDataFromHolderImpl());

	return res;
}

void ColumnConst<Tuple>::getExtremes(Field & min, Field & max) const
{
	min = data_type->getDefault();
	max = data_type->getDefault();
}

StringRef ColumnConst<Tuple>::getDataAt(size_t n) const
{
	throw Exception("Method getDataAt is not supported for " + this->getName(), ErrorCodes::NOT_IMPLEMENTED);
}

UInt64 ColumnConst<Tuple>::get64(size_t n) const
{
	throw Exception("Method get64 is not supported for " + this->getName(), ErrorCodes::NOT_IMPLEMENTED);
}

StringRef ColumnConst<Tuple>::getDataAtWithTerminatingZero(size_t n) const
{
	throw Exception("Method getDataAt is not supported for " + this->getName(), ErrorCodes::NOT_IMPLEMENTED);
}


}
