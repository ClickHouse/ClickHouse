#include <DB/DataTypes/FieldToDataType.h>

#include <DB/Columns/ColumnString.h>
#include <DB/Columns/ColumnConst.h>


namespace DB
{

template <> ColumnPtr ColumnConst<String>::convertToFullColumn() const
{
	ColumnString * res = new ColumnString;
	ColumnString::Offsets_t & offsets = res->getOffsets();
	ColumnUInt8::Container_t & vec = dynamic_cast<ColumnVector<UInt8> &>(res->getData()).getData();

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

	return res;
}


template <> ColumnPtr ColumnConst<Array>::convertToFullColumn() const
{
	size_t array_size = data.size();
	ColumnPtr nested_column = data.empty()
		? new ColumnUInt8	/// Для пустого массива непонятно, какого типа будет материализованный столбец. Пусть будет UInt8.
		: boost::apply_visitor(FieldToDataType(), data.at(0))->createColumn();

	nested_column->reserve(s * array_size, s * array_size * 32);

	ColumnArray * res = new ColumnArray(nested_column);
	ColumnArray::Offsets_t & offsets = res->getOffsets();

	offsets.resize(s);
	for (size_t i = 0; i < s; ++i)
	{
		offsets[i] = (i + 1) * array_size;
		for (size_t j = 0; j < array_size; ++j)
			nested_column->insert(data[j]);
	}

	return res;
}

}
