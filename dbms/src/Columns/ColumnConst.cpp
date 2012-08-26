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
	throw Exception("Materializing ColumnConstArray is not implemented.", ErrorCodes::NOT_IMPLEMENTED);
}

}
