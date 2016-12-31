#include <DB/DataTypes/DataTypeNull.h>

namespace DB
{

void DataTypeNull::serializeBinary(const IColumn & column, WriteBuffer & ostr, size_t offset, size_t limit) const
{
	size_t size = column.size();

	if ((limit == 0) || ((offset + limit) > size))
		limit = size - offset;

	UInt8 x = 1;
	for (size_t i = 0; i < limit; ++i)
		writeBinary(x, ostr);
}

void DataTypeNull::deserializeBinary(IColumn & column, ReadBuffer & istr, size_t limit, double avg_value_size_hint) const
{
	ColumnNull & null_col = static_cast<ColumnNull &>(column);

	istr.ignore(sizeof(UInt8) * limit);
	null_col.insertRangeFrom(ColumnNull{0, Null()}, 0, limit);
}

}
