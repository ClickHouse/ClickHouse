#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/DataTypeDateTime.h>


namespace DB
{

void DataTypeDateTime::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	writeDateTimeText(static_cast<const ColumnUInt32 &>(column).getData()[row_num], ostr);
}

static void deserializeText(IColumn & column, ReadBuffer & istr)
{
	time_t x;
	readDateTimeText(x, istr);
	static_cast<ColumnUInt32 &>(column).getData().push_back(x);
}

void DataTypeDateTime::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	serializeText(column, row_num, ostr);
}

void DataTypeDateTime::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
	deserializeText(column, istr);
}

void DataTypeDateTime::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	writeChar('\'', ostr);
	serializeText(column, row_num, ostr);
	writeChar('\'', ostr);
}

void DataTypeDateTime::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
	time_t x;
	assertChar('\'', istr);
	readDateTimeText(x, istr);
	assertChar('\'', istr);
	static_cast<ColumnUInt32 &>(column).getData().push_back(x);	/// Важно делать это в конце - для exception safety.
}

void DataTypeDateTime::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, bool) const
{
	writeChar('"', ostr);
	serializeText(column, row_num, ostr);
	writeChar('"', ostr);
}

void DataTypeDateTime::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
	time_t x;
	assertChar('"', istr);
	readDateTimeText(x, istr);
	assertChar('"', istr);
	static_cast<ColumnUInt32 &>(column).getData().push_back(x);
}

void DataTypeDateTime::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	writeChar('"', ostr);
	serializeText(column, row_num, ostr);
	writeChar('"', ostr);
}

void DataTypeDateTime::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const
{
	LocalDateTime value;
	readCSV(value, istr);
	static_cast<ColumnUInt32 &>(column).getData().push_back(static_cast<time_t>(value));
}

}
