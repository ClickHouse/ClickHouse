#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/DataTypeDate.h>


namespace DB
{

void DataTypeDate::serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	writeDateText(DayNum_t(static_cast<const ColumnUInt16 &>(column).getData()[row_num]), ostr);
}

static void deserializeText(IColumn & column, ReadBuffer & istr)
{
	DayNum_t x;
	readDateText(x, istr);
	static_cast<ColumnUInt16 &>(column).getData().push_back(x);
}

void DataTypeDate::serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	serializeText(column, row_num, ostr);
}

void DataTypeDate::deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const
{
	deserializeText(column, istr);
}

void DataTypeDate::serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	writeChar('\'', ostr);
	serializeText(column, row_num, ostr);
	writeChar('\'', ostr);
}

void DataTypeDate::deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const
{
	DayNum_t x;
	assertChar('\'', istr);
	readDateText(x, istr);
	assertChar('\'', istr);
	static_cast<ColumnUInt16 &>(column).getData().push_back(x);	/// Важно делать это в конце - для exception safety.
}

void DataTypeDate::serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr, bool) const
{
	writeChar('"', ostr);
	serializeText(column, row_num, ostr);
	writeChar('"', ostr);
}

void DataTypeDate::deserializeTextJSON(IColumn & column, ReadBuffer & istr) const
{
	DayNum_t x;
	assertChar('"', istr);
	readDateText(x, istr);
	assertChar('"', istr);
	static_cast<ColumnUInt16 &>(column).getData().push_back(x);
}

void DataTypeDate::serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const
{
	writeChar('"', ostr);
	serializeText(column, row_num, ostr);
	writeChar('"', ostr);
}

void DataTypeDate::deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const
{
	LocalDate value;
	readCSV(value, istr);
	static_cast<ColumnUInt16 &>(column).getData().push_back(value.getDayNum());
}

}
