#pragma once

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/IDataTypeNumberFixed.h>


namespace DB
{

class DataTypeDateTime final : public IDataTypeNumberFixed<UInt32, ColumnUInt32>
{
public:
	DataTypeDateTime() {}

	bool behavesAsNumber() const override { return false; }

	std::string getName() const override { return "DateTime"; }
	DataTypePtr clone() const override { return new DataTypeDateTime; }

	void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		writeDateTimeText(static_cast<const ColumnType &>(column).getData()[row_num], ostr);
	}

	static void deserializeText(IColumn & column, ReadBuffer & istr)
	{
		time_t x;
		readDateTimeText(x, istr);
		static_cast<ColumnType &>(column).getData().push_back(x);
	}

	void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		serializeText(column, row_num, ostr);
	}

	void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override
	{
		deserializeText(column, istr);
	}

	void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		writeChar('\'', ostr);
		serializeText(column, row_num, ostr);
		writeChar('\'', ostr);
	}

	void deserializeTextQuoted(IColumn & column, ReadBuffer & istr) const override
	{
		time_t x;
		assertChar('\'', istr);
		readDateTimeText(x, istr);
		assertChar('\'', istr);
		static_cast<ColumnType &>(column).getData().push_back(x);	/// Важно делать это в конце - для exception safety.
	}

	void serializeTextJSON(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		writeChar('"', ostr);
		serializeText(column, row_num, ostr);
		writeChar('"', ostr);
	}

	void deserializeTextJSON(IColumn & column, ReadBuffer & istr) const override
	{
		time_t x;
		assertChar('"', istr);
		readDateTimeText(x, istr);
		assertChar('"', istr);
		static_cast<ColumnType &>(column).getData().push_back(x);
	}

	void serializeTextCSV(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		writeChar('"', ostr);
		serializeText(column, row_num, ostr);
		writeChar('"', ostr);
	}

	void deserializeTextCSV(IColumn & column, ReadBuffer & istr, const char delimiter) const override
	{
		LocalDateTime value;
		readCSV(value, istr);
		static_cast<ColumnType &>(column).getData().push_back(static_cast<time_t>(value));
	}
};

}
