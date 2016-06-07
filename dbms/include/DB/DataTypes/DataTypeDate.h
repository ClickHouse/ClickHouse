#pragma once

#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/IDataTypeNumberFixed.h>


namespace DB
{

class DataTypeDate final : public IDataTypeNumberFixed<UInt16, ColumnUInt16>
{
public:
	DataTypeDate() {}

	bool behavesAsNumber() const override { return false; }

	std::string getName() const override { return "Date"; }
	DataTypePtr clone() const override { return new DataTypeDate; }

	void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		writeDateText(DayNum_t(static_cast<const ColumnType &>(column).getData()[row_num]), ostr);
	}

	static void deserializeText(IColumn & column, ReadBuffer & istr)
	{
		DayNum_t x;
		readDateText(x, istr);
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
		DayNum_t x;
		assertChar('\'', istr);
		readDateText(x, istr);
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
		DayNum_t x;
		assertChar('"', istr);
		readDateText(x, istr);
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
		LocalDate value;
		readCSV(value, istr);
		static_cast<ColumnType &>(column).getData().push_back(value.getDayNum());
	}
};

}
