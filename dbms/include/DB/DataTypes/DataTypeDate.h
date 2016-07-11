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
	DataTypePtr clone() const override { return std::make_shared<DataTypeDate>(); }

private:
	void serializeText(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		writeDateText(DayNum_t(static_cast<const ColumnType &>(column).getData()[row_num]), ostr);
	}

	void serializeTextEscaped(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		writeDateText(DayNum_t(static_cast<const ColumnType &>(column).getData()[row_num]), ostr);
	}

	void deserializeTextEscaped(IColumn & column, ReadBuffer & istr) const override
	{
		DayNum_t x;
		readDateText(x, istr);
		static_cast<ColumnType &>(column).getData().push_back(x);
	}

	void serializeTextQuoted(const IColumn & column, size_t row_num, WriteBuffer & ostr) const override
	{
		writeChar('\'', ostr);
		writeDateText(DayNum_t(static_cast<const ColumnType &>(column).getData()[row_num]), ostr);
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

	void serializeTextJSONImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const NullValuesByteMap * null_map) const override
	{
		if (isNullValue(null_map, row_num))
			writeCString(NullSymbol::JSON::name, ostr);
		else
		{
			writeChar('"', ostr);
			writeDateText(DayNum_t(static_cast<const ColumnType &>(column).getData()[row_num]), ostr);
			writeChar('"', ostr);
		}
	}

	void deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr, NullValuesByteMap * null_map) const override
	{
		if (NullSymbol::Deserializer<NullSymbol::JSON>::execute(column, istr, null_map))
		{
			FieldType default_val = get<FieldType>(getDefault());
			static_cast<ColumnType &>(column).getData().push_back(default_val);
		}
		else
		{
			DayNum_t x;
			assertChar('"', istr);
			readDateText(x, istr);
			assertChar('"', istr);
			static_cast<ColumnType &>(column).getData().push_back(x);
		}
	}

	void serializeTextCSVImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr, const NullValuesByteMap * null_map) const override
	{
		if (isNullValue(null_map, row_num))
			writeCString(NullSymbol::CSV::name, ostr);
		else
		{
			writeChar('"', ostr);
			writeDateText(DayNum_t(static_cast<const ColumnType &>(column).getData()[row_num]), ostr);
			writeChar('"', ostr);
		}
	}

	void deserializeTextCSVImpl(IColumn & column, ReadBuffer & istr, const char delimiter, NullValuesByteMap * null_map) const override
	{
		if (NullSymbol::Deserializer<NullSymbol::CSV>::execute(column, istr, null_map))
		{
			FieldType default_val = get<FieldType>(getDefault());
			static_cast<ColumnType &>(column).getData().push_back(default_val);
		}
		else
		{
			LocalDate value;
			readCSV(value, istr);
			static_cast<ColumnType &>(column).getData().push_back(value.getDayNum());
		}
	}
};

}
