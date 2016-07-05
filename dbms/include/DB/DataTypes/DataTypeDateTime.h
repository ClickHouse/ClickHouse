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
	DataTypePtr clone() const override { return std::make_shared<DataTypeDateTime>(); }

private:
	void serializeTextImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const NullValuesByteMap * null_map) const override
	{
		if (isNullValue(null_map, row_num))
			writeCString(NullSymbol::Plain::name, ostr);
		else
			writeDateTimeText(static_cast<const ColumnType &>(column).getData()[row_num], ostr);
	}

	void serializeTextEscapedImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const NullValuesByteMap * null_map) const override
	{
		if (isNullValue(null_map, row_num))
			writeCString(NullSymbol::Escaped::name, ostr);
		else
			serializeTextImpl(column, row_num, ostr, null_map);
	}

	void deserializeTextEscapedImpl(IColumn & column, ReadBuffer & istr,
		NullValuesByteMap * null_map) const override
	{
		if (NullSymbol::Deserializer<NullSymbol::Escaped>::execute(column, istr, null_map))
		{
			FieldType default_val = get<FieldType>(getDefault());
			static_cast<ColumnType &>(column).getData().push_back(default_val);
		}
		else
		{
			time_t x;
			readDateTimeText(x, istr);
			static_cast<ColumnType &>(column).getData().push_back(x);
		}
	}

	void serializeTextQuotedImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const NullValuesByteMap * null_map) const override
	{
		if (isNullValue(null_map, row_num))
			writeCString(NullSymbol::Quoted::name, ostr);
		else
		{
			writeChar('\'', ostr);
			serializeTextImpl(column, row_num, ostr, null_map);
			writeChar('\'', ostr);
		}
	}

	void deserializeTextQuotedImpl(IColumn & column, ReadBuffer & istr,
		NullValuesByteMap * null_map) const override
	{
		if (NullSymbol::Deserializer<NullSymbol::Quoted>::execute(column, istr, null_map))
		{
			FieldType default_val = get<FieldType>(getDefault());
			static_cast<ColumnType &>(column).getData().push_back(default_val);
		}
		else
		{
			time_t x;
			assertChar('\'', istr);
			readDateTimeText(x, istr);
			assertChar('\'', istr);
			static_cast<ColumnType &>(column).getData().push_back(x);	/// Важно делать это в конце - для exception safety.
		}
	}

	void serializeTextJSONImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const NullValuesByteMap * null_map) const override
	{
		if (isNullValue(null_map, row_num))
			writeCString(NullSymbol::JSON::name, ostr);
		else
		{
			writeChar('"', ostr);
			serializeTextImpl(column, row_num, ostr, null_map);
			writeChar('"', ostr);
		}
	}

	void deserializeTextJSONImpl(IColumn & column, ReadBuffer & istr,
		NullValuesByteMap * null_map) const override
	{
		if (NullSymbol::Deserializer<NullSymbol::JSON>::execute(column, istr, null_map))
		{
			FieldType default_val = get<FieldType>(getDefault());
			static_cast<ColumnType &>(column).getData().push_back(default_val);
		}
		else
		{
			time_t x;
			assertChar('"', istr);
			readDateTimeText(x, istr);
			assertChar('"', istr);
			static_cast<ColumnType &>(column).getData().push_back(x);
		}
	}

	void serializeTextCSVImpl(const IColumn & column, size_t row_num, WriteBuffer & ostr,
		const NullValuesByteMap * null_map) const override
	{
		if (isNullValue(null_map, row_num))
			writeCString(NullSymbol::CSV::name, ostr);
		else
		{
			writeChar('"', ostr);
			serializeTextImpl(column, row_num, ostr, null_map);
			writeChar('"', ostr);
		}
	}

	void deserializeTextCSVImpl(IColumn & column, ReadBuffer & istr, const char delimiter,
		NullValuesByteMap * null_map) const override
	{
		if (NullSymbol::Deserializer<NullSymbol::CSV>::execute(column, istr, null_map))
		{
			FieldType default_val = get<FieldType>(getDefault());
			static_cast<ColumnType &>(column).getData().push_back(default_val);
		}
		else
		{
			LocalDateTime value;
			readCSV(value, istr);
			static_cast<ColumnType &>(column).getData().push_back(static_cast<time_t>(value));
		}
	}
};

}
