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

	void serializeText(const Field & field, WriteBuffer & ostr) const override
	{
		writeDateTimeText(get<UInt64>(field), ostr);
	}

	void deserializeText(Field & field, ReadBuffer & istr) const override
	{
		time_t x;
		readDateTimeText(x, istr);
		field = NearestFieldType<UInt32>::Type(x);
	}

	void serializeTextEscaped(const Field & field, WriteBuffer & ostr) const override
	{
		serializeText(field, ostr);
	}

	void deserializeTextEscaped(Field & field, ReadBuffer & istr) const override
	{
		deserializeText(field, istr);
	}

	void serializeTextQuoted(const Field & field, WriteBuffer & ostr) const override
	{
		writeChar('\'', ostr);
		serializeText(field, ostr);
		writeChar('\'', ostr);
	}

	void serializeTextJSON(const Field & field, WriteBuffer & ostr) const override
	{
		writeChar('"', ostr);
		serializeText(field, ostr);
		writeChar('"', ostr);
	}

	void deserializeTextQuoted(Field & field, ReadBuffer & istr) const override
	{
		assertString("'", istr);
		deserializeText(field, istr);
		assertString("'", istr);
	}
};

}
