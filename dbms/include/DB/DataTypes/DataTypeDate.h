#ifndef DBMS_DATA_TYPES_NUMBER_FIXED_H
#define DBMS_DATA_TYPES_NUMBER_FIXED_H

#include <Poco/DateTimeParser.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/Columns/ColumnsNumber.h>
#include <DB/DataTypes/IDataTypeNumberFixed.h>


namespace DB
{

class DataTypeDate : public IDataTypeNumberFixed<UInt16, ColumnUInt64>
{
private:
	DateLUTSingleton & date_lut;

public:
	DataTypeDate() : date_lut(DateLUTSingleton::instance()) {}

	std::string getName() const { return "Date"; }

	void serializeText(const Field & field, std::ostream & ostr) const
	{
		DateLUT::Values & values = date_lut.getValues(boost::get<UInt16>(field));
		ostr << values.year << '-' << values.month << '-' << values.day_of_month;
	}
	
	void deserializeText(Field & field, std::istream & istr) const
	{
		std::string s;
		istr >> s;

		int time_zone_diff = 0;
		field = date_lut.toDayNum(Poco::DateTimeParser::parse(
			s, time_zone_diff).timestamp().epochTime());
	}

	void serializeTextEscaped(const Field & field, std::ostream & ostr) const
	{
		serializeText(field, ostr);
	}
	
	void deserializeTextEscaped(Field & field, std::istream & istr) const
	{
		deserializeText(field, istr);
	}
	
	void serializeTextQuoted(const Field & field, std::ostream & ostr, bool compatible = false) const
	{
		ostr << '\'';
		serializeText(field, ostr);
		ostr << '\'';
	}
	
	void deserializeTextQuoted(Field & field, std::istream & istr, bool compatible = false) const
	{
		char delim = istr.peek();
		if (delim == '\'' || delim == '"')
			istr.ignore();

		deserializeText(field, istr);

		if (istr.get() != delim)
			throw Exception("Delimiter in string literal doesn't match",
				ErrorCodes::DELIMITER_IN_STRING_LITERAL_DOESNT_MATCH);
	}
};

}

#endif
