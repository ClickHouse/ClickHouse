#include <DB/Core/FieldVisitors.h>


namespace DB
{


String FieldVisitorDump::operator() (const String 	& x) const
{
	String res;
	WriteBufferFromString wb(res);
	writeQuoted(x, wb);
	return res;
}

String FieldVisitorDump::operator() (const Array 	& x) const
{
	String res;
	WriteBufferFromString wb(res);
	FieldVisitorDump visitor;

	wb.write("Array_[", 7);
	for (Array::const_iterator it = x.begin(); it != x.end(); ++it)
	{
		if (it != x.begin())
			wb.write(", ", 2);
		writeString(apply_visitor(visitor, *it), wb);
	}
	writeChar(']', wb);

	return res;
}


String FieldVisitorToString::formatFloat(const Float64 x)
{
	char tmp[25];
	double_conversion::StringBuilder builder{tmp, sizeof(tmp)};

	const auto result = getDoubleToStringConverter().ToShortest(x, &builder);

	if (!result)
		throw Exception("Cannot print float or double number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

	return { tmp, tmp + builder.position() };
}

String FieldVisitorToString::operator() (const Array 		& x) const
{
	String res;
	WriteBufferFromString wb(res);
	FieldVisitorToString visitor;

	writeChar('[', wb);
	for (Array::const_iterator it = x.begin(); it != x.end(); ++it)
	{
		if (it != x.begin())
			wb.write(", ", 2);
		writeString(apply_visitor(visitor, *it), wb);
	}
	writeChar(']', wb);

	return res;
}


UInt64 stringToDateOrDateTime(const String & s)
{
	ReadBufferFromString in(s);

	if (s.size() == strlen("YYYY-MM-DD"))
	{
		DayNum_t date{};
		readDateText(date, in);
		return UInt64(date);
	}
	else
	{
		time_t date_time{};
		readDateTimeText(date_time, in);
		if (!in.eof())
			throw Exception("String is too long for DateTime: " + s);
		return UInt64(date_time);
	}
}


}
