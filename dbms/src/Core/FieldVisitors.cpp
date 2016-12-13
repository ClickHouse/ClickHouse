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

String FieldVisitorDump::operator() (const Tuple 	& x_def) const
{
	auto & x = x_def.t;
	String res;
	WriteBufferFromString wb(res);
	FieldVisitorDump visitor;

	wb.write("Tuple_[", 7);
	for (auto it = x.begin(); it != x.end(); ++it)
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
	DoubleConverter<true>::BufferType buffer;
	double_conversion::StringBuilder builder{buffer, sizeof(buffer)};

	const auto result = DoubleConverter<true>::instance().ToShortest(x, &builder);

	if (!result)
		throw Exception("Cannot print float or double number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

	return { buffer, buffer + builder.position() };
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

String FieldVisitorToString::operator() (const Tuple 		& x_def) const
{
	auto & x = x_def.t;
	String res;
	WriteBufferFromString wb(res);
	FieldVisitorToString visitor;

	writeChar('(', wb);
	for (auto it = x.begin(); it != x.end(); ++it)
	{
		if (it != x.begin())
			wb.write(", ", 2);
		writeString(apply_visitor(visitor, *it), wb);
	}
	writeChar(')', wb);

	return res;
}


UInt64 stringToDateOrDateTime(const String & s)
{
	if (s.size() == strlen("YYYY-MM-DD"))
		return stringToDate(s);
	else
		return stringToDateTime(s);
}


DayNum_t stringToDate(const String & s)
{
	ReadBufferFromString in(s);
	DayNum_t date{};

	readDateText(date, in);
	if (!in.eof())
		throw Exception("String is too long for Date: " + s);

	return date;
}

UInt64 stringToDateTime(const String & s)
{
	ReadBufferFromString in(s);
	time_t date_time{};

	readDateTimeText(date_time, in);
	if (!in.eof())
		throw Exception("String is too long for DateTime: " + s);

	return UInt64(date_time);
}


}
