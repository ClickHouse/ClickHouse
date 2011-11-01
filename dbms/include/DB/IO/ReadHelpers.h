#pragma once

#include <cstring>
#include <limits>
#include <algorithm>

#include <tr1/type_traits>

#include <Yandex/DateLUT.h>

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/VarInt.h>

#define DEFAULT_MAX_STRING_SIZE 0x00FFFFFFULL


namespace DB
{

/// Функции-помошники для форматированного чтения

static inline char parseEscapeSequence(char c)
{
	switch(c)
	{
		case 'b':
			return '\b';
		case 'f':
			return '\f';
		case 'n':
			return '\n';
		case 'r':
			return '\r';
		case 't':
			return '\t';
		case '0':
			return '\0';
		default:
			return c;
	}
}

static inline void throwReadAfterEOF()
{
	throw Exception("Attempt to read after eof", ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
}


/// Чтение числа в native формате
template <typename T>
inline void readBinary(T & x, ReadBuffer & buf)
{
	buf.readStrict(reinterpret_cast<char *>(&x), sizeof(x));
}

template <typename T>
inline void readIntBinary(T & x, ReadBuffer & buf)
{
	readBinary(x, buf);
}

template <typename T>
inline void readFloatBinary(T & x, ReadBuffer & buf)
{
	readBinary(x, buf);
}


inline void readStringBinary(std::string & s, DB::ReadBuffer & buf, size_t MAX_STRING_SIZE = DEFAULT_MAX_STRING_SIZE)
{
	size_t size = 0;
	DB::readVarUInt(size, buf);

	if (size > MAX_STRING_SIZE)
		throw Poco::Exception("Too large string size.");

	s.resize(size);
	buf.readStrict(const_cast<char *>(s.data()), size);
}


inline void readChar(char & x, ReadBuffer & buf)
{
	if (!buf.eof())
	{
		x = *buf.position();
		++buf.position();
	}
	else
		throwReadAfterEOF();
}

void assertString(const char * s, ReadBuffer & buf);

/// грубо
template <typename T>
void readIntText(T & x, ReadBuffer & buf)
{
	bool negative = false;
	x = 0;
	if (buf.eof())
		throwReadAfterEOF();
	
	while (!buf.eof())
	{
		switch (*buf.position())
		{
			case '+':
				break;
			case '-':
			    if (std::tr1::is_signed<T>::value)
					negative = true;
				else
					return;
				break;
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
				x *= 10;
				x += *buf.position() - '0';
				break;
			default:
				if (negative)
					x = -x;
				return;
		}
		++buf.position();
	}
	if (negative)
		x = -x;
}

/// грубо
template <typename T>
void readFloatText(T & x, ReadBuffer & buf)
{
	bool negative = false;
	x = 0;
	bool after_point = false;
	double power_of_ten = 1;

	if (buf.eof())
		throwReadAfterEOF();

	while (!buf.eof())
	{
		switch (*buf.position())
		{
			case '+':
				break;
			case '-':
				negative = true;
				break;
			case '.':
				after_point = true;
				break;
			case '0':
			case '1':
			case '2':
			case '3':
			case '4':
			case '5':
			case '6':
			case '7':
			case '8':
			case '9':
				if (after_point)
				{
					power_of_ten /= 10;
					x += (*buf.position() - '0') * power_of_ten;
				}
				else
				{
					x *= 10;
					x += *buf.position() - '0';
				}
				break;
			case 'e':
			case 'E':
			{
				++buf.position();
				Int32 exponent = 0;
				readIntText(exponent, buf);
				if (exponent == 0)
				{
					if (negative)
						x = -x;
					return;
				}
				else if (exponent > 0)
				{
					for (Int32 i = 0; i < exponent; ++i)
						x *= 10;
					if (negative)
						x = -x;
					return;
				}
				else
				{
					for (Int32 i = 0; i < exponent; ++i)
						x /= 10;
					if (negative)
						x = -x;
					return;
				}
			}
			case 'i':
				++buf.position();
				assertString("nf", buf);
				x = std::numeric_limits<T>::infinity();
				if (negative)
					x = -x;
				return;
			case 'I':
				++buf.position();
				assertString("NF", buf);
				x = std::numeric_limits<T>::infinity();
				if (negative)
					x = -x;
				return;
			case 'n':
				++buf.position();
				assertString("an", buf);
				x = std::numeric_limits<T>::quiet_NaN();
				return;
			case 'N':
				++buf.position();
				assertString("AN", buf);
				x = std::numeric_limits<T>::quiet_NaN();
				return;
			default:
				if (negative)
					x = -x;
				return;
		}
		++buf.position();
	}
	if (negative)
		x = -x;
}

template <typename T>
void readText(T & x, ReadBuffer & buf);

template <> inline void readText<UInt8>		(UInt8 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
template <> inline void readText<UInt16>	(UInt16 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
template <> inline void readText<UInt32>	(UInt32 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
template <> inline void readText<UInt64>	(UInt64 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
template <> inline void readText<Int8>		(Int8 & x, 		ReadBuffer & buf) { readIntText(x, buf); }
template <> inline void readText<Int16>		(Int16 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
template <> inline void readText<Int32>		(Int32 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
template <> inline void readText<Int64>		(Int64 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
template <> inline void readText<Float32>	(Float32 & x, 	ReadBuffer & buf) { readFloatText(x, buf); }
template <> inline void readText<Float64>	(Float64 & x, 	ReadBuffer & buf) { readFloatText(x, buf); }


/// грубо; всё до '\n' или '\t'
void readString(String & s, ReadBuffer & buf);

void readEscapedString(String & s, ReadBuffer & buf);

void readQuotedString(String & s, ReadBuffer & buf);

void readDoubleQuotedString(String & s, ReadBuffer & buf);

void readBackQuotedString(String & s, ReadBuffer & buf);


/// в формате YYYY-MM-DD
inline void readDateText(Yandex::DayNum_t & date, ReadBuffer & buf)
{
	char s[10];
	size_t size = buf.read(s, 10);
	if (10 != size)
	{
		s[size] = 0;
		throw Exception(std::string("Cannot parse date ") + s, ErrorCodes::CANNOT_PARSE_DATE);
	}

	UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
	UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
	UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');

	date = Yandex::DateLUTSingleton::instance().makeDayNum(year, month, day);
}


/// в формате YYYY-MM-DD HH:MM:SS, согласно текущему часовому поясу
inline void readDateTimeText(time_t & datetime, ReadBuffer & buf)
{
	char s[19];
	size_t size = buf.read(s, 19);
	if (19 != size)
	{
		s[size] = 0;
		throw Exception(std::string("Cannot parse datetime ") + s, ErrorCodes::CANNOT_PARSE_DATETIME);
	}

	UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
	UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
	UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');

	UInt8 hour = (s[11] - '0') * 10 + (s[12] - '0');
	UInt8 minute = (s[14] - '0') * 10 + (s[15] - '0');
	UInt8 second = (s[17] - '0') * 10 + (s[18] - '0');

	datetime = Yandex::DateLUTSingleton::instance().makeDateTime(year, month, day, hour, minute, second);
}


/// Пропустить пробельные символы.
inline void skipWhitespaceIfAny(ReadBuffer & buf)
{
	while (!buf.eof()
			&& (*buf.position() == ' '
			|| *buf.position() == '\t'
			|| *buf.position() == '\n'
			|| *buf.position() == '\r'
			|| *buf.position() == '\f'))
		++buf.position();
}

}
