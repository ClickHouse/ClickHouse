#pragma once

#include <cstring>
#include <limits>
#include <algorithm>

#include <type_traits>

#include <Yandex/Common.h>
#include <Yandex/DateLUT.h>

#include <mysqlxx/Date.h>
#include <mysqlxx/DateTime.h>

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/VarInt.h>
#include <city.h>

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


/// Эти функции находятся в VarInt.h
/// inline void throwReadAfterEOF()
/// inline void readChar(char & x, ReadBuffer & buf)


/// Чтение POD-типа в native формате
template <typename T>
inline void readPODBinary(T & x, ReadBuffer & buf)
{
	buf.readStrict(reinterpret_cast<char *>(&x), sizeof(x));
}

template <typename T>
inline void readIntBinary(T & x, ReadBuffer & buf)
{
	readPODBinary(x, buf);
}

template <typename T>
inline void readFloatBinary(T & x, ReadBuffer & buf)
{
	readPODBinary(x, buf);
}


inline void readStringBinary(std::string & s, ReadBuffer & buf, size_t MAX_STRING_SIZE = DEFAULT_MAX_STRING_SIZE)
{
	size_t size = 0;
	readVarUInt(size, buf);

	if (size > MAX_STRING_SIZE)
		throw Poco::Exception("Too large string size.");

	s.resize(size);
	buf.readStrict(&s[0], size);
}


template <typename T>
void readVectorBinary(std::vector<T> & v, ReadBuffer & buf, size_t MAX_VECTOR_SIZE = DEFAULT_MAX_STRING_SIZE)
{
	size_t size = 0;
	readVarUInt(size, buf);

	if (size > MAX_VECTOR_SIZE)
		throw Poco::Exception("Too large vector size.");

	v.resize(size);
	for (size_t i = 0; i < size; ++i)
		readBinary(v[i], buf);
}


void assertString(const char * s, ReadBuffer & buf);
void assertEOF(ReadBuffer & buf);

inline void assertString(const String & s, ReadBuffer & buf)
{
	assertString(s.c_str(), buf);
}


inline void readBoolText(bool & x, ReadBuffer & buf)
{
	char tmp = '0';
	readChar(tmp, buf);
	x = tmp != '0';
}


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
			    if (std::is_signed<T>::value)
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


/** Более оптимизированная версия (примерно в 1.5 раза на реальных данных).
  * Отличается тем, что:
  * - из чисел, начинающихся на ноль, парсится только ноль;
  * - не поддерживается символ '+' перед числом;
  * - символы :;<=>? парсятся, как некоторые цифры.
  */
template <typename T>
void readIntTextUnsafe(T & x, ReadBuffer & buf)
{
	bool negative = false;
	x = 0;

	if (unlikely(buf.eof()))
		throwReadAfterEOF();

	if (std::is_signed<T>::value && *buf.position() == '-')
	{
		++buf.position();
		negative = true;
		if (unlikely(buf.eof()))
			throwReadAfterEOF();
	}

	if (*buf.position() == '0')					/// В реальных данных много нулей.
	{
		++buf.position();
		return;
	}

	while (!buf.eof())
	{
		if ((*buf.position() & 0xF0) == 0x30)	/// Имеет смысл, что это условие находится внутри цикла.
		{
			x *= 10;
			x += *buf.position() & 0x0F;
			++buf.position();
		}
		else
			break;
	}

	if (std::is_signed<T>::value && negative)
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
				x *= exp10(exponent);
				if (negative)
					x = -x;
				return;
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


/// грубо; всё до '\n' или '\t'
void readString(String & s, ReadBuffer & buf);

void readEscapedString(String & s, ReadBuffer & buf);

void readQuotedString(String & s, ReadBuffer & buf);

void readDoubleQuotedString(String & s, ReadBuffer & buf);

void readBackQuotedString(String & s, ReadBuffer & buf);


/// в формате YYYY-MM-DD
inline void readDateText(DayNum_t & date, ReadBuffer & buf)
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

	date = DateLUT::instance().makeDayNum(year, month, day);
}

inline void readDateText(mysqlxx::Date & date, ReadBuffer & buf)
{
	char s[10];
	size_t size = buf.read(s, 10);
	if (10 != size)
	{
		s[size] = 0;
		throw Exception(std::string("Cannot parse date ") + s, ErrorCodes::CANNOT_PARSE_DATE);
	}

	date.year((s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0'));
	date.month((s[5] - '0') * 10 + (s[6] - '0'));
	date.day((s[8] - '0') * 10 + (s[9] - '0'));
}


template <typename T>
inline T parse(const char * data, size_t size);


void readDateTimeTextFallback(time_t & datetime, ReadBuffer & buf);

/** В формате YYYY-MM-DD hh:mm:ss, согласно текущему часовому поясу
  * В качестве исключения, также поддерживается парсинг из десятичного числа - unix timestamp.
  */
inline void readDateTimeText(time_t & datetime, ReadBuffer & buf)
{
	/** Считываем 10 символов, которые могут быть unix timestamp.
	  * При этом, поддерживается только unix timestamp из 10 символов - от 9 сентября 2001.
	  * Потом смотрим на пятый символ. Если это число - парсим unix timestamp.
	  * Если это не число - парсим YYYY-MM-DD hh:mm:ss.
	  */

	/// Оптимистичный вариант, когда всё значение точно лежит в буфере.
	const char * s = buf.position();
	if (s + 19 < buf.buffer().end())
	{
		if (s[4] < '0' || s[4] > '9')
		{
			UInt16 year = (s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0');
			UInt8 month = (s[5] - '0') * 10 + (s[6] - '0');
			UInt8 day = (s[8] - '0') * 10 + (s[9] - '0');

			UInt8 hour = (s[11] - '0') * 10 + (s[12] - '0');
			UInt8 minute = (s[14] - '0') * 10 + (s[15] - '0');
			UInt8 second = (s[17] - '0') * 10 + (s[18] - '0');

			if (unlikely(year == 0))
				datetime = 0;
			else
				datetime = DateLUT::instance().makeDateTime(year, month, day, hour, minute, second);

			buf.position() += 19;
		}
		else
			readIntTextUnsafe(datetime, buf);
	}
	else
		readDateTimeTextFallback(datetime, buf);
}

inline void readDateTimeText(mysqlxx::DateTime & datetime, ReadBuffer & buf)
{
	char s[19];
	size_t size = buf.read(s, 19);
	if (19 != size)
	{
		s[size] = 0;
		throw Exception(std::string("Cannot parse datetime ") + s, ErrorCodes::CANNOT_PARSE_DATETIME);
	}

	datetime.year((s[0] - '0') * 1000 + (s[1] - '0') * 100 + (s[2] - '0') * 10 + (s[3] - '0'));
	datetime.month((s[5] - '0') * 10 + (s[6] - '0'));
	datetime.day((s[8] - '0') * 10 + (s[9] - '0'));

	datetime.hour((s[11] - '0') * 10 + (s[12] - '0'));
	datetime.minute((s[14] - '0') * 10 + (s[15] - '0'));
	datetime.second((s[17] - '0') * 10 + (s[18] - '0'));
}


/// Общие методы для чтения значения в бинарном формате.
inline void readBinary(UInt8 & x, 	ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(UInt16 & x, 	ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(UInt32 & x, 	ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(UInt64 & x, 	ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(Int8 & x, 	ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(Int16 & x, 	ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(Int32 & x, 	ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(Int64 & x, 	ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(Float32 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(Float64 & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(String & x, 	ReadBuffer & buf) { readStringBinary(x, buf); }
inline void readBinary(bool & x, 	ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(uint128 & x,	ReadBuffer & buf) { readPODBinary(x, buf); }

inline void readBinary(VisitID_t & x, ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(mysqlxx::Date & x, 	ReadBuffer & buf) 	{ readPODBinary(x, buf); }
inline void readBinary(mysqlxx::DateTime & x, ReadBuffer & buf) { readPODBinary(x, buf); }


/// Общие методы для чтения значения в текстовом виде из tab-separated формата.
inline void readText(UInt8 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readText(UInt16 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readText(UInt32 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readText(UInt64 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readText(Int8 & x, 		ReadBuffer & buf) { readIntText(x, buf); }
inline void readText(Int16 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readText(Int32 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readText(Int64 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readText(Float32 & x, 	ReadBuffer & buf) { readFloatText(x, buf); }
inline void readText(Float64 & x, 	ReadBuffer & buf) { readFloatText(x, buf); }
inline void readText(String & x, 	ReadBuffer & buf) { readEscapedString(x, buf); }
inline void readText(bool & x, 		ReadBuffer & buf) { readBoolText(x, buf); }

inline void readText(VisitID_t & x, ReadBuffer & buf) { readIntText(x, buf); }
inline void readText(mysqlxx::Date & x, 	ReadBuffer & buf) { readDateText(x, buf); }
inline void readText(mysqlxx::DateTime & x, ReadBuffer & buf) { readDateTimeText(x, buf); }


/// Общие методы для чтения значения в текстовом виде, при необходимости, в кавычках.
inline void readQuoted(UInt8 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readQuoted(UInt16 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readQuoted(UInt32 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readQuoted(UInt64 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readQuoted(Int8 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readQuoted(Int16 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readQuoted(Int32 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readQuoted(Int64 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readQuoted(Float32 & x, ReadBuffer & buf) { readFloatText(x, buf); }
inline void readQuoted(Float64 & x, ReadBuffer & buf) { readFloatText(x, buf); }
inline void readQuoted(String & x, 	ReadBuffer & buf) { readQuotedString(x, buf); }
inline void readQuoted(bool & x, 	ReadBuffer & buf) { readBoolText(x, buf); }

inline void readQuoted(VisitID_t & x, ReadBuffer & buf) { readIntText(x, buf); }

inline void readQuoted(mysqlxx::Date & x, ReadBuffer & buf)
{
	assertString("'", buf);
	readDateText(x, buf);
	assertString("'", buf);
}

inline void readQuoted(mysqlxx::DateTime & x, ReadBuffer & buf)
{
	assertString("'", buf);
	readDateTimeText(x, buf);
	assertString("'", buf);
}


/// В двойных кавычках
inline void readDoubleQuoted(UInt8 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readDoubleQuoted(UInt16 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readDoubleQuoted(UInt32 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readDoubleQuoted(UInt64 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readDoubleQuoted(Int8 & x, 		ReadBuffer & buf) { readIntText(x, buf); }
inline void readDoubleQuoted(Int16 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readDoubleQuoted(Int32 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readDoubleQuoted(Int64 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readDoubleQuoted(Float32 & x, 	ReadBuffer & buf) { readFloatText(x, buf); }
inline void readDoubleQuoted(Float64 & x, 	ReadBuffer & buf) { readFloatText(x, buf); }
inline void readDoubleQuoted(String & x, 	ReadBuffer & buf) { readDoubleQuotedString(x, buf); }
inline void readDoubleQuoted(bool & x, 		ReadBuffer & buf) { readBoolText(x, buf); }

inline void readDoubleQuoted(VisitID_t & x, ReadBuffer & buf) { readIntText(x, buf); }

inline void readDoubleQuoted(mysqlxx::Date & x, ReadBuffer & buf)
{
	assertString("\"", buf);
	readDateText(x, buf);
	assertString("\"", buf);
}

inline void readDoubleQuoted(mysqlxx::DateTime & x, ReadBuffer & buf)
{
	assertString("\"", buf);
	readDateTimeText(x, buf);
	assertString("\"", buf);
}


template <typename T>
void readBinary(std::vector<T> & x, ReadBuffer & buf)
{
	size_t size = 0;
	readVarUInt(size, buf);

	if (size > DEFAULT_MAX_STRING_SIZE)
		throw Poco::Exception("Too large vector size.");

	x.resize(size);
	for (size_t i = 0; i < size; ++i)
		readBinary(x[i], buf);
}

template <typename T>
void readQuoted(std::vector<T> & x, ReadBuffer & buf)
{
	bool first = true;
	assertString("[", buf);
	while (!buf.eof() && *buf.position() != ']')
	{
		if (!first)
		{
			if (*buf.position() == ',')
				++buf.position();
			else
				throw Exception("Cannot read array from text", ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT);
		}

		first = false;

		x.push_back(T());
		readQuoted(x.back(), buf);
	}
	assertString("]", buf);
}

template <typename T>
void readDoubleQuoted(std::vector<T> & x, ReadBuffer & buf)
{
	bool first = true;
	assertString("[", buf);
	while (!buf.eof() && *buf.position() != ']')
	{
		if (!first)
		{
			if (*buf.position() == ',')
				++buf.position();
			else
				throw Exception("Cannot read array from text", ErrorCodes::CANNOT_READ_ARRAY_FROM_TEXT);
		}

		first = false;

		x.push_back(T());
		readDoubleQuoted(x.back(), buf);
	}
	assertString("]", buf);
}

template <typename T>
void readText(std::vector<T> & x, ReadBuffer & buf)
{
	readQuoted(x, buf);
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


/** Прочитать сериализованный эксепшен.
  * При сериализации/десериализации часть информации теряется
  * (тип обрезается до базового, message заменяется на displayText, и stack trace дописывается в message)
  * К нему может быть добавлено дополнительное сообщение (например, вы можете указать, откуда оно было прочитано).
  */
void readException(Exception & e, ReadBuffer & buf, const String & additional_message = "");
void readAndThrowException(ReadBuffer & buf, const String & additional_message = "");


/** Вспомогательная функция
 */
template <typename T>
static inline const char * tryReadIntText(T & x, const char * pos, const char * end)
{
	bool negative = false;
	x = 0;
	if (pos >= end)
		return pos;

	while (pos < end)
	{
		switch (*pos)
		{
			case '+':
				break;
			case '-':
				if (std::is_signed<T>::value)
					negative = true;
				else
					return pos;
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
				x += *pos - '0';
				break;
			default:
				if (negative)
					x = -x;
				return pos;
		}
		++pos;
	}
	if (negative)
		x = -x;
	return pos;
}


/// Простые для использования методы чтения чего-либо из строки в текстовом виде.
template <typename T>
inline T parse(const char * data, size_t size)
{
	T res;
	ReadBuffer buf(const_cast<char *>(data), size, 0);
	readText(res, buf);
	return res;
}

template <typename T>
inline T parse(const char * data)
{
	return parse<T>(data, strlen(data));
}

template <typename T>
inline T parse(const String & s)
{
	return parse<T>(s.data(), s.size());
}

}
