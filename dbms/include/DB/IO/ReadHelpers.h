#pragma once

#include <cstring>
#include <limits>
#include <algorithm>

#include <type_traits>

#include <common/Common.h>
#include <common/DateLUT.h>

#include <common/LocalDate.h>
#include <common/LocalDateTime.h>

#include <DB/Core/Types.h>
#include <DB/Common/Exception.h>

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/VarInt.h>
#include <city.h>

#define DEFAULT_MAX_STRING_SIZE 0x00FFFFFFULL


namespace DB
{

namespace ErrorCodes
{
	extern const int CANNOT_PARSE_DATE;
	extern const int CANNOT_PARSE_DATETIME;
	extern const int CANNOT_READ_ARRAY_FROM_TEXT;
}

/// Функции-помошники для форматированного чтения

inline char parseEscapeSequence(char c)
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

inline char unhex(char c)
{
	switch (c)
	{
		case '0' ... '9':
			return c - '0';
		case 'a' ... 'f':
			return c - 'a' + 10;
		case 'A' ... 'F':
			return c - 'A' + 10;
		default:
			return 0;
	}
}


/// Эти функции находятся в VarInt.h
/// inline void throwReadAfterEOF()


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
void assertChar(char symbol, ReadBuffer & buf);

inline void assertString(const String & s, ReadBuffer & buf)
{
	assertString(s.c_str(), buf);
}

bool checkString(const char * s, ReadBuffer & buf);
inline bool checkString(const String & s, ReadBuffer & buf)
{
	return checkString(s.c_str(), buf);
}

inline bool checkChar(char c, ReadBuffer & buf)
{
	if (buf.eof() || *buf.position() != c)
		return false;
	++buf.position();
	return true;
}

inline void readBoolText(bool & x, ReadBuffer & buf)
{
	char tmp = '0';
	readChar(tmp, buf);
	x = tmp != '0';
}

template <typename T, typename ReturnType = void>
ReturnType readIntTextImpl(T & x, ReadBuffer & buf)
{
	static constexpr bool throw_exception = std::is_same<ReturnType, void>::value;

	bool negative = false;
	x = 0;
	if (buf.eof())
	{
		if (throw_exception)
			throwReadAfterEOF();
		else
			return ReturnType(false);
	}

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
					return ReturnType(false);
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
				return ReturnType(true);
		}
		++buf.position();
	}
	if (negative)
		x = -x;

	return ReturnType(true);
}

template <typename T>
void readIntText(T & x, ReadBuffer & buf)
{
	readIntTextImpl<T, void>(x, buf);
}

template <typename T>
bool tryReadIntText(T & x, ReadBuffer & buf)
{
	return readIntTextImpl<T, bool>(x, buf);
}

/** Более оптимизированная версия (примерно в 1.5 раза на реальных данных).
  * Отличается тем, что:
  * - из чисел, начинающихся на ноль, парсится только ноль;
  * - не поддерживается символ '+' перед числом;
  * - символы :;<=>? парсятся, как некоторые цифры.
  */
template <typename T, bool throw_on_error = true>
void readIntTextUnsafe(T & x, ReadBuffer & buf)
{
	bool negative = false;
	x = 0;

	auto on_error = []
	{
		if (throw_on_error)
			throwReadAfterEOF();
	};

	if (unlikely(buf.eof()))
		return on_error();

	if (std::is_signed<T>::value && *buf.position() == '-')
	{
		++buf.position();
		negative = true;
		if (unlikely(buf.eof()))
			return on_error();
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

template<typename T>
void tryReadIntTextUnsafe(T & x, ReadBuffer & buf)
{
	return readIntTextUnsafe<T, false>(x, buf);
}


template <bool throw_exception, class ExcepFun, class NoExcepFun, class... Args>
bool exceptionPolicySelector(ExcepFun && excep_f, NoExcepFun && no_excep_f, Args &&... args)
{
	if (throw_exception)
	{
		excep_f(std::forward<Args>(args)...);
		return true;
	}
	else
		return no_excep_f(std::forward<Args>(args)...);
};


/// грубо
template <typename T, typename ReturnType, char point_symbol = '.'>
ReturnType readFloatTextImpl(T & x, ReadBuffer & buf)
{
	static constexpr bool throw_exception = std::is_same<ReturnType, void>::value;

	bool negative = false;
	x = 0;
	bool after_point = false;
	double power_of_ten = 1;

	if (buf.eof())
	{
		if (throw_exception)
			throwReadAfterEOF();
		else
			return ReturnType(false);
	}

	auto parse_special_value = [&buf, &x, &negative](const char * str, T value)
	{
		auto assert_str_lambda = [](const char * str, ReadBuffer & buf){ assertString(str, buf); };
		auto check_str_lambda = [](const char * str, ReadBuffer & buf){ return checkString(str, buf); };

		++buf.position();
		bool result = exceptionPolicySelector<throw_exception>(assert_str_lambda, check_str_lambda, str, buf);
		if (result)
		{
			x = value;
			if (negative)
				x = -x;
		}
		return result;
	};

	while (!buf.eof())
	{
		switch (*buf.position())
		{
			case '+':
				break;
			case '-':
				negative = true;
				break;
			case point_symbol:
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
				bool res = exceptionPolicySelector<throw_exception>(readIntText<Int32>, tryReadIntText<Int32>, exponent, buf);
				if (res)
				{
					x *= exp10(exponent);
					if (negative)
						x = -x;
				}
				return ReturnType(res);
			}
			case 'i':
				return ReturnType(parse_special_value("nf", std::numeric_limits<T>::infinity()));

			case 'I':
				return ReturnType(parse_special_value("NF", std::numeric_limits<T>::infinity()));

			case 'n':
				return ReturnType(parse_special_value("an", std::numeric_limits<T>::quiet_NaN()));

			case 'N':
				return ReturnType(parse_special_value("AN", std::numeric_limits<T>::quiet_NaN()));

			default:
				if (negative)
					x = -x;
				return ReturnType(true);
		}
		++buf.position();
	}
	if (negative)
		x = -x;

	return ReturnType(true);
}

template <class T>
inline bool tryReadFloatText(T & x, ReadBuffer & buf)
{
	return readFloatTextImpl<T, bool>(x, buf);
}

template <class T>
inline void readFloatText(T & x, ReadBuffer & buf)
{
	readFloatTextImpl<T, void>(x, buf);
}

/// грубо; всё до '\n' или '\t'
void readString(String & s, ReadBuffer & buf);

void readEscapedString(String & s, ReadBuffer & buf);

void readQuotedString(String & s, ReadBuffer & buf);

void readDoubleQuotedString(String & s, ReadBuffer & buf);

void readJSONString(String & s, ReadBuffer & buf);

void readBackQuotedString(String & s, ReadBuffer & buf);

void readStringUntilEOF(String & s, ReadBuffer & buf);


/** Прочитать строку в формате CSV.
  * Правила:
  * - строка может быть заключена в кавычки; кавычки могут быть одинарными: ' или двойными: ";
  * - а может быть не заключена в кавычки - это определяется по первому символу;
  * - если строка не заключена в кавычки, то она читается до следующего разделителя,
  *   либо до перевода строки (CR или LF),
  *   либо до конца потока;
  *   при этом, пробелы и табы на конце строки съедаются, но игнорируются.
  * - если строка заключена в кавычки, то она читается до закрывающей кавычки,
  *   но при этом, последовательность двух подряд идущих кавычек, считается одной кавычкой внутри строки;
  */
void readCSVString(String & s, ReadBuffer & buf, const char delimiter = ',');


/// Добавляют прочитанный результат в массив символов.
template <typename Vector>
void readStringInto(Vector & s, ReadBuffer & buf);

template <typename Vector>
void readEscapedStringInto(Vector & s, ReadBuffer & buf);

template <typename Vector>
void readQuotedStringInto(Vector & s, ReadBuffer & buf);

template <typename Vector>
void readDoubleQuotedStringInto(Vector & s, ReadBuffer & buf);

template <typename Vector>
void readBackQuotedStringInto(Vector & s, ReadBuffer & buf);

template <typename Vector>
void readStringUntilEOFInto(Vector & s, ReadBuffer & buf);

template <typename Vector>
void readCSVStringInto(Vector & s, ReadBuffer & buf, const char delimiter = ',');

template <typename Vector>
void readJSONStringInto(Vector & s, ReadBuffer & buf);

/// Это можно использовать в качестве параметра шаблона для функций выше, если данные не надо никуда считывать, но нужно просто пропустить.
struct NullSink
{
	void append(const char *, size_t) {};
	void push_back(char) {};
};


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

inline void readDateText(LocalDate & date, ReadBuffer & buf)
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
	  * При этом, поддерживается только unix timestamp из 5-10 символов.
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
			/// Почему не readIntTextUnsafe? Дело в том, что для нужд AdFox, поддерживается парсинг unix timestamp с отбивкой нулями: 000...NNNN.
			readIntText(datetime, buf);
	}
	else
		readDateTimeTextFallback(datetime, buf);
}

inline void readDateTimeText(LocalDateTime & datetime, ReadBuffer & buf)
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
inline void readBinary(LocalDate & x, 	ReadBuffer & buf) 	{ readPODBinary(x, buf); }
inline void readBinary(LocalDateTime & x, ReadBuffer & buf) { readPODBinary(x, buf); }


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
inline void readText(LocalDate & x, 	ReadBuffer & buf) { readDateText(x, buf); }
inline void readText(LocalDateTime & x, ReadBuffer & buf) { readDateTimeText(x, buf); }


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

inline void readQuoted(LocalDate & x, ReadBuffer & buf)
{
	assertChar('\'', buf);
	readDateText(x, buf);
	assertChar('\'', buf);
}

inline void readQuoted(LocalDateTime & x, ReadBuffer & buf)
{
	assertChar('\'', buf);
	readDateTimeText(x, buf);
	assertChar('\'', buf);
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

inline void readDoubleQuoted(LocalDate & x, ReadBuffer & buf)
{
	assertChar('"', buf);
	readDateText(x, buf);
	assertChar('"', buf);
}

inline void readDoubleQuoted(LocalDateTime & x, ReadBuffer & buf)
{
	assertChar('"', buf);
	readDateTimeText(x, buf);
	assertChar('"', buf);
}


/// CSV, для чисел, дат, дат-с-временем: кавычки не обязательны, специального эскейпинга нет.
template <typename T>
inline void readCSVSimple(T & x, ReadBuffer & buf)
{
	if (buf.eof())
		throwReadAfterEOF();

	char maybe_quote = *buf.position();

	if (maybe_quote == '\'' || maybe_quote == '\"')
		++buf.position();

	readText(x, buf);

	if (maybe_quote == '\'' || maybe_quote == '\"')
		assertChar(maybe_quote, buf);
}

inline void readCSV(UInt8 & x, 		ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(UInt16 & x, 	ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(UInt32 & x, 	ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(UInt64 & x, 	ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(Int8 & x, 		ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(Int16 & x, 		ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(Int32 & x, 		ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(Int64 & x, 		ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(Float32 & x, 	ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(Float64 & x, 	ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(String & x, ReadBuffer & buf, const char delimiter = ',') { readCSVString(x, buf, delimiter); }
inline void readCSV(bool & x, 		ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(VisitID_t & x, 	ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(LocalDate & x, 	ReadBuffer & buf) { readCSVSimple(x, buf); }
inline void readCSV(LocalDateTime & x, ReadBuffer & buf) { readCSVSimple(x, buf); }


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
	assertChar('[', buf);
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
	assertChar(']', buf);
}

template <typename T>
void readDoubleQuoted(std::vector<T> & x, ReadBuffer & buf)
{
	bool first = true;
	assertChar('[', buf);
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
	assertChar(']', buf);
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
