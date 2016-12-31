#pragma once

#include <cmath>
#include <cstring>
#include <limits>
#include <algorithm>

#include <type_traits>

#include <common/Common.h>
#include <common/DateLUT.h>

#include <common/LocalDate.h>
#include <common/LocalDateTime.h>

#include <common/exp10.h>

#include <DB/Core/Types.h>
#include <DB/Core/StringRef.h>
#include <DB/Common/Exception.h>
#include <DB/Common/StringUtils.h>
#include <DB/Common/Arena.h>

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

/// Helper functions for formatted input.

inline char parseEscapeSequence(char c)
{
	switch(c)
	{
		case 'a':
			return '\a';
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
		case 'v':
			return '\v';
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


/// These functions are located in VarInt.h
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


/// Read POD-type in native format
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


inline StringRef readStringBinaryInto(Arena & arena, ReadBuffer & buf)
{
	size_t size = 0;
	readVarUInt(size, buf);

	char * data = arena.alloc(size);
	buf.readStrict(data, size);

	return StringRef(data, size);
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

bool checkStringCaseInsensitive(const char * s, ReadBuffer & buf);
inline bool checkStringCaseInsensitive(const String & s, ReadBuffer & buf)
{
	return checkStringCaseInsensitive(s.c_str(), buf);
}

void assertStringCaseInsensitive(const char * s, ReadBuffer & buf);
inline void assertStringCaseInsensitive(const String & s, ReadBuffer & buf)
{
	return assertStringCaseInsensitive(s.c_str(), buf);
}

/** Check that next character in buf matches first character of s.
  * If true, then check all characters in s and throw exception if it doesn't match.
  * If false, then return false, and leave position in buffer unchanged.
  */
bool checkStringByFirstCharacterAndAssertTheRest(const char * s, ReadBuffer & buf);
bool checkStringByFirstCharacterAndAssertTheRestCaseInsensitive(const char * s, ReadBuffer & buf);

inline bool checkStringByFirstCharacterAndAssertTheRest(const String & s, ReadBuffer & buf)
{
	return checkStringByFirstCharacterAndAssertTheRest(s.c_str(), buf);
}

inline bool checkStringByFirstCharacterAndAssertTheRestCaseInsensitive(const String & s, ReadBuffer & buf)
{
	return checkStringByFirstCharacterAndAssertTheRestCaseInsensitive(s.c_str(), buf);
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

/** More efficient variant (about 1.5 times on real dataset).
  * Differs in following:
  * - for numbers starting with zero, parsed only zero;
  * - symbol '+' before number is not supported;
  * - symbols :;<=>? are parsed as some numbers.
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

	if (*buf.position() == '0')					/// There are many zeros in real datasets.
	{
		++buf.position();
		return;
	}

	while (!buf.eof())
	{
		if ((*buf.position() & 0xF0) == 0x30)	/// It makes sense to have this condition inside loop.
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


/// Returns true, iff parsed.
bool parseInfinity(ReadBuffer & buf);
bool parseNaN(ReadBuffer & buf);

void assertInfinity(ReadBuffer & buf);
void assertNaN(ReadBuffer & buf);


/// Rough: not exactly nearest machine representable number is returned.
/// Some garbage may be successfully parsed, examples: '.' parsed as 0; 123Inf parsed as inf.
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
			case 'I':
			{
				bool res = exceptionPolicySelector<throw_exception>(assertInfinity, parseInfinity, buf);
				if (res)
				{
					x = std::numeric_limits<T>::infinity();
					if (negative)
						x = -x;
				}
				return ReturnType(res);
			}

			case 'n':
			case 'N':
			{
				bool res = exceptionPolicySelector<throw_exception>(assertNaN, parseNaN, buf);
				if (res)
				{
					x = std::numeric_limits<T>::quiet_NaN();
					if (negative)
						x = -x;
				}
				return ReturnType(res);
			}

			default:
			{
				if (negative)
					x = -x;
				return ReturnType(true);
			}
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

/// rough; all until '\n' or '\t'
void readString(String & s, ReadBuffer & buf);

void readEscapedString(String & s, ReadBuffer & buf);

void readQuotedString(String & s, ReadBuffer & buf);

void readDoubleQuotedString(String & s, ReadBuffer & buf);

void readJSONString(String & s, ReadBuffer & buf);

void readBackQuotedString(String & s, ReadBuffer & buf);

void readStringUntilEOF(String & s, ReadBuffer & buf);


/** Read string in CSV format.
  * Parsing rules:
  * - string could be placed in quotes; quotes could be single: ' or double: ";
  * - or string could be unquoted - this is determined by first character;
  * - if string is unquoted, then it is read until next delimiter,
  *   either until end of line (CR or LF),
  *   or until end of stream;
  *   but spaces and tabs at begin and end of unquoted string are consumed but ignored (note that this behaviour differs from RFC).
  * - if string is in quotes, then it will be read until closing quote,
  *   but sequences of two consecutive quotes are parsed as single quote inside string;
  */
void readCSVString(String & s, ReadBuffer & buf, const char delimiter = ',');


/// Read and append result to array of characters.
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

/// This could be used as template parameter for functions above, if you want to just skip data.
struct NullSink
{
	void append(const char *, size_t) {};
	void push_back(char) {};
};


/// In YYYY-MM-DD format
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

/** In YYYY-MM-DD hh:mm:ss format, according to current time zone.
  * As an exception, also supported parsing of unix timestamp in form of decimal number.
  */
inline void readDateTimeText(time_t & datetime, ReadBuffer & buf)
{
	/** Read 10 characters, that could represent unix timestamp.
	  * Only unix timestamp of 5-10 characters is supported.
	  * Then look at 5th charater. If it is a number - treat whole as unix timestamp.
	  * If it is not a number - then parse datetime in YYYY-MM-DD hh:mm:ss format.
	  */

	/// Optimistic path, when whole value are in buffer.
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
			/// Why not readIntTextUnsafe? Because for needs of AdFox, parsing of unix timestamp with leading zeros is supported: 000...NNNN.
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


/// Generic methods to read value in native binary format.
inline void readBinary(UInt8 & x, 	ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(UInt16 & x, 	ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(UInt32 & x, 	ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(UInt64 & x, 	ReadBuffer & buf) { readPODBinary(x, buf); }
#ifdef __APPLE__
/**
 * On Linux x86_64 'int64_t' maps to "long", but on Apple x86_64 it maps to 'long long'
 * But on both platforms Int64 maps to 'long'. This is two different types
 * with the same size==8, so we need extra functions here
 */
inline void readBinary(uint64_t & x, 	ReadBuffer & buf) { readPODBinary(x, buf); }
inline void readBinary(int64_t & x, 	ReadBuffer & buf) { readPODBinary(x, buf); }
#endif
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


/// Generic methods to read value in text tab-separated format.
inline void readText(UInt8 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readText(UInt16 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readText(UInt32 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readText(UInt64 & x, 	ReadBuffer & buf) { readIntText(x, buf); }
#ifdef __APPLE__
inline void readText(uint64_t & x, 	ReadBuffer & buf) { readIntText(x, buf); }
inline void readText(int64_t & x, 	ReadBuffer & buf) { readIntText(x, buf); }
#endif
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


/// Generic methods to read value in text format,
///  possibly in single quotes (only for data types that use quotes in VALUES format of INSERT statement in SQL).
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


/// Same as above, but in double quotes.
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


/// CSV, for numbers, dates, datetimes: quotes are optional, no special escaping rules.
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


/// Skip whitespace characters.
inline void skipWhitespaceIfAny(ReadBuffer & buf)
{
	while (!buf.eof() && isWhitespaceASCII(*buf.position()))
		++buf.position();
}

/// Skips json value. If the value contains objects (i.e. {...} sequence), an exception will be thrown.
void skipJSONFieldPlain(ReadBuffer & buf, const StringRef & name_of_filed);


/** Read serialized exception.
  * During serialization/deserialization some information is lost
  * (type is cut to base class, 'message' replaced by 'displayText', and stack trace is appended to 'message')
  * Some additional message could be appended to exception (example: you could add information about from where it was received).
  */
void readException(Exception & e, ReadBuffer & buf, const String & additional_message = "");
void readAndThrowException(ReadBuffer & buf, const String & additional_message = "");


/** Helper function for implementation.
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


/// Convenient methods for reading something from string in text format.
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


/** Skip UTF-8 BOM if it is under cursor.
  * As BOM is usually located at start of stream, and buffer size is usually larger than three bytes,
  *  the function expects, that all three bytes of BOM is fully in buffer (otherwise it don't skip anything).
  */
inline void skipBOMIfExists(ReadBuffer & buf)
{
	if (!buf.eof()
		&& buf.position() + 3 < buf.buffer().end()
		&& buf.position()[0] == '\xEF'
		&& buf.position()[1] == '\xBB'
		&& buf.position()[2] == '\xBF')
	{
		buf.position() += 3;
	}
}

}
