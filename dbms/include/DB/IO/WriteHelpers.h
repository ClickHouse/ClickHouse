#pragma once

#include <cstring>
#include <cstdio>
#include <limits>
#include <algorithm>

#include <Yandex/DateLUT.h>

#include <mysqlxx/Row.h>

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/VarInt.h>

#define WRITE_HELPERS_DEFAULT_FLOAT_PRECISION 6U
/// 20 цифр и знак
#define WRITE_HELPERS_MAX_INT_WIDTH 21U


namespace DB
{

/// Функции-помошники для форматированной записи

inline void writeChar(char x, WriteBuffer & buf)
{
	buf.nextIfAtEnd();
	*buf.position() = x;
	++buf.position();
}


/// Запись числа в native формате
template <typename T>
inline void writeBinary(T & x, WriteBuffer & buf)
{
	buf.write(reinterpret_cast<const char *>(&x), sizeof(x));
}

template <typename T>
inline void writeIntBinary(T & x, WriteBuffer & buf)
{
	writeBinary(x, buf);
}

template <typename T>
inline void writeFloatBinary(T & x, WriteBuffer & buf)
{
	writeBinary(x, buf);
}


inline void writeStringBinary(const std::string & s, DB::WriteBuffer & buf)
{
	writeVarUInt(s.size(), buf);
	buf.write(s.data(), s.size());
}


inline void writeBoolText(bool x, WriteBuffer & buf)
{
	writeChar(x ? '1' : '0', buf);
}


template <typename T>
void writeIntText(T x, WriteBuffer & buf)
{
	char tmp[WRITE_HELPERS_MAX_INT_WIDTH];
	bool negative = false;

	if (x == 0)
	{
		writeChar('0', buf);
		return;
	}

	if (x < 0)
	{
		x = -x;
		negative = true;
	}

	char * pos;
	for (pos = tmp + WRITE_HELPERS_MAX_INT_WIDTH - 1; x != 0; --pos)
	{
		*pos = '0' + x % 10;
		x /= 10;
	}

	if (negative)
		*pos = '-';
	else
		++pos;

	buf.write(pos, tmp + WRITE_HELPERS_MAX_INT_WIDTH - pos);
}

template <typename T>
void writeFloatText(T x, WriteBuffer & buf, unsigned precision = WRITE_HELPERS_DEFAULT_FLOAT_PRECISION)
{
	unsigned size = precision + 10;
	char tmp[size];	/// знаки, +0.0e+123\0
	int res = std::snprintf(tmp, size, "%.*g", precision, x);

	if (res >= static_cast<int>(size) || res <= 0)
		throw Exception("Cannot print float or double number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

	buf.write(tmp, res);
}


inline void writeString(const String & s, WriteBuffer & buf)
{
	buf.write(s.data(), s.size());
}


template <char c>
void writeAnyEscapedString(const char * begin, const char * end, WriteBuffer & buf)
{
	for (const char * it = begin; it != end; ++it)
	{
		switch (*it)
		{
			case '\b':
				writeChar('\\', buf);
				writeChar('b', buf);
				break;
			case '\f':
				writeChar('\\', buf);
				writeChar('f', buf);
				break;
			case '\n':
				writeChar('\\', buf);
				writeChar('n', buf);
				break;
			case '\r':
				writeChar('\\', buf);
				writeChar('r', buf);
				break;
			case '\t':
				writeChar('\\', buf);
				writeChar('t', buf);
				break;
			case '\0':
				writeChar('\\', buf);
				writeChar('0', buf);
				break;
			case '\\':
				writeChar('\\', buf);
				writeChar('\\', buf);
				break;
			case c:
				writeChar('\\', buf);
				writeChar(c, buf);
				break;
			default:
				writeChar(*it, buf);
		}
	}
}


template <char c>
void writeAnyEscapedString(const String & s, WriteBuffer & buf)
{
	writeAnyEscapedString<c>(s.data(), s.data() + s.size(), buf);
}


inline void writeEscapedString(const String & s, WriteBuffer & buf)
{
	/// strpbrk хорошо оптимизирована (этот if ускоряет код в 1.5 раза)
	if (NULL == strpbrk(s.data(), "\b\f\n\r\t\'\\") && strlen(s.data()) == s.size())
		writeString(s, buf);
	else
		writeAnyEscapedString<'\''>(s, buf);
}


template <char c>
void writeAnyQuotedString(const String & s, WriteBuffer & buf)
{
	writeChar(c, buf);
	writeAnyEscapedString<c>(s, buf);
	writeChar(c, buf);
}


inline void writeQuotedString(const String & s, WriteBuffer & buf)
{
	writeAnyQuotedString<'\''>(s, buf);
}

/// Совместимо с JSON.
inline void writeDoubleQuotedString(const String & s, WriteBuffer & buf)
{
	writeAnyQuotedString<'"'>(s, buf);
}

/// Выводит строку в обратных кавычках, как идентификатор в MySQL.
inline void writeBackQuotedString(const String & s, WriteBuffer & buf)
{
	writeAnyQuotedString<'`'>(s, buf);
}

/// То же самое, но обратные кавычки применяются только при наличии символов, не подходящих для идентификатора без обратных кавычек.
inline void writeProbablyBackQuotedString(const String & s, WriteBuffer & buf)
{
	if (s.empty() || !((s[0] >= 'a' && s[0] <= 'z') || (s[0] >= 'A' && s[0] <= 'Z') || s[0] == '_'))
		writeBackQuotedString(s, buf);
	else
	{
		const char * pos = s.data() + 1;
		const char * end = s.data() + s.size();
		for (; pos < end; ++pos)
			if (!((*pos >= 'a' && *pos <= 'z') || (*pos >= 'A' && *pos <= 'Z') || (*pos >= '0' && *pos <= '9') || *pos == '_'))
				break;
		if (pos != end)
			writeBackQuotedString(s, buf);
		else
			writeString(s, buf);
	}
}


/// в формате YYYY-MM-DD
inline void writeDateText(Yandex::DayNum_t date, WriteBuffer & buf)
{
	char s[10] = {'0', '0', '0', '0', '-', '0', '0', '-', '0', '0'};

	if (unlikely(date > DATE_LUT_MAX_DAY_NUM || date == 0))
	{
		buf.write(s, 10);
		return;
	}

	const Yandex::DateLUT::Values & values = Yandex::DateLUTSingleton::instance().getValues(date);

	s[0] += values.year / 1000;
	s[1] += (values.year / 100) % 10;
	s[2] += (values.year / 10) % 10;
	s[3] += values.year % 10;
	s[5] += values.month / 10;
	s[6] += values.month % 10;
	s[8] += values.day_of_month / 10;
	s[9] += values.day_of_month % 10;
	
	buf.write(s, 10);
}

inline void writeDateText(mysqlxx::Date date, WriteBuffer & buf)
{
	char s[10] = {'0', '0', '0', '0', '-', '0', '0', '-', '0', '0'};

	s[0] += date.year() / 1000;
	s[1] += (date.year() / 100) % 10;
	s[2] += (date.year() / 10) % 10;
	s[3] += date.year() % 10;
	s[5] += date.month() / 10;
	s[6] += date.month() % 10;
	s[8] += date.day() / 10;
	s[9] += date.day() % 10;

	buf.write(s, 10);
}


/// в формате YYYY-MM-DD HH:MM:SS, согласно текущему часовому поясу
inline void writeDateTimeText(time_t datetime, WriteBuffer & buf)
{
	char s[19] = {'0', '0', '0', '0', '-', '0', '0', '-', '0', '0', ' ', '0', '0', ':', '0', '0', ':', '0', '0'};

	if (unlikely(datetime > DATE_LUT_MAX || datetime == 0))
	{
		buf.write(s, 19);
		return;
	}

	Yandex::DateLUTSingleton & date_lut = Yandex::DateLUTSingleton::instance();
	const Yandex::DateLUT::Values & values = date_lut.getValues(datetime);

	s[0] += values.year / 1000;
	s[1] += (values.year / 100) % 10;
	s[2] += (values.year / 10) % 10;
	s[3] += values.year % 10;
	s[5] += values.month / 10;
	s[6] += values.month % 10;
	s[8] += values.day_of_month / 10;
	s[9] += values.day_of_month % 10;

	UInt8 hour = date_lut.toHourInaccurate(datetime);
	UInt8 minute = date_lut.toMinute(datetime);
	UInt8 second = date_lut.toSecond(datetime);

	s[11] += hour / 10;
	s[12] += hour % 10;
	s[14] += minute / 10;
	s[15] += minute % 10;
	s[17] += second / 10;
	s[18] += second % 10;

	buf.write(s, 19);
}

inline void writeDateTimeText(mysqlxx::DateTime datetime, WriteBuffer & buf)
{
	char s[19] = {'0', '0', '0', '0', '-', '0', '0', '-', '0', '0', ' ', '0', '0', ':', '0', '0', ':', '0', '0'};

	s[0] += datetime.year() / 1000;
	s[1] += (datetime.year() / 100) % 10;
	s[2] += (datetime.year() / 10) % 10;
	s[3] += datetime.year() % 10;
	s[5] += datetime.month() / 10;
	s[6] += datetime.month() % 10;
	s[8] += datetime.day() / 10;
	s[9] += datetime.day() % 10;

	s[11] += datetime.hour() / 10;
	s[12] += datetime.hour() % 10;
	s[14] += datetime.minute() / 10;
	s[15] += datetime.minute() % 10;
	s[17] += datetime.second() / 10;
	s[18] += datetime.second() % 10;

	buf.write(s, 19);
}


/// Вывести mysqlxx::Row в tab-separated виде
inline void writeEscapedRow(const mysqlxx::Row & row, WriteBuffer & buf)
{
	for (size_t i = 0; i < row.size(); ++i)
	{
		if (i != 0)
			buf.write('\t');

		if (unlikely(row[i].isNull()))
		{
			buf.write("\\N", 2);
			continue;
		}

		writeAnyEscapedString<'\''>(row[i].data(), row[i].data() + row[i].length(), buf);
	}
}


template <typename T>
void writeText(const T & x, WriteBuffer & buf)
{
	/// Переношу ошибку в рантайм, так как метод требуется для компиляции DBObject-ов
	throw Exception("Method writeText is not implemented for this type.", ErrorCodes::NOT_IMPLEMENTED);
}

template <> inline void writeText<UInt8>	(const UInt8 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
template <> inline void writeText<UInt16>	(const UInt16 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
template <> inline void writeText<UInt32>	(const UInt32 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
template <> inline void writeText<UInt64>	(const UInt64 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
template <> inline void writeText<Int8>		(const Int8 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
template <> inline void writeText<Int16>	(const Int16 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
template <> inline void writeText<Int32>	(const Int32 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
template <> inline void writeText<Int64>	(const Int64 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
template <> inline void writeText<Float32>	(const Float32 & x, WriteBuffer & buf) { writeFloatText(x, buf); }
template <> inline void writeText<Float64>	(const Float64 & x, WriteBuffer & buf) { writeFloatText(x, buf); }
template <> inline void writeText<String>	(const String & x,	WriteBuffer & buf) { writeEscapedString(x, buf); }
template <> inline void writeText<bool>		(const bool & x, 	WriteBuffer & buf) { writeBoolText(x, buf); }

template <> inline void writeText<mysqlxx::Date>		(const mysqlxx::Date & x,		WriteBuffer & buf) { writeDateText(x, buf); }
template <> inline void writeText<mysqlxx::DateTime>	(const mysqlxx::DateTime & x,	WriteBuffer & buf) { writeDateTimeText(x, buf); }


}
