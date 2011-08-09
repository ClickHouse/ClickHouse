#ifndef DBMS_COMMON_WRITEHELPERS_H
#define DBMS_COMMON_WRITEHELPERS_H

#include <cstring>
#include <cstdio>
#include <limits>
#include <algorithm>

#include <Yandex/DateLUT.h>

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/WriteBuffer.h>

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

template <typename T>
void writeText(T x, WriteBuffer & buf);

template <> inline void writeText<UInt8>	(UInt8 x, 	WriteBuffer & buf) { writeIntText(x, buf); }
template <> inline void writeText<UInt16>	(UInt16 x, 	WriteBuffer & buf) { writeIntText(x, buf); }
template <> inline void writeText<UInt32>	(UInt32 x, 	WriteBuffer & buf) { writeIntText(x, buf); }
template <> inline void writeText<UInt64>	(UInt64 x, 	WriteBuffer & buf) { writeIntText(x, buf); }
template <> inline void writeText<Int8>		(Int8 x, 	WriteBuffer & buf) { writeIntText(x, buf); }
template <> inline void writeText<Int16>	(Int16 x, 	WriteBuffer & buf) { writeIntText(x, buf); }
template <> inline void writeText<Int32>	(Int32 x, 	WriteBuffer & buf) { writeIntText(x, buf); }
template <> inline void writeText<Int64>	(Int64 x, 	WriteBuffer & buf) { writeIntText(x, buf); }
template <> inline void writeText<Float32>	(Float32 x, WriteBuffer & buf) { writeFloatText(x, buf); }
template <> inline void writeText<Float64>	(Float64 x, WriteBuffer & buf) { writeFloatText(x, buf); }

inline void writeString(const String & s, WriteBuffer & buf)
{
	buf.write(s.data(), s.size());
}

/// предполагается, что строка в оперативке хранится непрерывно, и \0-terminated.
void writeEscapedString(const String & s, WriteBuffer & buf);

inline void writeQuotedString(const String & s, WriteBuffer & buf)
{
	writeChar('\'', buf);
	writeEscapedString(s, buf);
	writeChar('\'', buf);
}

/// Совместимо с JSON.
void writeDoubleQuotedString(const String & s, WriteBuffer & buf);


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


}

#endif
