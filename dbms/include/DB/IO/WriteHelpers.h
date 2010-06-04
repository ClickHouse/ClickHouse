#ifndef DBMS_COMMON_WRITEHELPERS_H
#define DBMS_COMMON_WRITEHELPERS_H

#include <cstring>
#include <cstdio>
#include <limits>
#include <algorithm>

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/WriteBuffer.h>

#define WRITE_HELPERS_DEFAULT_FLOAT_PRECISION 6
/// 20 цифр, знак, и \0 для конца строки
#define WRITE_HELPERS_MAX_INT_WIDTH 22


namespace DB
{

/// Функции-помошники для форматированной записи

inline void writeChar(char x, WriteBuffer & buf)
{
	buf.nextIfAtEnd();
	*buf.position() = x;
	++buf.position();
}


template <typename T> struct IntFormat { static const char * format; };

template <typename T>
void writeIntText(T x, WriteBuffer & buf)
{
	char tmp[WRITE_HELPERS_MAX_INT_WIDTH];
	int res = std::snprintf(tmp, WRITE_HELPERS_MAX_INT_WIDTH, IntFormat<T>::format, x);

	if (res >= WRITE_HELPERS_MAX_INT_WIDTH || res <= 0)
		throw Exception("Cannot print integer", ErrorCodes::CANNOT_PRINT_INTEGER);

	buf.write(tmp, res - 1);
}

template <typename T>
void writeFloatText(T x, WriteBuffer & buf, unsigned precision = WRITE_HELPERS_DEFAULT_FLOAT_PRECISION)
{
	unsigned size = precision + 10;
	char tmp[size];	/// знаки, +0.0e+123\0
	int res = std::snprintf(tmp, size, "%.*g", precision, x);

	if (res >= static_cast<int>(size) || res <= 0)
		throw Exception("Cannot print float or double number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

	buf.write(tmp, res - 1);
}

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


}

#endif
