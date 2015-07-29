#pragma once

#include <cstring>
#include <cstdio>
#include <limits>
#include <algorithm>

#include <Yandex/Common.h>
#include <Yandex/DateLUT.h>

#include <mysqlxx/Row.h>
#include <mysqlxx/Null.h>

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteIntText.h>
#include <DB/IO/VarInt.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/DoubleConverter.h>
#include <city.h>


namespace DB
{

/// Функции-помошники для форматированной записи

inline void writeChar(char x, WriteBuffer & buf)
{
	buf.nextIfAtEnd();
	*buf.position() = x;
	++buf.position();
}


/// Запись POD-типа в native формате
template <typename T>
inline void writePODBinary(const T & x, WriteBuffer & buf)
{
	buf.write(reinterpret_cast<const char *>(&x), sizeof(x));
}

template <typename T>
inline void writeIntBinary(const T & x, WriteBuffer & buf)
{
	writePODBinary(x, buf);
}

template <typename T>
inline void writeFloatBinary(const T & x, WriteBuffer & buf)
{
	writePODBinary(x, buf);
}


inline void writeStringBinary(const std::string & s, WriteBuffer & buf)
{
	writeVarUInt(s.size(), buf);
	buf.write(s.data(), s.size());
}

inline void writeStringBinary(const char * s, WriteBuffer & buf)
{
	writeVarUInt(strlen(s), buf);
	buf.write(s, strlen(s));
}


template <typename T>
void writeVectorBinary(const std::vector<T> & v, WriteBuffer & buf)
{
	writeVarUInt(v.size(), buf);

	for (typename std::vector<T>::const_iterator it = v.begin(); it != v.end(); ++it)
		writeBinary(*it, buf);
}


inline void writeBoolText(bool x, WriteBuffer & buf)
{
	writeChar(x ? '1' : '0', buf);
}


template <typename T>
void writeFloatText(T x, WriteBuffer & buf)
{
	char tmp[25];
	double_conversion::StringBuilder builder{tmp, sizeof(tmp)};

	const auto result = getDoubleToStringConverter<false>().ToShortest(x, &builder);

	if (!result)
		throw Exception("Cannot print float or double number", ErrorCodes::CANNOT_PRINT_FLOAT_OR_DOUBLE_NUMBER);

	buf.write(tmp, builder.position());
}


inline void writeString(const String & s, WriteBuffer & buf)
{
	buf.write(s.data(), s.size());
}

inline void writeString(const char * data, size_t size, WriteBuffer & buf)
{
	buf.write(data, size);
}


/** Пишет С-строку без создания временного объекта. Если строка - литерал, то strlen выполняется на этапе компиляции.
  * Используйте, когда строка - литерал.
  */
#define writeCString(s, buf) \
	(buf).write((s), strlen(s))

/** Пишет строку для использования в формате JSON:
 *  - строка выводится в двойных кавычках
 *  - эскейпится символ прямого слеша '/'
 *  - байты из диапазона 0x00-0x1F кроме '\b', '\f', '\n', '\r', '\t' эскейпятся как \u00XX
 *  - кодовые точки U+2028 и U+2029 (последовательности байт в UTF-8: e2 80 a8, e2 80 a9) эскейпятся как \u2028 и \u2029
 *  - предполагается, что строка в кодировке UTF-8, невалидный UTF-8 не обрабатывается
 *  - не-ASCII символы остаются как есть
 */
inline void writeJSONString(const char * begin, const char * end, WriteBuffer & buf)
{
	writeChar('"', buf);
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
			case '\\':
				writeChar('\\', buf);
				writeChar('\\', buf);
				break;
			case '/':
				writeChar('\\', buf);
				writeChar('/', buf);
				break;
			case '"':
				writeChar('\\', buf);
				writeChar('"', buf);
				break;
			default:
				if (0x00 <= *it && *it <= 0x1F)
				{
					char higher_half = (*it) >> 4;
					char lower_half = (*it) & 0xF;

					writeCString("\\u00", buf);
					writeChar('0' + higher_half, buf);

					if (0 <= lower_half && lower_half <= 9)
						writeChar('0' + lower_half, buf);
					else
						writeChar('A' + lower_half - 10, buf);
				}
				else if (end - it >= 3 && it[0] == '\xE2' && it[1] == '\x80' && (it[2] == '\xA8' || it[2] == '\xA9'))
				{
					if (it[2] == '\xA8')
						writeCString("\\u2028", buf);
					if (it[2] == '\xA9')
						writeCString("\\u2029", buf);
				}
				else
					writeChar(*it, buf);
		}
	}
	writeChar('"', buf);
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


inline void writeJSONString(const String & s, WriteBuffer & buf)
{
	writeJSONString(s.data(), s.data() + s.size(), buf);
}


template <char c>
void writeAnyEscapedString(const String & s, WriteBuffer & buf)
{
	writeAnyEscapedString<c>(s.data(), s.data() + s.size(), buf);
}


inline void writeEscapedString(const String & s, WriteBuffer & buf)
{
	/// strpbrk в libc под Linux на процессорах с SSE 4.2 хорошо оптимизирована (этот if ускоряет код в 1.5 раза)
	if (nullptr == strpbrk(s.data(), "\b\f\n\r\t\'\\") && strlen(s.data()) == s.size())
		writeString(s, buf);
	else
		writeAnyEscapedString<'\''>(s, buf);
}


template <char c>
void writeAnyQuotedString(const char * begin, const char * end, WriteBuffer & buf)
{
	writeChar(c, buf);
	writeAnyEscapedString<c>(begin, end, buf);
	writeChar(c, buf);
}



template <char c>
void writeAnyQuotedString(const String & s, WriteBuffer & buf)
{
	writeAnyQuotedString<c>(s.data(), s.data() + s.size(), buf);
}


inline void writeQuotedString(const String & s, WriteBuffer & buf)
{
	writeAnyQuotedString<'\''>(s, buf);
}

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
inline void writeDateText(DayNum_t date, WriteBuffer & buf)
{
	char s[10] = {'0', '0', '0', '0', '-', '0', '0', '-', '0', '0'};

	if (unlikely(date > DATE_LUT_MAX_DAY_NUM || date == 0))
	{
		buf.write(s, 10);
		return;
	}

	const DateLUT::Values & values = DateLUT::instance().getValues(date);

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
inline void writeDateTimeText(time_t datetime, WriteBuffer & buf, char date_delimeter = '-', char time_delimeter = ':')
{
	char s[19] = {'0', '0', '0', '0', date_delimeter, '0', '0', date_delimeter, '0', '0', ' ', '0', '0', time_delimeter, '0', '0', time_delimeter, '0', '0'};

	if (unlikely(datetime > DATE_LUT_MAX || datetime == 0))
	{
		buf.write(s, 19);
		return;
	}

	DateLUT & date_lut = DateLUT::instance();
	const DateLUT::Values & values = date_lut.getValues(datetime);

	s[0] += values.year / 1000;
	s[1] += (values.year / 100) % 10;
	s[2] += (values.year / 10) % 10;
	s[3] += values.year % 10;
	s[5] += values.month / 10;
	s[6] += values.month % 10;
	s[8] += values.day_of_month / 10;
	s[9] += values.day_of_month % 10;

	UInt8 hour = date_lut.toHourInaccurate(datetime);
	UInt8 minute = date_lut.toMinuteInaccurate(datetime);
	UInt8 second = date_lut.toSecondInaccurate(datetime);

	s[11] += hour / 10;
	s[12] += hour % 10;
	s[14] += minute / 10;
	s[15] += minute % 10;
	s[17] += second / 10;
	s[18] += second % 10;

	buf.write(s, 19);
}

inline void writeDateTimeText(mysqlxx::DateTime datetime, WriteBuffer & buf, char date_delimeter = '-', char time_delimeter = ':')
{
	char s[19] = {'0', '0', '0', '0', date_delimeter, '0', '0', date_delimeter, '0', '0', ' ', '0', '0', time_delimeter, '0', '0', time_delimeter, '0', '0'};

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


/// Методы вывода в бинарном виде
inline void writeBinary(const UInt8 & x, 	WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const UInt16 & x, 	WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const UInt32 & x, 	WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const UInt64 & x, 	WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const Int8 & x, 	WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const Int16 & x, 	WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const Int32 & x, 	WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const Int64 & x, 	WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const Float32 & x, 	WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const Float64 & x, 	WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const String & x,	WriteBuffer & buf) { writeStringBinary(x, buf); }
inline void writeBinary(const bool & x, 	WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const uint128 & x, 	WriteBuffer & buf) { writePODBinary(x, buf); }

inline void writeBinary(const VisitID_t & x, 	WriteBuffer & buf) { writePODBinary(static_cast<const UInt64 &>(x), buf); }
inline void writeBinary(const mysqlxx::Date & x,		WriteBuffer & buf) { writePODBinary(x, buf); }
inline void writeBinary(const mysqlxx::DateTime & x,	WriteBuffer & buf) { writePODBinary(x, buf); }


/// Методы для вывода значения в текстовом виде для tab-separated формата.
inline void writeText(const UInt8 & x, 		WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeText(const UInt16 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeText(const UInt32 & x,		WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeText(const UInt64 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeText(const Int8 & x, 		WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeText(const Int16 & x, 		WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeText(const Int32 & x, 		WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeText(const Int64 & x, 		WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeText(const Float32 & x, 	WriteBuffer & buf) { writeFloatText(x, buf); }
inline void writeText(const Float64 & x, 	WriteBuffer & buf) { writeFloatText(x, buf); }
inline void writeText(const String & x,		WriteBuffer & buf) { writeEscapedString(x, buf); }
inline void writeText(const bool & x, 		WriteBuffer & buf) { writeBoolText(x, buf); }

inline void writeText(const VisitID_t & x, 	WriteBuffer & buf) { writeIntText(static_cast<const UInt64 &>(x), buf); }
inline void writeText(const mysqlxx::Date & x,		WriteBuffer & buf) { writeDateText(x, buf); }
inline void writeText(const mysqlxx::DateTime & x,	WriteBuffer & buf) { writeDateTimeText(x, buf); }

template<typename T>
inline void writeText(const mysqlxx::Null<T> & x,	WriteBuffer & buf)
{
	if (x.isNull())
		writeCString("\\N", buf);
	else
		writeText(static_cast<const T &>(x), buf);
}


/// Методы для вывода в текстовом виде в кавычках
inline void writeQuoted(const UInt8 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeQuoted(const UInt16 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeQuoted(const UInt32 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeQuoted(const UInt64 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeQuoted(const Int8 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeQuoted(const Int16 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeQuoted(const Int32 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeQuoted(const Int64 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeQuoted(const Float32 & x, 	WriteBuffer & buf) { writeFloatText(x, buf); }
inline void writeQuoted(const Float64 & x, 	WriteBuffer & buf) { writeFloatText(x, buf); }
inline void writeQuoted(const String & x,	WriteBuffer & buf) { writeQuotedString(x, buf); }
inline void writeQuoted(const bool & x, 	WriteBuffer & buf) { writeBoolText(x, buf); }

inline void writeQuoted(const VisitID_t & x, 	WriteBuffer & buf)
{
	writeIntText(static_cast<const UInt64 &>(x), buf);
}

inline void writeQuoted(const mysqlxx::Date & x,		WriteBuffer & buf)
{
	writeChar('\'', buf);
	writeDateText(x, buf);
	writeChar('\'', buf);
}

inline void writeQuoted(const mysqlxx::DateTime & x,	WriteBuffer & buf)
{
	writeChar('\'', buf);
	writeDateTimeText(x, buf);
	writeChar('\'', buf);
}

template <typename T>
inline void writeQuoted(const mysqlxx::Null<T> & x,		WriteBuffer & buf)
{
	if (x.isNull())
		writeCString("NULL", buf);
	else
		writeText(static_cast<const T &>(x), buf);
}


/// В двойных кавычках
inline void writeDoubleQuoted(const UInt8 & x, 		WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeDoubleQuoted(const UInt16 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeDoubleQuoted(const UInt32 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeDoubleQuoted(const UInt64 & x, 	WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeDoubleQuoted(const Int8 & x, 		WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeDoubleQuoted(const Int16 & x, 		WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeDoubleQuoted(const Int32 & x, 		WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeDoubleQuoted(const Int64 & x, 		WriteBuffer & buf) { writeIntText(x, buf); }
inline void writeDoubleQuoted(const Float32 & x, 	WriteBuffer & buf) { writeFloatText(x, buf); }
inline void writeDoubleQuoted(const Float64 & x, 	WriteBuffer & buf) { writeFloatText(x, buf); }
inline void writeDoubleQuoted(const String & x,		WriteBuffer & buf) { writeDoubleQuotedString(x, buf); }
inline void writeDoubleQuoted(const bool & x, 		WriteBuffer & buf) { writeBoolText(x, buf); }

inline void writeDoubleQuoted(const VisitID_t & x, 	WriteBuffer & buf)
{
	writeIntText(static_cast<const UInt64 &>(x), buf);
}

inline void writeDoubleQuoted(const mysqlxx::Date & x,		WriteBuffer & buf)
{
	writeChar('"', buf);
	writeDateText(x, buf);
	writeChar('"', buf);
}

inline void writeDoubleQuoted(const mysqlxx::DateTime & x,	WriteBuffer & buf)
{
	writeChar('"', buf);
	writeDateTimeText(x, buf);
	writeChar('"', buf);
}


template <typename T>
void writeBinary(const std::vector<T> & x, WriteBuffer & buf)
{
	size_t size = x.size();
	writeVarUInt(size, buf);
	for (size_t i = 0; i < size; ++i)
		writeBinary(x[i], buf);
}

template <typename T>
void writeQuoted(const std::vector<T> & x, WriteBuffer & buf)
{
	writeChar('[', buf);
	for (size_t i = 0, size = x.size(); i < size; ++i)
	{
		if (i != 0)
			writeChar(',', buf);
		writeQuoted(x[i], buf);
	}
	writeChar(']', buf);
}

template <typename T>
void writeDoubleQuoted(const std::vector<T> & x, WriteBuffer & buf)
{
	writeChar('[', buf);
	for (size_t i = 0, size = x.size(); i < size; ++i)
	{
		if (i != 0)
			writeChar(',', buf);
		writeDoubleQuoted(x[i], buf);
	}
	writeChar(']', buf);
}

template <typename T>
void writeText(const std::vector<T> & x, WriteBuffer & buf)
{
	writeQuoted(x, buf);
}



/// Сериализация эксепшена (чтобы его можно было передать по сети)
void writeException(const Exception & e, WriteBuffer & buf);


/// Простой для использования метод преобразования чего-либо в строку в текстовом виде.
template <typename T>
inline String toString(const T & x)
{
	String res;
	{
		WriteBufferFromString buf(res);
		writeText(x, buf);
	}
	return res;
}

}
