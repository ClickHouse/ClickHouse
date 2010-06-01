#ifndef DBMS_COMMON_WRITEBUFFER_H
#define DBMS_COMMON_WRITEBUFFER_H

#include <cstring>
#include <cstdio>
#include <limits>
#include <algorithm>

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#define DEFAULT_WRITE_BUFFER_SIZE 1048576
#define DEFAULT_FLOAT_PRECISION 6
/// 20 цифр, знак, и \0 для конца строки
#define MAX_INT_WIDTH 22


namespace DB
{

/** Простой абстрактный класс для буферизованной записи данных (последовательности char) куда-нибудь.
  * В отличие от std::ostream, предоставляет доступ к внутреннему буферу,
  *  а также позволяет вручную управлять позицией внутри буфера.
  *
  * Наследники должны реализовать метод next().
  *
  * Также предоставляет набор функций для форматированной и неформатированной записи.
  * (с простой и грубой реализацией)
  */
class WriteBuffer
{
public:
	typedef char * Position;

	struct Buffer
	{
		Buffer(Position begin_pos_, Position end_pos_) : begin_pos(begin_pos_), end_pos(end_pos_) {}

		inline Position begin() { return begin_pos; }
		inline Position end() { return end_pos; }

	private:
		Position begin_pos;
		Position end_pos;		/// на 1 байт после конца буфера
	};

	WriteBuffer() : working_buffer(internal_buffer, internal_buffer + DEFAULT_WRITE_BUFFER_SIZE), pos(internal_buffer) {}

	/// получить часть буфера, в который можно писать данные
	inline Buffer & buffer() { return working_buffer; }
	
	/// получить (для чтения и изменения) позицию в буфере
	inline Position & position() { return pos; };

	/** записать данные, находящиеся в буфере (от начала буфера до текущей позиции);
	  * переместить позицию в начало; кинуть исключение, если что-то не так
	  */
	virtual void next() {}

	virtual ~WriteBuffer() {}


	inline void nextIfAtEnd()
	{
		if (pos == working_buffer.end())
			next();
	}

	
	void write(const char * from, size_t n)
	{
		size_t bytes_copied = 0;

		while (bytes_copied < n)
		{
			nextIfAtEnd();
			size_t bytes_to_copy = std::min(static_cast<size_t>(working_buffer.end() - pos), n - bytes_copied);
			std::memcpy(pos, from + bytes_copied, bytes_to_copy);
			pos += bytes_to_copy;
			bytes_copied += bytes_to_copy;
		}
	}

protected:
	char internal_buffer[DEFAULT_WRITE_BUFFER_SIZE];
	Buffer working_buffer;
	Position pos;
};


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
	char tmp[MAX_INT_WIDTH];
	int res = std::snprintf(tmp, MAX_INT_WIDTH, IntFormat<T>::format, x);

	if (res >= MAX_INT_WIDTH || res <= 0)
		throw Exception("Cannot print integer", ErrorCodes::CANNOT_PRINT_INTEGER);

	buf.write(tmp, res - 1);
}

template <typename T>
void writeFloatText(T x, WriteBuffer & buf, unsigned precision = DEFAULT_FLOAT_PRECISION)
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
