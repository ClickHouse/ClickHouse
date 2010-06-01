#ifndef DBMS_COMMON_READBUFFER_H
#define DBMS_COMMON_READBUFFER_H

#include <cstring>
#include <algorithm>

#include <DB/Core/Types.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#define DEFAULT_READ_BUFFER_SIZE 1048576


namespace DB
{

/** Простой абстрактный класс для буферизованного чтения данных (последовательности char) откуда-нибудь.
  * В отличие от std::istream, предоставляет доступ к внутреннему буферу,
  *  а также позволяет вручную управлять позицией внутри буфера.
  *
  * Наследники должны реализовать метод next().
  *
  * Также предоставляет набор функций для форматированного и неформатированного чтения.
  * (с простой и грубой реализацией)
  */
class ReadBuffer
{
public:
	typedef const char * Position;

	struct Buffer
	{
		Buffer(Position begin_pos_, Position end_pos_) : begin_pos(begin_pos_), end_pos(end_pos_) {}

		inline Position begin() { return begin_pos; }
		inline Position end() { return end_pos; }

	private:
		Position begin_pos;
		Position end_pos;		/// на 1 байт после конца буфера
	};

	ReadBuffer() : working_buffer(internal_buffer, internal_buffer), pos(internal_buffer) {}

	/// получить часть буфера, из которого можно читать данные
	inline Buffer & buffer() { return working_buffer; }
	
	/// получить (для чтения и изменения) позицию в буфере
	inline Position & position() { return pos; };

	/** прочитать следующие данные и заполнить ими буфер; переместить позицию в начало;
	  * вернуть false в случае конца, true иначе; кинуть исключение, если что-то не так
	  */
	virtual bool next() { return false; }

	virtual ~ReadBuffer() {}


	inline bool eof()
	{
		return pos == working_buffer.end() && !next();
	}

	void ignore()
	{
		if (!eof())
			++pos;
	}

	size_t read(char * to, size_t n)
	{
		size_t bytes_copied = 0;

		while (!eof() && bytes_copied < n)
		{
			size_t bytes_to_copy = std::min(static_cast<size_t>(working_buffer.end() - pos), n - bytes_copied);
			std::memcpy(to, pos, bytes_to_copy);
			pos += bytes_to_copy;
		}

		return bytes_copied;
	}

protected:
	char internal_buffer[DEFAULT_READ_BUFFER_SIZE];
	Buffer working_buffer;
	Position pos;
};



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

static void assertString(const char * s, ReadBuffer & buf)
{
	for (; *s; ++s)
	{
		if (buf.eof() || *buf.position() != *s)
			throw Exception(String("Cannot parse input: expected ") + s, ErrorCodes::CANNOT_PARSE_INPUT_ASSERTION_FAILED);
		++buf.position();
	}
}


/// грубо
template <typename T>
void readIntText(T & x, ReadBuffer & buf)
{
	bool negative = false;
	x = 0;
	while (!buf.eof())
	{
		switch (*buf.position())
		{
			case '+':
				break;
			case '-':
				negative = true;
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

/// грубо; всё до '\n' или '\t'
void readString(String & s, ReadBuffer & buf)
{
	s = "";
	while (!buf.eof())
	{
		size_t bytes = 0;
		for (; buf.position() + bytes != buf.buffer().end(); ++bytes)
			if (buf.position()[bytes] == '\t' || buf.position()[bytes] == '\n')
				break;

		s.append(buf.position(), bytes);
		buf.position() += bytes;

		if (buf.position() != buf.buffer().end())
			return;
	}
}

void readEscapedString(String & s, ReadBuffer & buf)
{
	s = "";
	while (!buf.eof())
	{
		size_t bytes = 0;
		for (; buf.position() + bytes != buf.buffer().end(); ++bytes)
			if (buf.position()[bytes] == '\\' || buf.position()[bytes] == '\t' || buf.position()[bytes] == '\n')
				break;

		s.append(buf.position(), bytes);
		buf.position() += bytes;

		if (*buf.position() == '\t' || *buf.position() == '\n')
			return;

		if (*buf.position() == '\\')
		{
			++buf.position();
			if (buf.eof())
				throw Exception("Cannot parse escape sequence", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);
			s += parseEscapeSequence(*buf.position());
			++buf.position();
		}
	}
}

void readQuotedString(String & s, ReadBuffer & buf)
{
	s = "";

	if (buf.eof() || *buf.position() != '\'')
		throw Exception("Cannot parse quoted string: expected opening single quote",
			ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
	++buf.position();

	while (!buf.eof())
	{
		size_t bytes = 0;
		for (; buf.position() + bytes != buf.buffer().end(); ++bytes)
			if (buf.position()[bytes] == '\\' || buf.position()[bytes] == '\'')
				break;

		s.append(buf.position(), bytes);
		buf.position() += bytes;

		if (*buf.position() == '\'')
		{
			++buf.position();
			return;
		}

		if (*buf.position() == '\\')
		{
			++buf.position();
			if (buf.eof())
				throw Exception("Cannot parse escape sequence", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);
			s += parseEscapeSequence(*buf.position());
			++buf.position();
		}
	}

	throw Exception("Cannot parse quoted string: expected closing single quote",
		ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
}


}

#endif
