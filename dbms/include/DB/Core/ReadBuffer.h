#ifndef DBMS_COMMON_READBUFFER_H
#define DBMS_COMMON_READBUFFER_H

#include <string.h>		// memcpy

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


	/// Функции для чтения конкретных данных

	inline bool eof()
	{
		return pos == working_buffer.end() && !next();
	}


	void readChar(char & x)
	{
		x = 0;
		if (!eof())
		{
			x = *pos;
			++pos;
		}
	}


	void ignore()
	{
		if (!eof())
			++pos;
	}
	

	/// грубо
	template <typename T>
	void readIntText(T & x)
	{
		x = 0;
		while (!eof())
		{
			switch (*pos)
			{
				case '+':
					break;
				case '-':
					x = -x;
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
					return;
			}
			++pos;
		}
	}

	/// грубо; поддерживается только простой формат
	template <typename T>
	void readFloatText(T & x)
	{
		x = 0;
		bool after_point = false;
		double power_of_ten = 1;
		
		while (!eof())
		{
			switch (*pos)
			{
				case '+':
					break;
				case '-':
					x = -x;
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
						x += (*pos - '0') * power_of_ten;
					}
					else
					{
						x *= 10;
						x += *pos - '0';
					}
					break;
				default:
					return;
			}
			++pos;
		}
	}

	/// грубо; всё до '\n' или '\t'
	void readString(String & s)
	{
		s = "";
		while (!eof())
		{
			size_t bytes = 0;
			for (; pos + bytes != working_buffer.end(); ++bytes)
				if (pos[bytes] == '\t' || pos[bytes] == '\n')
					break;
				
			s.append(pos, bytes);
			pos += bytes;

			if (pos != working_buffer.end())
				return;
		}
	}

	void readEscapedString(String & s)
	{
		s = "";
		while (!eof())
		{
			size_t bytes = 0;
			for (; pos + bytes != working_buffer.end(); ++bytes)
				if (pos[bytes] == '\\' || pos[bytes] == '\t' || pos[bytes] == '\n')
					break;
	
			s.append(pos, bytes);
			pos += bytes;

			if (*pos == '\t' || *pos == '\n')
				return;

			if (*pos == '\\')
			{
				++pos;
				if (eof())
					throw Exception("Cannot parse escape sequence", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);
				s += parseEscapeSequence(*pos);
				++pos;
			}
		}
	}

	void readQuotedString(String & s)
	{
		s = "";

		if (eof() || *pos != '\'')
			throw Exception("Cannot parse quoted string: expected opening single quote",
				ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
		++pos;
		
		while (!eof())
		{
			size_t bytes = 0;
			for (; pos + bytes != working_buffer.end(); ++bytes)
				if (pos[bytes] == '\\' || pos[bytes] == '\'')
					break;
	
			s.append(pos, bytes);
			pos += bytes;

			if (*pos == '\'')
			{
				++pos;
				return;
			}

			if (*pos == '\\')
			{
				++pos;
				if (eof())
					throw Exception("Cannot parse escape sequence", ErrorCodes::CANNOT_PARSE_ESCAPE_SEQUENCE);
				s += parseEscapeSequence(*pos);
				++pos;
			}
		}

		throw Exception("Cannot parse quoted string: expected closing single quote",
			ErrorCodes::CANNOT_PARSE_QUOTED_STRING);
	}


	size_t read(char * to, size_t n)
	{
		size_t bytes_copied = 0;

		while (!eof() && bytes_copied < n)
		{
			size_t bytes_to_copy = std::min(static_cast<size_t>(working_buffer.end() - pos), n - bytes_copied);
			memcpy(to, pos, bytes_to_copy);
			pos += bytes_to_copy;
		}

		return bytes_copied;
	}


	

protected:
	char internal_buffer[DEFAULT_READ_BUFFER_SIZE];
	Buffer working_buffer;
	Position pos;

private:
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
			default:
				return c;
		}
	}
};


}

#endif
