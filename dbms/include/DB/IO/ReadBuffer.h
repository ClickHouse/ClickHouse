#ifndef DBMS_COMMON_READBUFFER_H
#define DBMS_COMMON_READBUFFER_H

#include <vector>
#include <cstring>
#include <algorithm>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#define DEFAULT_READ_BUFFER_SIZE 1048576UL


namespace DB
{

/** Простой абстрактный класс для буферизованного чтения данных (последовательности char) откуда-нибудь.
  * В отличие от std::istream, предоставляет доступ к внутреннему буферу,
  *  а также позволяет вручную управлять позицией внутри буфера.
  *
  * Наследники должны реализовать метод nextImpl().
  */
class ReadBuffer
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

	ReadBuffer()
		: internal_buffer(DEFAULT_READ_BUFFER_SIZE),
		working_buffer(&internal_buffer[0], &internal_buffer[0]),
		pos(&internal_buffer[0]),
		bytes_read(0)
	{}

	/// получить часть буфера, из которого можно читать данные
	inline Buffer & buffer() { return working_buffer; }
	
	/// получить (для чтения и изменения) позицию в буфере
	inline Position & position() { return pos; };

	/** прочитать следующие данные и заполнить ими буфер; переместить позицию в начало;
	  * вернуть false в случае конца, true иначе; кинуть исключение, если что-то не так
	  */
	inline bool next()
	{
		bytes_read += pos - working_buffer.begin();
		bool res = nextImpl();
		pos = working_buffer.begin();
		return res;
	}

	virtual ~ReadBuffer() {}


	/** В отличие от std::istream, возвращает true, если все данные были прочитаны
	  *  (а не в случае, если была попытка чтения после конца).
	  * Если на данный момент позиция находится на конце буфера, то вызывает метод next().
	  * То есть, имеет побочный эффект - если буфер закончился, то обновляет его и переносит позицию в начало.
	  *
	  * При попытке чтения после конца, следует кидать исключение.
	  */
	inline bool eof()
	{
		return pos == working_buffer.end() && !next();
	}

	void ignore()
	{
		if (!eof())
			++pos;
		else
			throw Exception("Attempt to read after eof", ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
	}

	void ignore(size_t n)
	{
		while (!eof() && n != 0)
		{
			size_t bytes_to_ignore = std::min(static_cast<size_t>(working_buffer.end() - pos), n);
			pos += bytes_to_ignore;
			n -= bytes_to_ignore;
		}

		if (n)
			throw Exception("Attempt to read after eof", ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF);
	}

	/** Читает столько, сколько есть, не больше n байт. */
	size_t read(char * to, size_t n)
	{
		size_t bytes_copied = 0;

		while (!eof() && bytes_copied < n)
		{
			size_t bytes_to_copy = std::min(static_cast<size_t>(working_buffer.end() - pos), n - bytes_copied);
			std::memcpy(to + bytes_copied, pos, bytes_to_copy);
			pos += bytes_to_copy;
			bytes_copied += bytes_to_copy;
		}

		return bytes_copied;
	}

	/** Читает n байт, если есть меньше - кидает исключение. */
	void readStrict(char * to, size_t n)
	{
		if (n != read(to, n))
			throw Exception("Cannot read all data", ErrorCodes::CANNOT_READ_ALL_DATA);
	}


	/** Сколько байт было прочитано из буфера. */
	size_t count()
	{
		return bytes_read + pos - working_buffer.begin();
	}

protected:
	std::vector<char> internal_buffer;
	Buffer working_buffer;
	Position pos;

private:
	size_t bytes_read;


	/** Прочитать следующие данные и заполнить ими буфер.
	  * Вернуть false в случае конца, true иначе.
	  * Кинуть исключение, если что-то не так.
	  */
	virtual bool nextImpl() { return false; };
};


}

#endif
