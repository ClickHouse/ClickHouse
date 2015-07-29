#pragma once

#include <cstring>
#include <algorithm>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/IO/BufferBase.h>


namespace DB
{

/** Простой абстрактный класс для буферизованного чтения данных (последовательности char) откуда-нибудь.
  * В отличие от std::istream, предоставляет доступ к внутреннему буферу,
  *  а также позволяет вручную управлять позицией внутри буфера.
  *
  * Замечание! Используется char *, а не const char *
  *  (для того, чтобы можно было вынести общий код в BufferBase, а также для того, чтобы можно было заполнять буфер новыми данными).
  * Это вызывает неудобства - например, при использовании ReadBuffer для чтения из куска памяти const char *,
  *  приходится использовать const_cast.
  *
  * Наследники должны реализовать метод nextImpl().
  */
class ReadBuffer : public BufferBase
{
public:
	/** Создаёт буфер и устанавливает кусок доступных данных для чтения нулевого размера,
	  *  чтобы при первой попытке чтения вызвалась функция next() для загрузки в буфер новой порции данных.
	  */
	ReadBuffer(Position ptr, size_t size) : BufferBase(ptr, size, 0) { working_buffer.resize(0); }

	/** Используется, если буфер уже заполнен данными, которые можно читать.
	  *  (в этом случае, передайте 0 в качестве offset)
	  */
	ReadBuffer(Position ptr, size_t size, size_t offset) : BufferBase(ptr, size, offset) {}

	void set(Position ptr, size_t size) { BufferBase::set(ptr, size, 0); working_buffer.resize(0); }

	/** прочитать следующие данные и заполнить ими буфер; переместить позицию в начало;
	  * вернуть false в случае конца, true иначе; кинуть исключение, если что-то не так
	  */
	bool next()
	{
		bytes += offset();
		bool res = nextImpl();
		if (!res)
			working_buffer.resize(0);

		pos = working_buffer.begin() + working_buffer_offset;
		working_buffer_offset = 0;
		return res;
	}


	inline void nextIfAtEnd()
	{
		if (!hasPendingData())
			next();
	}

	virtual ~ReadBuffer() {}


	/** В отличие от std::istream, возвращает true, если все данные были прочитаны
	  *  (а не в случае, если была попытка чтения после конца).
	  * Если на данный момент позиция находится на конце буфера, то вызывает метод next().
	  * То есть, имеет побочный эффект - если буфер закончился, то обновляет его и переносит позицию в начало.
	  *
	  * При попытке чтения после конца, следует кидать исключение.
	  */
	bool eof()
	{
		return !hasPendingData() && !next();
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

	/// Можно было бы назвать этот метод ignore, а ignore назвать ignoreStrict.
	size_t tryIgnore(size_t n)
	{
		size_t bytes_ignored = 0;

		while (bytes_ignored < n && !eof())
		{
			size_t bytes_to_ignore = std::min(static_cast<size_t>(working_buffer.end() - pos), n - bytes_ignored);
			pos += bytes_to_ignore;
			bytes_ignored += bytes_to_ignore;
		}

		return bytes_ignored;
	}

	/** Читает столько, сколько есть, не больше n байт. */
	size_t read(char * to, size_t n)
	{
		size_t bytes_copied = 0;

		while (bytes_copied < n && !eof())
		{
			size_t bytes_to_copy = std::min(static_cast<size_t>(working_buffer.end() - pos), n - bytes_copied);
			::memcpy(to + bytes_copied, pos, bytes_to_copy);
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

	/** Метод, который может быть более эффективно реализован в наследниках, в случае чтения достаточно больших блоков.
	  * Реализация может читать данные сразу в to, без лишнего копирования, если в to есть достаточно места для работы.
	  * Например, CompressedReadBuffer может разжимать данные сразу в to, если весь разжатый блок туда помещается.
	  * По-умолчанию - то же, что и read.
	  * Для маленьких чтений использовать не нужно.
	  */
	virtual size_t readBig(char * to, size_t n)
	{
		return read(to, n);
	}

protected:
	/// Количество игнорируемых байт с начальной позиции буфера working_buffer.
	size_t working_buffer_offset = 0;

private:
	/** Прочитать следующие данные и заполнить ими буфер.
	  * Вернуть false в случае конца, true иначе.
	  * Кинуть исключение, если что-то не так.
	  */
	virtual bool nextImpl() { return false; };
};


}
