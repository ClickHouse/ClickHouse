#pragma once

#include <algorithm>
#include <cstring>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>
#include <DB/IO/BufferBase.h>


namespace DB
{

/** Простой абстрактный класс для буферизованной записи данных (последовательности char) куда-нибудь.
  * В отличие от std::ostream, предоставляет доступ к внутреннему буферу,
  *  а также позволяет вручную управлять позицией внутри буфера.
  *
  * Наследники должны реализовать метод nextImpl().
  */
class WriteBuffer : public BufferBase
{
public:
    WriteBuffer(Position ptr, size_t size) : BufferBase(ptr, size, 0) {}
    void set(Position ptr, size_t size) { BufferBase::set(ptr, size, 0); }

	/** записать данные, находящиеся в буфере (от начала буфера до текущей позиции);
	  * переместить позицию в начало; кинуть исключение, если что-то не так
	  */
	inline void next()
	{
		if (!offset())
			return;
		bytes += offset();

		try
		{
			nextImpl();
		}
		catch (...)
		{
			/** Если вызов nextImpl() был неудачным, то переместим курсор в начало,
			  * чтобы потом (например, при развёртке стека) не было второй попытки записать данные.
			  */
			pos = working_buffer.begin();
			throw;
		}

		pos = working_buffer.begin();
	}

	/** желательно в наследниках поместить в деструктор вызов next(),
	  * чтобы последние данные записались
	  */
	virtual ~WriteBuffer() {}


	inline void nextIfAtEnd()
	{
		if (!hasPendingData())
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


	inline void write(char x)
	{
		nextIfAtEnd();
		*pos = x;
		++pos;
	}

private:
	/** Записать данные, находящиеся в буфере (от начала буфера до текущей позиции).
	  * Кинуть исключение, если что-то не так.
	  */
	virtual void nextImpl() { throw Exception("Cannot write after end of buffer.", ErrorCodes::CANNOT_WRITE_AFTER_END_OF_BUFFER); };
};


}
