#ifndef DBMS_COMMON_WRITEBUFFER_H
#define DBMS_COMMON_WRITEBUFFER_H

#include <cstring>
#include <algorithm>

#define DEFAULT_WRITE_BUFFER_SIZE 1048576U


namespace DB
{

/** Простой абстрактный класс для буферизованной записи данных (последовательности char) куда-нибудь.
  * В отличие от std::ostream, предоставляет доступ к внутреннему буферу,
  *  а также позволяет вручную управлять позицией внутри буфера.
  *
  * Наследники должны реализовать метод next().
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

	/** желательно в наследниках поместить в деструктор вызов next(),
	  * чтобы последние данные записались
	  */
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


}

#endif
