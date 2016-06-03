#pragma once

#include <vector>

#include <DB/IO/ReadBuffer.h>


namespace DB
{

/** Читает из конкатенации нескольких ReadBuffer-ов
  */
class ConcatReadBuffer : public ReadBuffer
{
public:
	typedef std::vector<ReadBuffer *> ReadBuffers;

protected:
	ReadBuffers buffers;
	ReadBuffers::iterator current;

	bool nextImpl()
	{
		if (buffers.end() == current)
			return false;

		/// Первое чтение
		if (working_buffer.size() == 0 && (*current)->hasPendingData())
		{
			working_buffer = Buffer((*current)->position(), (*current)->buffer().end());
			return true;
		}

		if (!(*current)->next())
		{
			++current;
			if (buffers.end() == current)
				return false;

			/// Пропускаем закончившиеся буферы; если буфер не закончился, но курсор на конце, то прочитаем следующую порцию данных.
			while ((*current)->eof())
			{
				++current;
				if (buffers.end() == current)
					return false;
			}
		}

		working_buffer = Buffer((*current)->position(), (*current)->buffer().end());
		return true;
	}

public:
	ConcatReadBuffer(const ReadBuffers & buffers_) : ReadBuffer(nullptr, 0), buffers(buffers_), current(buffers.begin()) {}

	ConcatReadBuffer(ReadBuffer & buf1, ReadBuffer & buf2) : ReadBuffer(nullptr, 0)
	{
		buffers.push_back(&buf1);
		buffers.push_back(&buf2);
		current = buffers.begin();
	}
};

}
