#pragma once

#include <DB/IO/ReadBuffer.h>


namespace DB
{

/** Читает из конкатенации нескольких ReadBuffer-ов
  */
class ConcatReadBuffer : public ReadBuffer
{
protected:
	typedef std::vector<ReadBuffer *> ReadBuffers;
	ReadBuffers buffers;
	ReadBuffers::iterator current;
	
	bool nextImpl()
	{
		if (!(*current)->next())
		{
			++current;
			if (buffers.end() == current)
				return false;
		}
		
		internal_buffer = (*current)->internal_buffer;
		working_buffer = (*current)->working_buffer;

		return true;
	}

public:
	ConcatReadBuffer(const ReadBuffers & buffers_) : buffers(buffers_), current(buffers.begin()) {}

	ConcatReadBuffer(ReadBuffer & buf1, ReadBuffer & buf2)
	{
		buffers.push_bask(&buf1);
		buffers.push_bask(&buf2);
		current = buffers.begin();
	}
};

}
