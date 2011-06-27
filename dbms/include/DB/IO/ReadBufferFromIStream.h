#ifndef DBMS_COMMON_READBUFFER_FROM_ISTREAM_H
#define DBMS_COMMON_READBUFFER_FROM_ISTREAM_H

#include <iostream>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>


namespace DB
{

class ReadBufferFromIStream : public BufferWithOwnMemory<ReadBuffer>
{
private:
	std::istream & istr;

	bool nextImpl()
	{
		istr.read(working_buffer.begin(), working_buffer.size());
		size_t gcount = istr.gcount();

		if (!gcount)
		{
			if (istr.eof())
				return false;
			else
				throw Exception("Cannot read from istream", ErrorCodes::CANNOT_READ_FROM_ISTREAM);
		}
		else
			working_buffer.resize(gcount);

		return true;
	}

public:
	ReadBufferFromIStream(std::istream & istr_) : istr(istr_) {}
};

}

#endif
