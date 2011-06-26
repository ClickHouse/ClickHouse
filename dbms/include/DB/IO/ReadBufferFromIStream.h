#ifndef DBMS_COMMON_READBUFFER_FROM_ISTREAM_H
#define DBMS_COMMON_READBUFFER_FROM_ISTREAM_H

#include <iostream>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadBuffer.h>


namespace DB
{

class ReadBufferFromIStream : public ReadBuffer
{
private:
	std::istream & istr;

	bool nextImpl()
	{
		istr.read(working_buffer.begin(), DEFAULT_READ_BUFFER_SIZE);
		size_t gcount = istr.gcount();

		if (!gcount)
		{
			if (istr.eof())
				return false;
			else
				throw Exception("Cannot read from istream", ErrorCodes::CANNOT_READ_FROM_ISTREAM);
		}
		else
			working_buffer = Buffer(working_buffer.begin(), working_buffer.begin() + gcount);

		return true;
	}

public:
	ReadBufferFromIStream(std::istream & istr_) : istr(istr_) {}
};

}

#endif
