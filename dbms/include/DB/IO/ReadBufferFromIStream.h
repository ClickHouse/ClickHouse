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

public:
	ReadBufferFromIStream(std::istream & istr_) : istr(istr_) {}

	bool next()
	{
		istr.read(internal_buffer, DEFAULT_READ_BUFFER_SIZE);

		pos = internal_buffer;
		working_buffer = Buffer(internal_buffer, internal_buffer + istr.gcount());

		if (working_buffer.end() == working_buffer.begin())
		{
			if (istr.eof())
				return false;
			if (!istr.good())
				throw Exception("Cannot read from istream", ErrorCodes::CANNOT_READ_FROM_ISTREAM);
		}
		
		return true;
	}
};

}

#endif
