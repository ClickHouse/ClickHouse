#pragma once

#include <iostream>

#include <DB/Common/Exception.h>
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
		istr.read(internal_buffer.begin(), internal_buffer.size());
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
	ReadBufferFromIStream(std::istream & istr_, size_t size = DBMS_DEFAULT_BUFFER_SIZE)
		: BufferWithOwnMemory<ReadBuffer>(size), istr(istr_) {}
};

}
