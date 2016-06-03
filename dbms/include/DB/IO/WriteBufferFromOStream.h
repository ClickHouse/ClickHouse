#pragma once

#include <iostream>

#include <DB/Common/Exception.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int CANNOT_WRITE_TO_OSTREAM;
}

class WriteBufferFromOStream : public BufferWithOwnMemory<WriteBuffer>
{
private:
	std::ostream & ostr;

	void nextImpl()
	{
		if (!offset())
			return;
		
		ostr.write(working_buffer.begin(), offset());
		ostr.flush();

		if (!ostr.good())
			throw Exception("Cannot write to ostream", ErrorCodes::CANNOT_WRITE_TO_OSTREAM);
	}

public:
	WriteBufferFromOStream(std::ostream & ostr_, size_t size = DBMS_DEFAULT_BUFFER_SIZE)
		: BufferWithOwnMemory<WriteBuffer>(size), ostr(ostr_) {}

	~WriteBufferFromOStream()
	{
		try
		{
			next();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}
	}
};

}
