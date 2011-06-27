#ifndef DBMS_COMMON_WRITEBUFFER_FROM_OSTREAM_H
#define DBMS_COMMON_WRITEBUFFER_FROM_OSTREAM_H

#include <iostream>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>


namespace DB
{

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
	WriteBufferFromOStream(std::ostream & ostr_) : ostr(ostr_) {}

	~WriteBufferFromOStream()
	{
		nextImpl();
	}
};

}

#endif
