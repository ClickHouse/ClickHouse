#ifndef DBMS_COMMON_WRITEBUFFER_FROM_OSTREAM_H
#define DBMS_COMMON_WRITEBUFFER_FROM_OSTREAM_H

#include <iostream>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/WriteBuffer.h>


namespace DB
{

class WriteBufferFromOStream : public WriteBuffer
{
private:
	std::ostream & ostr;

public:
	WriteBufferFromOStream(std::ostream & ostr_) : ostr(ostr_) {}

	void nextImpl()
	{
		ostr.write(working_buffer.begin(), pos - working_buffer.begin());
		ostr.flush();

		if (!ostr.good())
			throw Exception("Cannot write to ostream", ErrorCodes::CANNOT_WRITE_TO_OSTREAM);
	}

	~WriteBufferFromOStream()
	{
		next();
	}
};

}

#endif
