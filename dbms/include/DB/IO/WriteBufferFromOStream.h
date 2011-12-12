#pragma once

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
	WriteBufferFromOStream(std::ostream & ostr_, size_t size = DBMS_DEFAULT_BUFFER_SIZE)
		: BufferWithOwnMemory<WriteBuffer>(size), ostr(ostr_) {}

	~WriteBufferFromOStream()
	{
		bool uncaught_exception = std::uncaught_exception();

		try
		{
			nextImpl();
		}
		catch (...)
		{
			/// Если до этого уже было какое-то исключение, то второе исключение проигнорируем.
			if (!uncaught_exception)
				throw;
		}
	}
};

}
