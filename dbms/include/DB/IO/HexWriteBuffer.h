#pragma once

#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/IO/WriteBuffer.h>


/// Так как HexWriteBuffer часто создаётся во внутреннем цикле, сделаем у него размер буфера маленьким.
#define DBMS_HEX_WRITE_BUFFER_SIZE 32


namespace DB
{

/** Всё что в него пишут, переводит в HEX (большими буквами) и пишет в другой WriteBuffer.
  */
class HexWriteBuffer : public WriteBuffer
{
protected:
	char buf[DBMS_HEX_WRITE_BUFFER_SIZE];
	
	WriteBuffer & out;
	
	void nextImpl()
	{
		if (!offset())
			return;

		for (Position p = working_buffer.begin(); p != pos; ++p)
		{
			out.write("0123456789ABCDEF"[static_cast<unsigned char>(*p) >> 4]);
			out.write("0123456789ABCDEF"[static_cast<unsigned char>(*p) & 0xF]);
		}
	}

public:
	HexWriteBuffer(WriteBuffer & out_) : WriteBuffer(buf, sizeof(buf)), out(out_) {}

    ~HexWriteBuffer()
	{
		try
		{
			nextImpl();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}
	}
};

}
