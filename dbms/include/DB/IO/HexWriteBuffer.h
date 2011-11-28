#pragma once

#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/IO/WriteBuffer.h>


namespace DB
{

/** Всё что в него пишут, переводит в HEX (большими буквами) и пишет в другой WriteBuffer.
  */
class HexWriteBuffer : public BufferWithOwnMemory<WriteBuffer>
{
protected:
	WriteBuffer & out;
	
	void nextImpl()
	{
		if (!offset())
			return;

		for (Position pos = working_buffer.begin(); pos != working_buffer.end(); ++pos)
		{
			out.write("0123456789ABCDEF"[static_cast<unsigned char>(*pos) >> 4]);
			out.write("0123456789ABCDEF"[static_cast<unsigned char>(*pos) & 0xF]);
		}
	}

public:
	HexWriteBuffer(WriteBuffer & out_) : out(out_) {}

    virtual ~HexWriteBuffer()
	{
		nextImpl();
	}
};

}
