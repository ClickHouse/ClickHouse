#pragma once

#include <DB/IO/CompressedReadBufferBase.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/IO/ReadBuffer.h>


namespace DB
{

class CompressedReadBuffer : public CompressedReadBufferBase, public BufferWithOwnMemory<ReadBuffer>
{
private:
	size_t size_compressed = 0;

	bool nextImpl() override;

public:
	CompressedReadBuffer(ReadBuffer & in_)
		: CompressedReadBufferBase(&in_), BufferWithOwnMemory<ReadBuffer>(0)
	{
	}

	size_t readBig(char * to, size_t n) override;

	/// Сжатый размер текущего блока.
	size_t getSizeCompressed() const
	{
		return size_compressed;
	}
};

}
