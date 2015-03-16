#pragma once

#include <DB/IO/IReadFileOperations.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/IO/ReadBuffer.h>

namespace DB
{

class ReadBufferFromFileBase : public IReadFileOperations, public BufferWithOwnMemory<ReadBuffer>
{
public:
	ReadBufferFromFileBase(size_t buf_size, char * existing_memory, size_t alignment)
	: BufferWithOwnMemory<ReadBuffer>(buf_size, existing_memory, alignment) {}
	virtual ~ReadBufferFromFileBase() = default;
};

}
