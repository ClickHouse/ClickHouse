#pragma once

#include <DB/IO/IWriteFileOperations.h>
#include <DB/IO/BufferWithOwnMemory.h>
#include <DB/IO/WriteBuffer.h>

namespace DB
{

class WriteBufferFromFileBase : public IWriteFileOperations, public BufferWithOwnMemory<WriteBuffer>
{
public:
	WriteBufferFromFileBase(size_t buf_size, char * existing_memory, size_t alignment)
	: BufferWithOwnMemory<WriteBuffer>(buf_size, existing_memory, alignment) {}
	virtual ~WriteBufferFromFileBase() = default;
};

}
