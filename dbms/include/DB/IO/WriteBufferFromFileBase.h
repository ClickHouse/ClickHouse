#pragma once

#include <string>
#include <fcntl.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>

namespace DB
{

class WriteBufferFromFileBase : public BufferWithOwnMemory<WriteBuffer>
{
public:
	WriteBufferFromFileBase(size_t buf_size, char * existing_memory, size_t alignment)
	: BufferWithOwnMemory<WriteBuffer>(buf_size, existing_memory, alignment)
	{
	}

	virtual ~WriteBufferFromFileBase() {}
	virtual off_t seek(off_t off, int whence) = 0;
	virtual off_t getPositionInFile() = 0;
	virtual void truncate(off_t length) = 0;
	virtual void sync() = 0;
	virtual std::string getFileName() const noexcept = 0;
	virtual int getFD() const noexcept = 0;
};

}
