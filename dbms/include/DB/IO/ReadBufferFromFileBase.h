#pragma once

#include <string>
#include <fcntl.h>

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>

namespace DB
{

class ReadBufferFromFileBase : public BufferWithOwnMemory<ReadBuffer>
{
public:
	ReadBufferFromFileBase(size_t buf_size, char * existing_memory, size_t alignment)
	: BufferWithOwnMemory<ReadBuffer>(buf_size, existing_memory, alignment)
	{
	}

	virtual ~ReadBufferFromFileBase() {}
	virtual off_t seek(off_t off, int whence) = 0;
	virtual off_t getPositionInFile() = 0;
	virtual std::string getFileName() const noexcept = 0;
	virtual int getFD() const noexcept = 0;
};

}
