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
	ReadBufferFromFileBase(size_t buf_size, char * existing_memory, size_t alignment);
	virtual ~ReadBufferFromFileBase();
	off_t seek(off_t off, int whence = SEEK_SET);
	virtual off_t getPositionInFile() = 0;
	virtual std::string getFileName() const = 0;
	virtual int getFD() const = 0;

protected:
	virtual off_t doSeek(off_t off, int whence) = 0;
};

}
