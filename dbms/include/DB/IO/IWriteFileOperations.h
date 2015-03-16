#pragma once

#include <string>
#include <fcntl.h>
#include <sys/types.h>

namespace DB
{

class IWriteFileOperations
{
public:
	virtual ~IWriteFileOperations() = default;
	virtual off_t seek(off_t off, int whence) = 0;
	virtual off_t getPositionInFile() = 0;
	virtual void truncate(off_t length) = 0;
	virtual void sync() = 0;
	virtual std::string getFileName() const noexcept = 0;
	virtual int getFD() const noexcept = 0;
};

}
