#pragma once

#include <string>
#include <fcntl.h>
#include <sys/types.h>

namespace DB
{

class IReadFileOperations
{
public:
	virtual ~IReadFileOperations() = default;
	virtual off_t seek(off_t off, int whence) = 0;
	virtual off_t getPositionInFile() = 0;
	virtual std::string getFileName() const noexcept = 0;
	virtual int getFD() const noexcept = 0;
};

}
