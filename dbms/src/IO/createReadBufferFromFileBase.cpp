#include <DB/IO/createReadBufferFromFileBase.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/ReadBufferAIO.h>

namespace DB
{

ReadBufferFromFileBase * createReadBufferFromFileBase(const std::string & filename_, size_t aio_threshold, 
		size_t buffer_size_, int flags_, char * existing_memory_, size_t alignment)
{
	struct stat info;
	int res = ::stat(filename_.c_str(), &info);
	if (res != 0)
		throwFromErrno("Cannot stat file " + filename_);

	if (info.st_size < 0)
		throw Exception("Corrupted file " + filename_, ErrorCodes::LOGICAL_ERROR);

	if (static_cast<size_t>(info.st_size) < aio_threshold)
		return new ReadBufferFromFile(filename_, buffer_size_, flags_, existing_memory_, alignment);
	else
		return new ReadBufferAIO(filename_, buffer_size_, flags_, existing_memory_);
}

}
