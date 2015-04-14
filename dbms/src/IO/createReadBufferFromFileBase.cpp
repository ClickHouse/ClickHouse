#include <DB/IO/createReadBufferFromFileBase.h>
#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/ReadBufferAIO.h>

namespace DB
{

ReadBufferFromFileBase * createReadBufferFromFileBase(const std::string & filename_, size_t estimated_size,
		size_t aio_threshold, size_t buffer_size_, int flags_, char * existing_memory_, size_t alignment)
{
	if ((aio_threshold == 0) || (estimated_size < aio_threshold))
		return new ReadBufferFromFile(filename_, buffer_size_, flags_, existing_memory_, alignment);
	else
		return new ReadBufferAIO(filename_, buffer_size_, flags_, existing_memory_);
}

}
