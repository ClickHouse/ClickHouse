#include <DB/IO/createWriteBufferFromFileBase.h>
#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/WriteBufferAIO.h>
#include <DB/Common/ProfileEvents.h>

namespace DB
{

WriteBufferFromFileBase * createWriteBufferFromFileBase(const std::string & filename_, size_t estimated_size,
		size_t aio_threshold, size_t buffer_size_, int flags_, mode_t mode, char * existing_memory_,
		size_t alignment)
{
	if ((aio_threshold == 0) || (estimated_size < aio_threshold))
	{
		ProfileEvents::increment(ProfileEvents::CreatedWriteBufferOrdinary);
		return new WriteBufferFromFile(filename_, buffer_size_, flags_, mode, existing_memory_, alignment);
	}
	else
	{
		ProfileEvents::increment(ProfileEvents::CreatedWriteBufferAIO);
		return new WriteBufferAIO(filename_, buffer_size_, flags_, mode, existing_memory_);
	}
}

}
