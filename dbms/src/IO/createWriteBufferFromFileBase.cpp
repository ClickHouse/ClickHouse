#include <DB/IO/createWriteBufferFromFileBase.h>
#include <DB/IO/WriteBufferFromFile.h>
#ifndef __APPLE__
#include <DB/IO/WriteBufferAIO.h>
#endif
#include <DB/Common/ProfileEvents.h>


namespace ProfileEvents
{
	extern const Event CreatedWriteBufferOrdinary;
	extern const Event CreatedWriteBufferAIO;
}

namespace DB
{

#ifdef __APPLE__
namespace ErrorCodes
{
		extern const int NOT_IMPLEMENTED;
}
#endif

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
#ifndef __APPLE__
		ProfileEvents::increment(ProfileEvents::CreatedWriteBufferAIO);
		return new WriteBufferAIO(filename_, buffer_size_, flags_, mode, existing_memory_);
#else
		throw Exception("AIO is not implemented yet on MacOS X", ErrorCodes::NOT_IMPLEMENTED);
#endif
	}
}

}
