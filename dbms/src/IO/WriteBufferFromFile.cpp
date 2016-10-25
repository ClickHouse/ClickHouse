#include <sys/stat.h>
#include <fcntl.h>

#include <DB/Common/ProfileEvents.h>

#include <DB/IO/WriteBufferFromFile.h>
#include <DB/IO/WriteHelpers.h>


namespace ProfileEvents
{
	extern const Event FileOpen;
}

namespace DB
{

namespace ErrorCodes
{
	extern const int FILE_DOESNT_EXIST;
	extern const int CANNOT_OPEN_FILE;
	extern const int CANNOT_CLOSE_FILE;
}


WriteBufferFromFile::WriteBufferFromFile(
	const std::string & file_name_,
	size_t buf_size,
	int flags,
	mode_t mode,
	char * existing_memory,
	size_t alignment)
	: WriteBufferFromFileDescriptor(-1, buf_size, existing_memory, alignment), file_name(file_name_)
{
	ProfileEvents::increment(ProfileEvents::FileOpen);

	fd = open(file_name.c_str(), flags == -1 ? O_WRONLY | O_TRUNC | O_CREAT : flags, mode);

	if (-1 == fd)
		throwFromErrno("Cannot open file " + file_name, errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
}


/// Use pre-opened file descriptor.
WriteBufferFromFile::WriteBufferFromFile(
	int fd,
	size_t buf_size,
	int flags,
	mode_t mode,
	char * existing_memory,
	size_t alignment)
	: WriteBufferFromFileDescriptor(fd, buf_size, existing_memory, alignment), file_name("(fd = " + toString(fd) + ")")
{
}


WriteBufferFromFile::~WriteBufferFromFile()
{
	if (fd < 0)
		return;

	try
	{
		next();
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}

	::close(fd);
}


/// Close file before destruction of object.
void WriteBufferFromFile::close()
{
	next();

	if (0 != ::close(fd))
		throw Exception("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);

	fd = -1;
	metric_increment.destroy();
}

}
