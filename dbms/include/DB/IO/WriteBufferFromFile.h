#pragma once

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <DB/Common/ProfileEvents.h>
#include <DB/Common/CurrentMetrics.h>

#include <DB/IO/WriteBufferFromFileDescriptor.h>


namespace ProfileEvents
{
	extern const Event FileOpen;
}

namespace CurrentMetrics
{
	extern const Metric OpenFileForWrite;
}

namespace DB
{

namespace ErrorCodes
{
	extern const int FILE_DOESNT_EXIST;
	extern const int CANNOT_OPEN_FILE;
	extern const int CANNOT_CLOSE_FILE;
}


/** Accepts path to file and opens it, or pre-opened file descriptor.
  * Closes file by himself (thus "owns" a file descriptor).
  */
class WriteBufferFromFile : public WriteBufferFromFileDescriptor
{
private:
	std::string file_name;
	CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForWrite};

public:
	WriteBufferFromFile(const std::string & file_name_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, int flags = -1, mode_t mode = 0666,
		char * existing_memory = nullptr, size_t alignment = 0)
		: WriteBufferFromFileDescriptor(-1, buf_size, existing_memory, alignment), file_name(file_name_)
	{
		ProfileEvents::increment(ProfileEvents::FileOpen);

		fd = open(file_name.c_str(), flags == -1 ? O_WRONLY | O_TRUNC | O_CREAT : flags, mode);

		if (-1 == fd)
			throwFromErrno("Cannot open file " + file_name, errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
	}

	/// Use pre-opened file descriptor.
	WriteBufferFromFile(int fd, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, int flags = -1, mode_t mode = 0666,
		char * existing_memory = nullptr, size_t alignment = 0)
		: WriteBufferFromFileDescriptor(fd, buf_size, existing_memory, alignment), file_name("(fd = " + toString(fd) + ")")
	{
	}

	~WriteBufferFromFile()
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
	void close()
	{
		next();

		if (0 != ::close(fd))
			throw Exception("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);

		fd = -1;
		metric_increment.destroy();
	}

	std::string getFileName() const override
	{
		return file_name;
	}
};

}
