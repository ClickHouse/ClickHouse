#pragma once

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <DB/Common/ProfileEvents.h>
#include <DB/Common/CurrentMetrics.h>

#include <DB/IO/WriteBufferFromFileDescriptor.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int FILE_DOESNT_EXIST;
	extern const int CANNOT_OPEN_FILE;
	extern const int CANNOT_CLOSE_FILE;
}


/** Принимает имя файла. Самостоятельно открывает и закрывает файл.
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

	/// Использовать уже открытый файл.
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

	/// Закрыть файл раньше вызова деструктора.
	void close()
	{
		next();

		if (0 != ::close(fd))
			throw Exception("Cannot close file", ErrorCodes::CANNOT_CLOSE_FILE);

		fd = -1;
		metric_increment.destroy();
	}

	/** fsync() transfers ("flushes") all modified in-core data of (i.e., modified buffer cache pages for) the file
	  * referred to by the file descriptor fd to the disk device (or other permanent storage device)
	  * so that all changed information can be retrieved even after the system crashed or was rebooted.
	  * This includes writing through or flushing a disk cache if present. The call blocks until the device
	  * reports that the transfer has completed. It also flushes metadata information associated with the file (see stat(2)).
	  *    - man fsync */
	void sync() override
	{
		fsync(fd);
	}

	std::string getFileName() const override
	{
		return file_name;
	}
};

}
