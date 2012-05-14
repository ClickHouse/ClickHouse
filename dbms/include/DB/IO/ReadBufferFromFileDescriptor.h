#pragma once

#include <unistd.h>
#include <errno.h>

#include <Poco/NumberFormatter.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/ReadBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>


namespace DB
{

/** Работает с готовым файловым дескриптором. Не открывает и не закрывает файл.
  */
class ReadBufferFromFileDescriptor : public BufferWithOwnMemory<ReadBuffer>
{
protected:
	int fd;
	
	bool nextImpl()
	{
		size_t bytes_read = 0;
		while (!bytes_read)
		{
			ssize_t res = ::read(fd, internal_buffer.begin(), internal_buffer.size());
			if (!res)
				break;
			
			if (-1 == res && errno != EINTR)
				throwFromErrno("Cannot read from file " + getFileName(), ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);

			if (res > 0)
				bytes_read += res;
		}

		if (bytes_read)
			working_buffer.resize(bytes_read);
		else
			return false;

		return true;
	}

	/// Имя или описание файла
	virtual std::string getFileName()
	{
		return "(fd = " + Poco::NumberFormatter::format(fd) + ")";
	}

public:
	ReadBufferFromFileDescriptor(int fd_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
		: BufferWithOwnMemory<ReadBuffer>(buf_size), fd(fd_) {}

	int getFD()
	{
		return fd;
	}

	off_t seek(off_t offset, int whence = SEEK_SET)
	{
		off_t res = lseek(fd, offset, whence);
		if (-1 == res)
			throwFromErrno("Cannot seek through file " + getFileName(), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
		return res;
	}
};

}
