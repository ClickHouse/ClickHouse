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
		ssize_t bytes_read = ::read(fd, working_buffer.begin(), working_buffer.size());
		if (-1 == bytes_read)
			throwFromErrno("Cannot read from file " + getFileName(), ErrorCodes::CANNOT_READ_FROM_FILE_DESCRIPTOR);
		
		if (!bytes_read)
			return false;
		else
			working_buffer.resize(bytes_read);

		return true;
	}

	void throwFromErrno(const std::string & s, int code)
	{
		char buf[128];
		throw Exception(s + ", errno: " + Poco::NumberFormatter::format(errno) + ", strerror: " + std::string(strerror_r(errno, buf, sizeof(buf))), code);
	}

	/// Имя или описание файла
	virtual std::string getFileName()
	{
		return "(fd = " + Poco::NumberFormatter::format(fd) + ")";
	}

public:
	ReadBufferFromFileDescriptor(int fd_) : fd(fd_) {}
};

}
