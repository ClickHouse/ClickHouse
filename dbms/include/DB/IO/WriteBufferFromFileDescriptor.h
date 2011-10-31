#pragma once

#include <unistd.h>
#include <errno.h>

#include <Poco/NumberFormatter.h>

#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

#include <DB/IO/WriteBuffer.h>
#include <DB/IO/BufferWithOwnMemory.h>


namespace DB
{

/** Работает с готовым файловым дескриптором. Не открывает и не закрывает файл.
  */
class WriteBufferFromFileDescriptor : public BufferWithOwnMemory<WriteBuffer>
{
protected:
	int fd;
	
	void nextImpl()
	{
		if (!offset())
			return;
		
		ssize_t bytes_written = ::write(fd, working_buffer.begin(), offset());
		if (-1 == bytes_written || 0 == bytes_written)
			throwFromErrno("Cannot write to file " + getFileName(), ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);
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
	WriteBufferFromFileDescriptor(int fd_) : fd(fd_) {}

    virtual ~WriteBufferFromFileDescriptor()
	{
		nextImpl();
	}
};

}
