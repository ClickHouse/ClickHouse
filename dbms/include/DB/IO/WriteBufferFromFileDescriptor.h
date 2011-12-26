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

		size_t bytes_written = 0;
		while (bytes_written != offset())
		{
			ssize_t res = ::write(fd, working_buffer.begin() + bytes_written, offset() - bytes_written);

			if ((-1 == res || 0 == res) && errno != EINTR)
				throwFromErrno("Cannot write to file " + getFileName(), ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);

			if (res > 0)
				bytes_written += res;
		}
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
		bool uncaught_exception = std::uncaught_exception();

		try
		{
			next();
		}
		catch (...)
		{
			/// Если до этого уже было какое-то исключение, то второе исключение проигнорируем.
			if (!uncaught_exception)
				throw;
		}
	}
};

}
