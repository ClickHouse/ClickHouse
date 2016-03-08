#pragma once

#include <unistd.h>
#include <errno.h>

#include <DB/Common/Exception.h>
#include <DB/Common/ProfileEvents.h>
#include <DB/Common/CurrentMetrics.h>

#include <DB/IO/WriteBufferFromFileBase.h>
#include <DB/IO/WriteBuffer.h>
#include <DB/IO/WriteHelpers.h>
#include <DB/IO/BufferWithOwnMemory.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int CANNOT_WRITE_TO_FILE_DESCRIPTOR;
	extern const int CANNOT_FSYNC;
	extern const int CANNOT_SEEK_THROUGH_FILE;
	extern const int CANNOT_TRUNCATE_FILE;
}

/** Работает с готовым файловым дескриптором. Не открывает и не закрывает файл.
  */
class WriteBufferFromFileDescriptor : public WriteBufferFromFileBase
{
protected:
	int fd;

	void nextImpl() override
	{
		if (!offset())
			return;

		size_t bytes_written = 0;
		while (bytes_written != offset())
		{
			ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWrite);

			ssize_t res = 0;
			{
				CurrentMetrics::Increment metric_increment{CurrentMetrics::Write};
				res = ::write(fd, working_buffer.begin() + bytes_written, offset() - bytes_written);
			}

			if ((-1 == res || 0 == res) && errno != EINTR)
				throwFromErrno("Cannot write to file " + getFileName(), ErrorCodes::CANNOT_WRITE_TO_FILE_DESCRIPTOR);

			if (res > 0)
				bytes_written += res;
		}

		ProfileEvents::increment(ProfileEvents::WriteBufferFromFileDescriptorWriteBytes, bytes_written);
	}

	/// Имя или описание файла
	virtual std::string getFileName() const override
	{
		return "(fd = " + toString(fd) + ")";
	}

public:
	WriteBufferFromFileDescriptor(int fd_ = -1, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, char * existing_memory = nullptr, size_t alignment = 0)
		: WriteBufferFromFileBase(buf_size, existing_memory, alignment), fd(fd_) {}

	/** Можно вызывать для инициализации, если нужный fd не был передан в конструктор.
	  * Менять fd во время работы нельзя.
	  */
	void setFD(int fd_)
	{
		fd = fd_;
	}

    ~WriteBufferFromFileDescriptor()
	{
		try
		{
			if (fd >= 0)
				next();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}
	}

	int getFD() const override
	{
		return fd;
	}

	off_t getPositionInFile() override
	{
		return seek(0, SEEK_CUR);
	}

	void sync() override
	{
		/// Если в буфере ещё остались данные - запишем их.
		next();

		/// Попросим ОС сбросить данные на диск.
		int res = fsync(fd);
		if (-1 == res)
			throwFromErrno("Cannot fsync " + getFileName(), ErrorCodes::CANNOT_FSYNC);
	}

private:
	off_t doSeek(off_t offset, int whence) override
	{
		off_t res = lseek(fd, offset, whence);
		if (-1 == res)
			throwFromErrno("Cannot seek through file " + getFileName(), ErrorCodes::CANNOT_SEEK_THROUGH_FILE);
		return res;
	}

	void doTruncate(off_t length) override
	{
		int res = ftruncate(fd, length);
		if (-1 == res)
			throwFromErrno("Cannot truncate file " + getFileName(), ErrorCodes::CANNOT_TRUNCATE_FILE);
	}
};

}
