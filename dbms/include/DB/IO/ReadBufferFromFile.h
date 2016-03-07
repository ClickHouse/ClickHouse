#pragma once

#include <fcntl.h>

#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/Common/CurrentMetrics.h>


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
class ReadBufferFromFile : public ReadBufferFromFileDescriptor
{
private:
	std::string file_name;
	CurrentMetrics::Increment metric_increment{CurrentMetrics::OpenFileForRead};

public:
	ReadBufferFromFile(const std::string & file_name_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, int flags = -1,
		char * existing_memory = nullptr, size_t alignment = 0)
		: ReadBufferFromFileDescriptor(-1, buf_size, existing_memory, alignment), file_name(file_name_)
	{
		ProfileEvents::increment(ProfileEvents::FileOpen);

		fd = open(file_name.c_str(), flags == -1 ? O_RDONLY : flags);

		if (-1 == fd)
			throwFromErrno("Cannot open file " + file_name, errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
	}

	/// Использовать уже открытый файл.
	ReadBufferFromFile(int fd, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE, int flags = -1,
		char * existing_memory = nullptr, size_t alignment = 0)
		: ReadBufferFromFileDescriptor(fd, buf_size, existing_memory, alignment), file_name("(fd = " + toString(fd) + ")")
	{
	}

	virtual ~ReadBufferFromFile()
	{
		if (fd < 0)
			return;

		::close(fd);
	}

	/// Закрыть файл раньше вызова деструктора.
	void close()
	{
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
