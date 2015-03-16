#pragma once

#include <fcntl.h>

#include <DB/IO/ReadBufferFromFileDescriptor.h>


namespace DB
{

/** Принимает имя файла. Самостоятельно открывает и закрывает файл.
  */
class ReadBufferFromFile : public ReadBufferFromFileDescriptor
{
private:
	std::string file_name;
	
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

	virtual ~ReadBufferFromFile()
	{
		close(fd);
	}

	virtual std::string getFileName()
	{
		return file_name;
	}
};

}
