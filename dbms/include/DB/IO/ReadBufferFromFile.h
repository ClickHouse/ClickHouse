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
	ReadBufferFromFile(const std::string & file_name_, size_t buf_size = DBMS_DEFAULT_BUFFER_SIZE)
		: ReadBufferFromFileDescriptor(-1, buf_size), file_name(file_name_)
	{
		fd = open(file_name.c_str(), O_RDONLY);
		
		if (-1 == fd)
			throwFromErrno("Cannot open file " + file_name, errno == ENOENT ? ErrorCodes::FILE_DOESNT_EXIST : ErrorCodes::CANNOT_OPEN_FILE);
	}

    virtual ~ReadBufferFromFile()
	{
		if (0 != close(fd))
			throwFromErrno("Cannot close file " + file_name, ErrorCodes::CANNOT_CLOSE_FILE);
	}

	virtual std::string getFileName()
	{
		return file_name;
	}
};

}
