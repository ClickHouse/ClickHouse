#include "StatusFile.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <fcntl.h>

#include <Poco/File.h>
#include <common/logger_useful.h>
#include <common/ClickHouseRevision.h>
#include <common/LocalDateTime.h>

#include <DB/IO/ReadBufferFromFile.h>
#include <DB/IO/LimitReadBuffer.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/Operators.h>


namespace DB
{


StatusFile::StatusFile(const std::string & path_)
	: path(path_)
{
	/// Если файл уже существует. NOTE Незначительный race condition.
	if (Poco::File(path).exists())
	{
		std::string contents;
		{
			ReadBufferFromFile in(path, 1024);
			LimitReadBuffer limit_in(in, 1024);
			readStringUntilEOF(contents, limit_in);
		}

		if (!contents.empty())
			LOG_INFO(&Logger::get("StatusFile"), "Status file " << path << " already exists - unclean restart. Contents:\n" << contents);
		else
			LOG_INFO(&Logger::get("StatusFile"), "Status file " << path << " already exists and is empty - probably unclean hardware restart.");
	}

	fd = open(path.c_str(), O_WRONLY | O_CREAT, 0666);

	if (-1 == fd)
		throwFromErrno("Cannot open file " + path);

	try
	{
		int flock_ret = flock(fd, LOCK_EX | LOCK_NB);
		if (-1 == flock_ret)
		{
			if (errno == EWOULDBLOCK)
				throw Exception("Cannot lock file " + path + ". Another server instance in same directory is already running.");
			else
				throwFromErrno("Cannot lock file " + path);
		}

		if (0 != ftruncate(fd, 0))
			throwFromErrno("Cannot ftruncate " + path);

		if (0 != lseek(fd, 0, SEEK_SET))
			throwFromErrno("Cannot lseek " + path);

		/// Записываем в файл информацию о текущем экземпляре сервера.
		{
			WriteBufferFromFileDescriptor out(fd, 1024);
			out
				<< "PID: " << getpid() << "\n"
				<< "Started at: " << LocalDateTime(time(0)) << "\n"
				<< "Revision: " << ClickHouseRevision::get() << "\n";
		}
	}
	catch (...)
	{
		close(fd);
		throw;
	}
}


StatusFile::~StatusFile()
{
	char buf[128];

	if (0 != close(fd))
		LOG_ERROR(&Logger::get("StatusFile"), "Cannot close file " << path << ", errno: "
			<< errno << ", strerror: " << strerror_r(errno, buf, sizeof(buf)));

	if (0 != unlink(path.c_str()))
		LOG_ERROR(&Logger::get("StatusFile"), "Cannot unlink file " << path << ", errno: "
			<< errno << ", strerror: " << strerror_r(errno, buf, sizeof(buf)));
}

}
