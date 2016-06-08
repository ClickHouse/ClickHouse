#pragma once

#include <fcntl.h>
#include <sys/file.h>

#include <string>
#include <iostream>

#include <Poco/File.h>
#include <Poco/Exception.h>
#include <mutex>
#include <Poco/ScopedLock.h>

#include <DB/Common/Exception.h>
#include <DB/IO/ReadBufferFromFileDescriptor.h>
#include <DB/IO/WriteBufferFromFileDescriptor.h>
#include <DB/IO/ReadHelpers.h>
#include <DB/IO/WriteHelpers.h>

#include <common/Common.h>

#define SMALL_READ_WRITE_BUFFER_SIZE 16


/** Хранит в файле число.
 * Предназначен для редких вызовов (не рассчитан на производительность).
 */
class CounterInFile
{
public:
	/// path - имя файла, включая путь
	CounterInFile(const std::string & path_) : path(path_) {}

	/** Добавить delta к числу в файле и вернуть новое значение.
	 * Если параметр create_if_need не установлен в true, то
	 *  в файле уже должно быть записано какое-нибудь число (если нет - создайте файл вручную с нулём).
	 *
	 * Для защиты от race condition-ов между разными процессами, используются файловые блокировки.
	 * (Но при первом создании файла race condition возможен, так что лучше создать файл заранее.)
	 *
	 * locked_callback вызывается при заблокированном файле со счетчиком. В него передается новое значение.
	 * locked_callback можно использовать, чтобы делать что-нибудь атомарно с увеличением счетчика (например, переименовывать файлы).
	 */
	template <typename Callback>
	Int64 add(Int64 delta, Callback && locked_callback, bool create_if_need = false)
	{
		std::lock_guard<std::mutex> lock(mutex);

		Int64 res = -1;

		bool file_doesnt_exists = !Poco::File(path).exists();
		if (file_doesnt_exists && !create_if_need)
		{
			throw Poco::Exception("File " + path + " does not exist. "
			"You must create it manulally with appropriate value or 0 for first start.");
		}

		int fd = open(path.c_str(), O_RDWR | O_CREAT, 0666);
		if (-1 == fd)
			DB::throwFromErrno("Cannot open file " + path);

		try
		{
			int flock_ret = flock(fd, LOCK_EX);
			if (-1 == flock_ret)
				DB::throwFromErrno("Cannot lock file " + path);

			if (!file_doesnt_exists)
			{
				DB::ReadBufferFromFileDescriptor rb(fd, SMALL_READ_WRITE_BUFFER_SIZE);
				try
				{
					DB::readIntText(res, rb);
				}
				catch (const DB::Exception & e)
				{
					/// Более понятное сообщение об ошибке.
					if (e.code() == DB::ErrorCodes::CANNOT_READ_ALL_DATA || e.code() == DB::ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
						throw DB::Exception("File " + path + " is empty. You must fill it manually with appropriate value.", e.code());
					else
						throw;
				}
			}
			else
				res = 0;

			if (delta || file_doesnt_exists)
			{
				res += delta;

				DB::WriteBufferFromFileDescriptor wb(fd, SMALL_READ_WRITE_BUFFER_SIZE);
				wb.seek(0);
				wb.truncate();
				DB::writeIntText(res, wb);
				DB::writeChar('\n', wb);
				wb.sync();
			}

			locked_callback(res);
		}
		catch (...)
		{
			close(fd);
			throw;
		}

		close(fd);
		return res;
	}

	Int64 add(Int64 delta, bool create_if_need = false)
	{
		return add(delta, [](UInt64){}, create_if_need);
	}

	const std::string & getPath() const
	{
		return path;
	}

	/// Изменить путь к файлу.
	void setPath(std::string path_)
	{
		path = path_;
	}

	// Не thread-safe и не синхронизирован между процессами.
	void fixIfBroken(UInt64 value)
	{
		bool file_exists = Poco::File(path).exists();

		int fd = open(path.c_str(), O_RDWR | O_CREAT, 0666);
		if (-1 == fd)
			DB::throwFromErrno("Cannot open file " + path);

		try
		{
			bool broken = true;

			if (file_exists)
			{
				DB::ReadBufferFromFileDescriptor rb(fd, SMALL_READ_WRITE_BUFFER_SIZE);
				try
				{
					UInt64 current_value;
					DB::readIntText(current_value, rb);
					char c;
					DB::readChar(c, rb);
					if (rb.count() > 0 && c == '\n' && rb.eof())
						broken = false;
				}
				catch (const DB::Exception & e)
				{
					if (e.code() != DB::ErrorCodes::CANNOT_READ_ALL_DATA && e.code() != DB::ErrorCodes::ATTEMPT_TO_READ_AFTER_EOF)
						throw;
				}
			}

			if (broken)
			{
				DB::WriteBufferFromFileDescriptor wb(fd, SMALL_READ_WRITE_BUFFER_SIZE);
				wb.seek(0);
				wb.truncate();
				DB::writeIntText(value, wb);
				DB::writeChar('\n', wb);
				wb.sync();
			}
		}
		catch (...)
		{
			close(fd);
			throw;
		}

		close(fd);
	}

private:
	std::string path;
	std::mutex mutex;
};


#undef SMALL_READ_WRITE_BUFFER_SIZE
