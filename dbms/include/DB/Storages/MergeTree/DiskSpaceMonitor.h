#pragma once
#include <Poco/Mutex.h>
#include <sys/statvfs.h>
#include <boost/noncopyable.hpp>
#include <Yandex/logger_useful.h>
#include <DB/Core/Exception.h>
#include <DB/Core/ErrorCodes.h>

namespace DB
{

/** Узнает количество свободного места в файловой системе.
  * Можно "резервировать" место, чтобы разные операции могли согласованно планировать использование диска.
  * Резервирования не разделяются по файловым системам.
  * Вместо этого при запросе свободного места считается, что все резервирования сделаны в той же файловой системе.
  */
class DiskSpaceMonitor
{
public:
	class Reservation : private boost::noncopyable
	{
	friend class DiskSpaceMonitor;
	public:
		~Reservation()
		{
			try
			{
				Poco::ScopedLock<Poco::FastMutex> lock(DiskSpaceMonitor::reserved_bytes_mutex);
				if (DiskSpaceMonitor::reserved_bytes < size)
				{
					DiskSpaceMonitor::reserved_bytes = 0;
					LOG_ERROR(&Logger::get("DiskSpaceMonitor"), "Unbalanced reservations; it's a bug");
				}
				else
				{
					DiskSpaceMonitor::reserved_bytes -= size;
				}
			}
			catch (...)
			{
				tryLogCurrentException("~DiskSpaceMonitor");
			}
		}
	private:
		Reservation(size_t size_) : size(size_)
		{
			Poco::ScopedLock<Poco::FastMutex> lock(DiskSpaceMonitor::reserved_bytes_mutex);
			DiskSpaceMonitor::reserved_bytes += size;
		}
		size_t size;
	};

	typedef Poco::SharedPtr<Reservation> ReservationPtr;

	static size_t getUnreservedFreeSpace(const std::string & path)
	{
		struct statvfs fs;

		if (statvfs(path.c_str(), &fs) != 0)
			throwFromErrno("Could not calculate available disk space (statvfs)", ErrorCodes::CANNOT_STATVFS);

		size_t res = fs.f_bfree * fs.f_bsize;

		Poco::ScopedLock<Poco::FastMutex> lock(reserved_bytes_mutex);

		if (reserved_bytes > res)
			res = 0;
		else
			res -= reserved_bytes;

		return res;
	}

	/// Если места (приблизительно) недостаточно, возвращает nullptr.
	static ReservationPtr reserve(const std::string & path, size_t size)
	{
		if (getUnreservedFreeSpace(path) < size)
			return nullptr;
		return new Reservation(size);
	}

private:
	static size_t reserved_bytes;
	static Poco::FastMutex reserved_bytes_mutex;
};

}
