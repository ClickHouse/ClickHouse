#pragma once

#include <string>
#include <Poco/File.h>
#include <Yandex/logger_useful.h>


/// Удаляет директорию в деструкторе.
class DatabaseDropper
{
public:
	DatabaseDropper(const std::string & data_path_) : drop_on_destroy(false), data_path(data_path_) {}
	
	~DatabaseDropper()
	{
		if (drop_on_destroy)
		{
			try
			{
				if (std::uncaught_exception())
					LOG_ERROR(&Logger::get("DatabaseDropper"), "Didn't remove database data directory because of uncaught exception.");
				else
					Poco::File(data_path).remove(false);
			}
			catch(...)
			{
			}
		}
	}
	
	bool drop_on_destroy;
	
private:
	std::string data_path;
};

typedef boost::shared_ptr<DatabaseDropper> DatabaseDropperPtr;
