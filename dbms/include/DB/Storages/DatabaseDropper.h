#pragma once

#include <string>
#include <Poco/File.h>
#include <Yandex/logger_useful.h>


/// Удаляет директорию в деструкторе.
class DatabaseDropper
{
public:
	DatabaseDropper(const std::string & data_path_) : data_path(data_path_), drop_on_destroy(false) {}
	
	~DatabaseDropper()
	{
		if (drop_on_destroy)
		{
			if (std::uncaught_exception())
			{
				try
				{
					LOG_ERROR(&Logger::get("DatabaseDropper"), "Didn't remove database data directory because of uncaught exception.");
				}
				catch(...)
				{
				}
			}
			else
			{
				Poco::File(data_path).remove(false);
			}
		}
	}
	
	bool drop_on_destroy;
	
private:
	std::string data_path;
};

typedef boost::shared_ptr<DatabaseDropper> DatabaseDropperPtr;
