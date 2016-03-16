#pragma once

#include <DB/Databases/IDatabase.h>


namespace DB
{

class DatabaseFactory
{
public:
	static DatabasePtr get(const String & name, const String & path, boost::threadpool::pool & thread_pool);
};

}
