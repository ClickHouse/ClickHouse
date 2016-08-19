#pragma once

#include <DB/Common/ThreadPool.h>
#include <DB/Databases/IDatabase.h>


namespace DB
{

class DatabaseFactory
{
public:
	static DatabasePtr get(
		const String & engine_name,
		const String & database_name,
		const String & path,
		Context & context);
};

}
