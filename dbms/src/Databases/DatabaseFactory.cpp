#include <DB/Databases/DatabaseFactory.h>
#include <DB/Databases/DatabaseOrdinary.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int UNKNOWN_DATABASE_ENGINE;
}


DatabasePtr DatabaseFactory::get(
	const String & engine_name,
	const String & database_name,
	const String & path,
	Context & context,
	boost::threadpool::pool * thread_pool)
{
	if (engine_name == "Ordinary")
		return std::make_shared<DatabaseOrdinary>(database_name, path, context, thread_pool);

	throw Exception("Unknown database engine: " + engine_name, ErrorCodes::UNKNOWN_DATABASE_ENGINE);
}

}
