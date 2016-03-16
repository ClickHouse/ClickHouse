#include <DB/Databases/DatabaseFactory.h>
#include <DB/Databases/DatabaseOrdinary.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int UNKNOWN_DATABASE_ENGINE;
}


DatabasePtr DatabaseFactory::get(
	const String & name, const String & path, boost::threadpool::pool & thread_pool)
{
	if (name == "Ordinary")
		return std::make_shared<DatabaseOrdinary>(path, thread_pool);

	throw Exception("Unknown database engine: " + name, ErrorCodes::UNKNOWN_DATABASE_ENGINE);
}

}
