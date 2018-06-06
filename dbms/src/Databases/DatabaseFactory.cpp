#include <Databases/DatabaseFactory.h>
#include <Databases/DatabaseOrdinary.h>
#include <Databases/DatabaseMemory.h>
#include <Databases/DatabaseDictionary.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_DATABASE_ENGINE;
}


DatabasePtr DatabaseFactory::get(
    const String & engine_name,
    const String & database_name,
    const String & metadata_path,
    Context & context)
{
    if (engine_name == "Ordinary")
        return std::make_shared<DatabaseOrdinary>(database_name, metadata_path, context);
    else if (engine_name == "Memory")
        return std::make_shared<DatabaseMemory>(database_name);
    else if (engine_name == "Dictionary")
        return std::make_shared<DatabaseDictionary>(database_name, context);

    throw Exception("Unknown database engine: " + engine_name, ErrorCodes::UNKNOWN_DATABASE_ENGINE);
}

}
