#pragma once

#include <common/ThreadPool.h>
#include <Databases/IDatabase.h>


namespace DB
{

class DatabaseFactory
{
public:
    static DatabasePtr get(
        const String & engine_name,
        const String & database_name,
        const String & metadata_path,
        Context & context);
};

}
