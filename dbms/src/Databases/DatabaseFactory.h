#pragma once

#include <Common/ThreadPool.h>
#include <Databases/IDatabase.h>
#include <Parsers/ASTCreateQuery.h>


namespace DB
{

class DatabaseFactory
{
public:
    static DatabasePtr get(
        const String & database_name,
        const String & metadata_path,
        const ASTStorage * engine_define,
        Context & context);
};

}
