#pragma once

#include <Common/ThreadPool.h>
#include <Databases/IDatabase.h>

namespace DB
{

class ASTStorage;

class DatabaseFactory
{
public:
    static DatabasePtr get(const String & database_name, const String & metadata_path, const ASTStorage * engine_define, Context & context);

    static DatabasePtr getImpl(const String & database_name, const String & metadata_path, const ASTStorage * engine_define, Context & context);
};

}
