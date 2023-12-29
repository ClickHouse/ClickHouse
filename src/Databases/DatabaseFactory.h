#pragma once

#include <Interpreters/Context_fwd.h>
#include <Databases/IDatabase.h>

#if USE_MYSQL
#    include <Parsers/ASTCreateQuery.h>
#    include <Core/MySQL/MySQLClient.h>
#    include <Core/MySQL/MySQLClient.h>
#    include <Databases/MySQL/MaterializedMySQLSettings.h>
#    include <Storages/MySQL/MySQLSettings.h>
#    include <mysqlxx/PoolWithFailover.h>
#endif

namespace DB
{

class ASTCreateQuery;

class DatabaseFactory : private boost::noncopyable
{
public:

    static DatabaseFactory & instance();

    struct Arguments
    {
        const String & engine_name;
        ASTs & engine_args;
        ASTStorage * storage_def;
        const ASTCreateQuery & create_query;
        const String & database_name;
        const String & metadata_path;
        const UUID & uuid;
        ContextPtr & context;
    };

    DatabasePtr get(const ASTCreateQuery & create, const String & metadata_path, ContextPtr context);

    DatabasePtr getImpl(const ASTCreateQuery & create, const String & metadata_path, ContextPtr context);

    using CreatorFn = std::function<DatabasePtr(const Arguments & arguments)>;

    struct Creator
    {
        CreatorFn creator_fn;
    };
    using Databases = std::unordered_map<std::string, Creator>;

    void registerDatabase(const std::string & name, CreatorFn creator_fn);

    const Databases & getAllDatabases() const { return databases; }

private:
    Databases databases;
};

}
