#pragma once

#include <Interpreters/Context_fwd.h>
#include <Databases/IDatabase.h>

namespace DB
{

class ASTCreateQuery;

class DatabaseFactory : private boost::noncopyable
{
public:

    static DatabaseFactory & instance();

    struct Arguments
    {
        const String & database_name;
        const String & metadata_path;
        const UUID & uuid;
        const ContextPtr & context;
        const UInt64 & cache_expiration_time_seconds;
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
