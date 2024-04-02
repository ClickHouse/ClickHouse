#pragma once

#include <Common/NamePrompter.h>
#include <Interpreters/Context_fwd.h>
#include <Databases/IDatabase.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class ASTCreateQuery;

template <typename ValueType>
static inline ValueType safeGetLiteralValue(const ASTPtr &ast, const String &engine_name)
{
    if (!ast || !ast->as<ASTLiteral>())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database engine {} requested literal argument.", engine_name);

    return ast->as<ASTLiteral>()->value.safeGet<ValueType>();
}

class DatabaseFactory : private boost::noncopyable, public IHints<>
{
public:

    static DatabaseFactory & instance();

    struct Arguments
    {
        const String & engine_name;
        ASTs & engine_args;
        ASTStorage * storage = nullptr;
        const ASTCreateQuery & create_query;
        const String & database_name;
        const String & metadata_path;
        const UUID & uuid;
        ContextPtr & context;
    };

    DatabasePtr get(const ASTCreateQuery & create, const String & metadata_path, ContextPtr context);

    using CreatorFn = std::function<DatabasePtr(const Arguments & arguments)>;

    using DatabaseEngines = std::unordered_map<std::string, CreatorFn>;

    void registerDatabase(const std::string & name, CreatorFn creator_fn);

    const DatabaseEngines & getDatabaseEngines() const { return database_engines; }

    std::vector<String> getAllRegisteredNames() const override
    {
        std::vector<String> result;
        auto getter = [](const auto & pair) { return pair.first; };
        std::transform(database_engines.begin(), database_engines.end(), std::back_inserter(result), getter);
        return result;
    }

private:
    DatabaseEngines database_engines;

    DatabasePtr getImpl(const ASTCreateQuery & create, const String & metadata_path, ContextPtr context);
};

}
