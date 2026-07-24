#pragma once

#include <Common/NamePrompter.h>
#include <Interpreters/Context_fwd.h>
#include <Databases/IDatabase.h>
#include <Parsers/ASTLiteral.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

class ASTCreateQuery;
class ASTStorage;

template <typename ValueType>
static inline ValueType safeGetLiteralValue(const ASTPtr &ast, const String &engine_name)
{
    if (!ast || !ast->as<ASTLiteral>())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Database engine {} requested literal argument.", engine_name);

    return static_cast<ValueType>(ast->as<ASTLiteral>()->value.safeGet<ValueType>());
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

    struct EngineFeatures
    {
        bool supports_arguments = false;
        bool supports_settings = false;
        bool supports_table_overrides = false;
        /// Whether this database engine accesses external data sources
        /// (e.g. MySQL, PostgreSQL, S3, DataLakeCatalog).
        /// Used by restore to skip external databases when
        /// restore_replace_external_engines_to_null is set.
        bool is_external = false;
    };

    using CreatorFn = std::function<DatabasePtr(const Arguments & arguments)>;

    struct Creator
    {
        CreatorFn creator_fn;
        EngineFeatures features;
    };

    DatabasePtr get(const ASTCreateQuery & create, const String & metadata_path, ContextPtr context);

    using DatabaseEngines = std::unordered_map<std::string, Creator>;

    void registerDatabase(const std::string & name, CreatorFn creator_fn, EngineFeatures features = EngineFeatures{
        .supports_arguments = false,
        .supports_settings = false,
        .supports_table_overrides = false,
        .is_external = false,
    });

    const DatabaseEngines & getDatabaseEngines() const { return database_engines; }

    /// Returns true if the given database engine accesses external data sources.
    bool isDatabaseExternal(const String & engine_name) const;

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

    /// validate validates the database engine that's specified in the create query for
    /// engine arguments, settings and table overrides.
    void validate(const ASTCreateQuery & create_query) const;
};

}
