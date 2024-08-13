#include <Common/NamedCollections/NamedCollectionUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/FieldVisitorToString.h>
#include <Common/logger_useful.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Parsers/ASTDropNamedCollectionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Interpreters/Context.h>
#include <Common/NamedCollections/NamedCollections.h>
#include <Common/NamedCollections/NamedCollectionConfiguration.h>

#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int NAMED_COLLECTION_ALREADY_EXISTS;
    extern const int NAMED_COLLECTION_DOESNT_EXIST;
    extern const int BAD_ARGUMENTS;
}

namespace NamedCollectionUtils
{

static std::atomic<bool> is_loaded_from_config = false;
static std::atomic<bool> is_loaded_from_sql = false;

class LoadFromConfig
{
private:
    const Poco::Util::AbstractConfiguration & config;

public:
    explicit LoadFromConfig(const Poco::Util::AbstractConfiguration & config_)
        : config(config_) {}

    std::vector<std::string> listCollections() const
    {
        Poco::Util::AbstractConfiguration::Keys collections_names;
        config.keys(NAMED_COLLECTIONS_CONFIG_PREFIX, collections_names);
        return collections_names;
    }

    NamedCollectionsMap getAll() const
    {
        NamedCollectionsMap result;
        for (const auto & collection_name : listCollections())
        {
            if (result.contains(collection_name))
            {
                throw Exception(
                    ErrorCodes::NAMED_COLLECTION_ALREADY_EXISTS,
                    "Found duplicate named collection `{}`",
                    collection_name);
            }
            result.emplace(collection_name, get(collection_name));
        }
        return result;
    }

    MutableNamedCollectionPtr get(const std::string & collection_name) const
    {
        const auto collection_prefix = getCollectionPrefix(collection_name);
        std::queue<std::string> enumerate_input;
        std::set<std::string, std::less<>> enumerate_result;

        enumerate_input.push(collection_prefix);
        NamedCollectionConfiguration::listKeys(config, std::move(enumerate_input), enumerate_result, -1);

        /// Collection does not have any keys.
        /// (`enumerate_result` == <collection_path>).
        const bool collection_is_empty = enumerate_result.size() == 1
            && *enumerate_result.begin() == collection_prefix;
        std::set<std::string, std::less<>> keys;
        if (!collection_is_empty)
        {
            /// Skip collection prefix and add +1 to avoid '.' in the beginning.
            for (const auto & path : enumerate_result)
                keys.emplace(path.substr(collection_prefix.size() + 1));
        }

        return NamedCollection::create(
            config, collection_name, collection_prefix, keys, SourceId::CONFIG, /* is_mutable */false);
    }

private:
    static constexpr auto NAMED_COLLECTIONS_CONFIG_PREFIX = "named_collections";

    static std::string getCollectionPrefix(const std::string & collection_name)
    {
        return fmt::format("{}.{}", NAMED_COLLECTIONS_CONFIG_PREFIX, collection_name);
    }
};


class LoadFromSQL : private WithContext
{
private:
    const std::string metadata_path;

public:
    explicit LoadFromSQL(ContextPtr context_)
        : WithContext(context_)
        , metadata_path(
            fs::canonical(context_->getPath()) / NAMED_COLLECTIONS_METADATA_DIRECTORY)
    {
        if (fs::exists(metadata_path))
            cleanUp();
        else
            fs::create_directories(metadata_path);
    }

    std::vector<std::string> listCollections() const
    {
        std::vector<std::string> collection_names;
        fs::directory_iterator it{metadata_path};
        for (; it != fs::directory_iterator{}; ++it)
        {
            const auto & current_path = it->path();
            if (current_path.extension() == ".sql")
            {
                collection_names.push_back(it->path().stem());
            }
            else
            {
                LOG_WARNING(
                    getLogger("NamedCollectionsLoadFromSQL"),
                    "Unexpected file {} in named collections directory",
                    current_path.filename().string());
            }
        }
        return collection_names;
    }

    NamedCollectionsMap getAll() const
    {
        NamedCollectionsMap result;
        for (const auto & collection_name : listCollections())
        {
            if (result.contains(collection_name))
            {
                throw Exception(
                    ErrorCodes::NAMED_COLLECTION_ALREADY_EXISTS,
                    "Found duplicate named collection `{}`",
                    collection_name);
            }
            result.emplace(collection_name, get(collection_name));
        }
        return result;
    }

    MutableNamedCollectionPtr get(const std::string & collection_name) const
    {
        const auto query = readCreateQueryFromMetadata(
            getMetadataPath(collection_name),
            getContext()->getSettingsRef());
        return createNamedCollectionFromAST(query);
    }

    MutableNamedCollectionPtr create(const ASTCreateNamedCollectionQuery & query)
    {
        writeCreateQueryToMetadata(
            query,
            getMetadataPath(query.collection_name),
            getContext()->getSettingsRef());

        return createNamedCollectionFromAST(query);
    }

    void update(const ASTAlterNamedCollectionQuery & query)
    {
        const auto path = getMetadataPath(query.collection_name);
        auto create_query = readCreateQueryFromMetadata(path, getContext()->getSettings());

        std::unordered_map<std::string, Field> result_changes_map;
        for (const auto & [name, value] : query.changes)
        {
            auto [it, inserted] = result_changes_map.emplace(name, value);
            if (!inserted)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Value with key `{}` is used twice in the SET query (collection name: {})",
                    name, query.collection_name);
            }
        }

        for (const auto & [name, value] : create_query.changes)
            result_changes_map.emplace(name, value);

        std::unordered_map<std::string, bool> result_overridability_map;
        for (const auto & [name, value] : query.overridability)
            result_overridability_map.emplace(name, value);
        for (const auto & [name, value] : create_query.overridability)
            result_overridability_map.emplace(name, value);

        for (const auto & delete_key : query.delete_keys)
        {
            auto it = result_changes_map.find(delete_key);
            if (it == result_changes_map.end())
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Cannot delete key `{}` because it does not exist in collection",
                    delete_key);
            }
            else
            {
                result_changes_map.erase(it);
                auto it_override = result_overridability_map.find(delete_key);
                if (it_override != result_overridability_map.end())
                    result_overridability_map.erase(it_override);
            }
        }

        create_query.changes.clear();
        for (const auto & [name, value] : result_changes_map)
            create_query.changes.emplace_back(name, value);
        create_query.overridability = std::move(result_overridability_map);

        if (create_query.changes.empty())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Named collection cannot be empty (collection name: {})",
                query.collection_name);

        writeCreateQueryToMetadata(
            create_query,
            getMetadataPath(query.collection_name),
            getContext()->getSettingsRef(),
            true);
    }

    void remove(const std::string & collection_name)
    {
        auto collection_path = getMetadataPath(collection_name);
        if (!fs::exists(collection_path))
        {
            throw Exception(
                ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
                "Cannot remove collection `{}`, because it doesn't exist",
                collection_name);
        }
        fs::remove(collection_path);
    }

private:
    static constexpr auto NAMED_COLLECTIONS_METADATA_DIRECTORY = "named_collections";

    static MutableNamedCollectionPtr createNamedCollectionFromAST(
        const ASTCreateNamedCollectionQuery & query)
    {
        const auto & collection_name = query.collection_name;
        const auto config = NamedCollectionConfiguration::createConfiguration(collection_name, query.changes, query.overridability);

        std::set<std::string, std::less<>> keys;
        for (const auto & [name, _] : query.changes)
            keys.insert(name);

        return NamedCollection::create(
            *config, collection_name, "", keys, SourceId::SQL, /* is_mutable */true);
    }

    std::string getMetadataPath(const std::string & collection_name) const
    {
        return fs::path(metadata_path) / (escapeForFileName(collection_name) + ".sql");
    }

    /// Delete .tmp files. They could be left undeleted in case of
    /// some exception or abrupt server restart.
    void cleanUp()
    {
        fs::directory_iterator it{metadata_path};
        std::vector<std::string> files_to_remove;
        for (; it != fs::directory_iterator{}; ++it)
        {
            const auto & current_path = it->path();
            if (current_path.extension() == ".tmp")
                files_to_remove.push_back(current_path);
        }
        for (const auto & file : files_to_remove)
            fs::remove(file);
    }

    static ASTCreateNamedCollectionQuery readCreateQueryFromMetadata(
        const std::string & path,
        const Settings & settings)
    {
        ReadBufferFromFile in(path);
        std::string query;
        readStringUntilEOF(query, in);

        ParserCreateNamedCollectionQuery parser;
        auto ast = parseQuery(parser, query, "in file " + path, 0, settings.max_parser_depth, settings.max_parser_backtracks);
        const auto & create_query = ast->as<const ASTCreateNamedCollectionQuery &>();
        return create_query;
    }

    static void writeCreateQueryToMetadata(
        const ASTCreateNamedCollectionQuery & query,
        const std::string & path,
        const Settings & settings,
        bool replace = false)
    {
        if (!replace && fs::exists(path))
        {
            throw Exception(
                ErrorCodes::NAMED_COLLECTION_ALREADY_EXISTS,
                "Metadata file {} for named collection already exists",
                path);
        }

        auto tmp_path = path + ".tmp";
        String formatted_query = serializeAST(query);
        WriteBufferFromFile out(tmp_path, formatted_query.size(), O_WRONLY | O_CREAT | O_EXCL);
        writeString(formatted_query, out);

        out.next();
        if (settings.fsync_metadata)
            out.sync();
        out.close();

        fs::rename(tmp_path, path);
    }
};

std::unique_lock<std::mutex> lockNamedCollectionsTransaction()
{
    static std::mutex transaction_lock;
    return std::unique_lock(transaction_lock);
}

void loadFromConfigUnlocked(const Poco::Util::AbstractConfiguration & config, std::unique_lock<std::mutex> &)
{
    auto named_collections = LoadFromConfig(config).getAll();
    LOG_TRACE(
        getLogger("NamedCollectionsUtils"),
        "Loaded {} collections from config", named_collections.size());

    NamedCollectionFactory::instance().add(std::move(named_collections));
    is_loaded_from_config = true;
}

void loadFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    auto lock = lockNamedCollectionsTransaction();
    loadFromConfigUnlocked(config, lock);
}

void reloadFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    auto lock = lockNamedCollectionsTransaction();
    auto collections = LoadFromConfig(config).getAll();
    auto & instance = NamedCollectionFactory::instance();
    instance.removeById(SourceId::CONFIG);
    instance.add(collections);
    is_loaded_from_config = true;
}

void loadFromSQLUnlocked(ContextPtr context, std::unique_lock<std::mutex> &)
{
    auto named_collections = LoadFromSQL(context).getAll();
    LOG_TRACE(
        getLogger("NamedCollectionsUtils"),
        "Loaded {} collections from SQL", named_collections.size());

    NamedCollectionFactory::instance().add(std::move(named_collections));
    is_loaded_from_sql = true;
}

void loadFromSQL(ContextPtr context)
{
    auto lock = lockNamedCollectionsTransaction();
    loadFromSQLUnlocked(context, lock);
}

void loadIfNotUnlocked(std::unique_lock<std::mutex> & lock)
{
    auto global_context = Context::getGlobalContextInstance();
    if (!is_loaded_from_config)
        loadFromConfigUnlocked(global_context->getConfigRef(), lock);
    if (!is_loaded_from_sql)
        loadFromSQLUnlocked(global_context, lock);
}

void loadIfNot()
{
    if (is_loaded_from_sql && is_loaded_from_config)
        return;
    auto lock = lockNamedCollectionsTransaction();
    return loadIfNotUnlocked(lock);
}

void removeFromSQL(const ASTDropNamedCollectionQuery & query, ContextPtr context)
{
    auto lock = lockNamedCollectionsTransaction();
    loadIfNotUnlocked(lock);
    auto & instance = NamedCollectionFactory::instance();
    if (!instance.exists(query.collection_name))
    {
        if (!query.if_exists)
        {
            throw Exception(
                ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
                "Cannot remove collection `{}`, because it doesn't exist",
                query.collection_name);
        }
        return;
    }
    LoadFromSQL(context).remove(query.collection_name);
    instance.remove(query.collection_name);
}

void createFromSQL(const ASTCreateNamedCollectionQuery & query, ContextPtr context)
{
    auto lock = lockNamedCollectionsTransaction();
    loadIfNotUnlocked(lock);
    auto & instance = NamedCollectionFactory::instance();
    if (instance.exists(query.collection_name))
    {
        if (!query.if_not_exists)
        {
            throw Exception(
                ErrorCodes::NAMED_COLLECTION_ALREADY_EXISTS,
                "A named collection `{}` already exists",
                query.collection_name);
        }
        return;
    }
    instance.add(query.collection_name, LoadFromSQL(context).create(query));
}

void updateFromSQL(const ASTAlterNamedCollectionQuery & query, ContextPtr context)
{
    auto lock = lockNamedCollectionsTransaction();
    loadIfNotUnlocked(lock);
    auto & instance = NamedCollectionFactory::instance();
    if (!instance.exists(query.collection_name))
    {
        if (!query.if_exists)
        {
            throw Exception(
                ErrorCodes::NAMED_COLLECTION_DOESNT_EXIST,
                "Cannot remove collection `{}`, because it doesn't exist",
                query.collection_name);
        }
        return;
    }
    LoadFromSQL(context).update(query);

    auto collection = instance.getMutable(query.collection_name);
    auto collection_lock = collection->lock();

    for (const auto & [name, value] : query.changes)
    {
        auto it_override = query.overridability.find(name);
        if (it_override != query.overridability.end())
            collection->setOrUpdate<String, true>(name, convertFieldToString(value), it_override->second);
        else
            collection->setOrUpdate<String, true>(name, convertFieldToString(value), {});
    }

    for (const auto & key : query.delete_keys)
        collection->remove<true>(key);
}

}

}
