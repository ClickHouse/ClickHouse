#include <Storages/NamedCollectionUtils.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteHelpers.h>
#include <Parsers/formatAST.h>
#include <Parsers/ASTCreateNamedCollectionQuery.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/parseQuery.h>
#include <Parsers/ParserCreateQuery.h>
#include <Interpreters/Context.h>
#include <Common/escapeForFileName.h>
#include <Storages/NamedCollections.h>
#include <Storages/NamedCollectionConfiguration.h>
#include <Common/FieldVisitorToString.h>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int NAMED_COLLECTION_ALREADY_EXISTS;
    extern const int BAD_ARGUMENTS;
}

namespace NamedCollectionUtils
{

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
        std::set<std::string> enumerate_result;

        enumerate_input.push(collection_prefix);
        collectKeys(config, std::move(enumerate_input), enumerate_result);

        /// Collection does not have any keys.
        /// (`enumerate_result` == <collection_path>).
        const bool collection_is_empty = enumerate_result.size() == 1
            && *enumerate_result.begin() == collection_prefix;
        std::set<std::string> keys;
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

    /// Enumerate keys paths of the config recursively.
    /// E.g. if `enumerate_paths` = {"root.key1"} and config like
    /// <root>
    ///     <key0></key0>
    ///     <key1>
    ///         <key2></key2>
    ///         <key3>
    ///            <key4></key4>
    ///         </key3>
    ///     </key1>
    /// </root>
    /// the `result` will contain two strings: "root.key1.key2" and "root.key1.key3.key4"
    static void collectKeys(
        const Poco::Util::AbstractConfiguration & config,
        std::queue<std::string> enumerate_paths,
        std::set<std::string> & result)
    {
        if (enumerate_paths.empty())
            return;

        auto initial_paths = std::move(enumerate_paths);
        enumerate_paths = {};
        while (!initial_paths.empty())
        {
            auto path = initial_paths.front();
            initial_paths.pop();

            Poco::Util::AbstractConfiguration::Keys keys;
            config.keys(path, keys);

            if (keys.empty())
            {
                result.insert(path);
            }
            else
            {
                for (const auto & key : keys)
                    enumerate_paths.emplace(path + '.' + key);
            }
        }

        collectKeys(config, enumerate_paths, result);
    }
};


class LoadFromSQL : private WithContext
{
private:
    const std::string metadata_path;

public:
    explicit LoadFromSQL(ContextPtr context_)
        : WithContext(context_)
        , metadata_path(fs::path(context_->getPath()) / NAMED_COLLECTIONS_METADATA_DIRECTORY)
    {
        if (!fs::exists(metadata_path))
            fs::create_directories(metadata_path);
    }

    std::vector<std::string> listCollections() const
    {
        std::vector<std::string> collection_names;
        fs::directory_iterator it{metadata_path};
        for (; it != fs::directory_iterator{}; ++it)
            collection_names.push_back(it->path().stem());
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

        std::unordered_map<std::string, Field> alter_changes_map;
        for (const auto & [name, value] : query.changes)
        {
            auto [it, inserted] = alter_changes_map.emplace(name, value);
            if (!inserted)
            {
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Value with key `{}` already exists in named collection `{}`",
                    name, query.collection_name);
            }
        }

        for (auto & [name, value] : create_query.changes)
        {
            auto it = alter_changes_map.find(name);
            if (it == alter_changes_map.end())
                continue;
            value = it->second;
            alter_changes_map.erase(name);
        }

        for (const auto & [name, value] : alter_changes_map)
            create_query.changes.emplace_back(name, value);

        writeCreateQueryToMetadata(
            create_query,
            getMetadataPath(query.collection_name),
            getContext()->getSettingsRef(),
            true);
    }

    void remove(const std::string & collection_name)
    {
        auto collection_path = getMetadataPath(collection_name);
        fs::remove(collection_path);
    }

    void removeIfExists(const std::string & collection_name)
    {
        auto collection_path = getMetadataPath(collection_name);
        if (fs::exists(collection_path))
            fs::remove(collection_path);
    }

private:
    static constexpr auto NAMED_COLLECTIONS_METADATA_DIRECTORY = "named_collections";

    static MutableNamedCollectionPtr createNamedCollectionFromAST(
        const ASTCreateNamedCollectionQuery & query)
    {
        const auto & collection_name = query.collection_name;
        const auto config = NamedCollectionConfiguration::createConfiguration(
            collection_name, query.changes);

        std::set<std::string> keys;
        for (const auto & [name, _] : query.changes)
            keys.insert(name);

        return NamedCollection::create(
            *config, collection_name, "", keys, SourceId::SQL, /* is_mutable */true);
    }

    std::string getMetadataPath(const std::string & collection_name) const
    {
        return fs::path(metadata_path) / (escapeForFileName(collection_name) + ".sql");
    }

    static ASTCreateNamedCollectionQuery readCreateQueryFromMetadata(
        const std::string & path,
        const Settings & settings)
    {
        ReadBufferFromFile in(path);
        std::string query;
        readStringUntilEOF(query, in);

        ParserCreateNamedCollectionQuery parser;
        auto ast = parseQuery(parser, query, "in file " + path, 0, settings.max_parser_depth);
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
        WriteBufferFromOwnString wb;
        formatAST(query, wb, false);
        writeChar('\n', wb);
        String formatted_query = wb.str();

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
    return std::unique_lock<std::mutex>(transaction_lock);
}

void loadFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    auto lock = lockNamedCollectionsTransaction();
    NamedCollectionFactory::instance().add(LoadFromConfig(config).getAll());
}

void reloadFromConfig(const Poco::Util::AbstractConfiguration & config)
{
    auto lock = lockNamedCollectionsTransaction();
    auto collections = LoadFromConfig(config).getAll();
    auto & instance = NamedCollectionFactory::instance();
    instance.removeById(SourceId::CONFIG);
    instance.add(collections);
}

void loadFromSQL(ContextPtr context)
{
    auto lock = lockNamedCollectionsTransaction();
    NamedCollectionFactory::instance().add(LoadFromSQL(context).getAll());
}

void removeFromSQL(const std::string & collection_name, ContextPtr context)
{
    auto lock = lockNamedCollectionsTransaction();
    LoadFromSQL(context).remove(collection_name);
    NamedCollectionFactory::instance().remove(collection_name);
}

void removeIfExistsFromSQL(const std::string & collection_name, ContextPtr context)
{
    auto lock = lockNamedCollectionsTransaction();
    LoadFromSQL(context).removeIfExists(collection_name);
    NamedCollectionFactory::instance().removeIfExists(collection_name);
}

void createFromSQL(const ASTCreateNamedCollectionQuery & query, ContextPtr context)
{
    auto lock = lockNamedCollectionsTransaction();
    NamedCollectionFactory::instance().add(query.collection_name, LoadFromSQL(context).create(query));
}

void updateFromSQL(const ASTAlterNamedCollectionQuery & query, ContextPtr context)
{
    auto lock = lockNamedCollectionsTransaction();
    LoadFromSQL(context).update(query);

    auto collection = NamedCollectionFactory::instance().getMutable(query.collection_name);
    auto collection_lock = collection->lock();

    for (const auto & [name, value] : query.changes)
        collection->setOrUpdate<String, true>(name, convertFieldToString(value));

    for (const auto & key : query.delete_keys)
        collection->remove<true>(key);
}

}

}
