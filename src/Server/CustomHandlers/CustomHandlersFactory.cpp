#include <Server/CustomHandlers/CustomHandlersFactory.h>
#include <Parsers/ASTCreateHandlerQuery.h>
#include <Parsers/ASTAlterHandlerQuery.h>
#include <Parsers/ParserCreateHandlerQuery.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Common/ZooKeeper/ZooKeeper.h>

#include <filesystem>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

static constexpr auto custom_handlers_storage_config_path = "custom_handlers_storage";

CustomHandlersFactory & CustomHandlersFactory::instance()
{
    static CustomHandlersFactory factory;
    return factory;
}

bool CustomHandlersFactory::exists(const std::string & handler_name) const
{
    std::lock_guard lock(mutex);
    return handlers.contains(handler_name);
}

CustomHandlerDefinition CustomHandlersFactory::get(const std::string & handler_name) const
{
    std::lock_guard lock(mutex);
    auto it = handlers.find(handler_name);
    if (it == handlers.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Handler `{}` does not exist", handler_name);
    return it->second;
}

std::optional<CustomHandlerDefinition> CustomHandlersFactory::tryGet(const std::string & handler_name) const
{
    std::lock_guard lock(mutex);
    auto it = handlers.find(handler_name);
    if (it == handlers.end())
        return std::nullopt;
    return it->second;
}

std::vector<CustomHandlerDefinition> CustomHandlersFactory::getAll() const
{
    std::lock_guard lock(mutex);
    std::vector<CustomHandlerDefinition> result;
    result.reserve(handlers.size());
    for (const auto & [_, def] : handlers)
        result.push_back(def);
    return result;
}

CustomHandlerDefinition CustomHandlersFactory::parseDefinition(const ASTCreateHandlerQuery & create_query) const
{
    CustomHandlerDefinition def;
    def.name = create_query.handler_name;
    def.url = create_query.url;
    def.url_type = create_query.url_type;
    def.methods = create_query.methods;
    def.query = create_query.query;

    if (def.methods.empty())
        def.methods.push_back("GET");

    if (def.url_type == "regexp")
        def.compiled_regex = std::make_shared<re2::RE2>(def.url);

    return def;
}

std::string CustomHandlersFactory::serializeHandler(const std::string & handler_name) const
{
    auto it = handlers.find(handler_name);
    if (it == handlers.end())
        return {};

    const auto & def = it->second;

    auto query = make_intrusive<ASTCreateHandlerQuery>();
    query->handler_name = def.name;
    query->url = def.url;
    query->url_type = def.url_type;
    query->methods = def.methods;
    query->query = def.query;

    WriteBufferFromOwnString buf;
    IAST::FormatSettings format_settings(/*one_line=*/true);
    query->format(buf, format_settings);
    return buf.str();
}

void CustomHandlersFactory::create(const ASTCreateHandlerQuery & query)
{
    std::lock_guard lock(mutex);

    if (handlers.contains(query.handler_name))
    {
        if (query.if_not_exists)
            return;
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Handler `{}` already exists", query.handler_name);
    }

    handlers[query.handler_name] = parseDefinition(query);
    persist(query.handler_name);
}

void CustomHandlersFactory::alter(const ASTAlterHandlerQuery & query)
{
    std::lock_guard lock(mutex);

    auto it = handlers.find(query.handler_name);
    if (it == handlers.end())
    {
        if (query.if_exists)
            return;
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Handler `{}` does not exist", query.handler_name);
    }

    /// Build the new definition from the old one, applying changes.
    /// Persist first, then swap in-memory state so that a failed write
    /// does not leave the in-memory map out of sync with disk/ZooKeeper.
    CustomHandlerDefinition new_def = it->second;

    if (query.url)
    {
        new_def.url = *query.url;
        new_def.url_type = query.url_type.value_or("exact");
        if (new_def.url_type == "regexp")
            new_def.compiled_regex = std::make_shared<re2::RE2>(new_def.url);
        else
            new_def.compiled_regex = nullptr;
    }

    if (query.methods)
        new_def.methods = *query.methods;

    if (query.query)
        new_def.query = *query.query;

    auto old_def = std::move(it->second);
    it->second = new_def;

    try
    {
        persist(query.handler_name);
    }
    catch (...)
    {
        it->second = std::move(old_def);
        throw;
    }
}

void CustomHandlersFactory::remove(const std::string & handler_name)
{
    std::lock_guard lock(mutex);

    auto it = handlers.find(handler_name);
    if (it == handlers.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Handler `{}` does not exist", handler_name);

    handlers.erase(it);
    unpersist(handler_name);
}

bool CustomHandlersFactory::removeIfExists(const std::string & handler_name)
{
    std::lock_guard lock(mutex);

    auto it = handlers.find(handler_name);
    if (it == handlers.end())
        return false;

    handlers.erase(it);
    unpersist(handler_name);

    return true;
}

void CustomHandlersFactory::persist(const std::string & handler_name) const
{
    if (!metadata_path.empty())
        saveToDisk(handler_name);
    if (!zk_path.empty())
        saveToZooKeeper(handler_name);
}

void CustomHandlersFactory::unpersist(const std::string & handler_name) const
{
    if (!metadata_path.empty())
        removeFromDisk(handler_name);
    if (!zk_path.empty())
        removeFromZooKeeper(handler_name);
}

void CustomHandlersFactory::loadFromConfig(const ContextPtr & context)
{
    std::lock_guard lock(mutex);
    global_context = context;

    const auto & config = context->getConfigRef();
    const auto storage_type = config.getString(
        std::string(custom_handlers_storage_config_path) + ".type", "local");

    auto log = getLogger("CustomHandlersFactory");

    if (storage_type == "local")
    {
        metadata_path = config.getString(
            std::string(custom_handlers_storage_config_path) + ".path",
            fs::path(context->getPath()) / "handlers_metadata/");

        LOG_INFO(log, "Using local storage for SQL handlers at path: {}", metadata_path);
        loadFromDisk(metadata_path);
    }
    else if (storage_type == "zookeeper" || storage_type == "keeper")
    {
        zk_path = config.getString(std::string(custom_handlers_storage_config_path) + ".path");

        LOG_INFO(log, "Using ZooKeeper storage for SQL handlers at path: {}", zk_path);
        loadFromZooKeeper();
    }
    else
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unknown custom_handlers_storage type '{}', expected 'local', 'zookeeper', or 'keeper'",
            storage_type);
    }
}

void CustomHandlersFactory::loadFromDisk(const std::string & path)
{
    metadata_path = path;

    if (!fs::exists(metadata_path))
    {
        fs::create_directories(metadata_path);
        return;
    }

    auto log = getLogger("CustomHandlersFactory");

    for (const auto & entry : fs::directory_iterator(metadata_path))
    {
        if (!entry.is_regular_file() || entry.path().extension() != ".sql")
            continue;

        try
        {
            std::string content;
            {
                ReadBufferFromFile buf(entry.path());
                readStringUntilEOF(content, buf);
            }

            ParserCreateHandlerQuery parser;
            ASTPtr ast = parseQuery(parser, content, 0, 0, 0);
            const auto & create_query = ast->as<ASTCreateHandlerQuery &>();

            handlers[create_query.handler_name] = parseDefinition(create_query);
            LOG_INFO(log, "Loaded handler `{}` from {}", create_query.handler_name, entry.path().string());
        }
        catch (...)
        {
            LOG_ERROR(log, "Failed to load handler from {}: {}", entry.path().string(), getCurrentExceptionMessage(true));
        }
    }
}

void CustomHandlersFactory::loadFromZooKeeper()
{
    auto log = getLogger("CustomHandlersFactory");

    auto zk = global_context->getZooKeeper();
    zk->createIfNotExists(zk_path, "");

    auto children = zk->getChildren(zk_path);
    for (const auto & child : children)
    {
        try
        {
            std::string content;
            zk->tryGet(zk_path + "/" + child, content);

            if (content.empty())
                continue;

            ParserCreateHandlerQuery parser;
            ASTPtr ast = parseQuery(parser, content, 0, 0, 0);
            const auto & create_query = ast->as<ASTCreateHandlerQuery &>();

            handlers[create_query.handler_name] = parseDefinition(create_query);
            LOG_INFO(log, "Loaded handler `{}` from ZooKeeper {}/{}", create_query.handler_name, zk_path, child);
        }
        catch (...)
        {
            LOG_ERROR(log, "Failed to load handler from ZooKeeper {}/{}: {}", zk_path, child, getCurrentExceptionMessage(true));
        }
    }
}

void CustomHandlersFactory::shutdown()
{
}

void CustomHandlersFactory::saveToDisk(const std::string & handler_name) const
{
    if (metadata_path.empty())
        return;

    std::string content = serializeHandler(handler_name);
    if (content.empty())
        return;

    std::string file_path = metadata_path + "/" + escapeForFileName(handler_name) + ".sql";
    std::string tmp_file_path = file_path + ".tmp";

    /// Write to a temporary file first, then atomically rename
    {
        WriteBufferFromFile out(tmp_file_path);
        writeString(content, out);
        out.sync();
        out.finalize();
    }
    fs::rename(tmp_file_path, file_path);
}

void CustomHandlersFactory::removeFromDisk(const std::string & handler_name) const
{
    if (metadata_path.empty())
        return;

    std::string file_path = metadata_path + "/" + escapeForFileName(handler_name) + ".sql";
    if (fs::exists(file_path))
    {
        LOG_INFO(getLogger("CustomHandlersFactory"), "Removing handler `{}` metadata file {}", handler_name, file_path);
        fs::remove(file_path);
    }
}

void CustomHandlersFactory::saveToZooKeeper(const std::string & handler_name) const
{
    if (zk_path.empty() || !global_context)
        return;

    std::string content = serializeHandler(handler_name);
    if (content.empty())
        return;

    auto zk = global_context->getZooKeeper();
    std::string node_path = zk_path + "/" + escapeForFileName(handler_name);
    zk->createOrUpdate(node_path, content, zkutil::CreateMode::Persistent);
}

void CustomHandlersFactory::removeFromZooKeeper(const std::string & handler_name) const
{
    if (zk_path.empty() || !global_context)
        return;

    auto zk = global_context->getZooKeeper();
    std::string node_path = zk_path + "/" + escapeForFileName(handler_name);
    zk->tryRemove(node_path);
    LOG_INFO(getLogger("CustomHandlersFactory"), "Removed handler `{}` from ZooKeeper {}", handler_name, node_path);
}

}
