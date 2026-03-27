#include <Server/CustomHandlers/CustomHandlersFactory.h>
#include <Parsers/ASTCreateHandlerQuery.h>
#include <Parsers/ASTAlterHandlerQuery.h>
#include <Parsers/ParserCreateHandlerQuery.h>
#include <Parsers/ParserQuery.h>
#include <Parsers/parseQuery.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteHelpers.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>

#include <Poco/Util/AbstractConfiguration.h>

#include <algorithm>
#include <filesystem>
#include <unordered_set>


namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int SYNTAX_ERROR;
}

static constexpr auto custom_handlers_storage_config_path = "custom_handlers_storage";

/// Validate that each method in the list is one of the allowed HTTP methods.
static void validateMethods(const std::vector<std::string> & methods)
{
    static const std::unordered_set<std::string> allowed_methods = {"GET", "POST", "PUT", "DELETE"};
    for (const auto & m : methods)
    {
        if (!allowed_methods.contains(m))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown HTTP method '{}'. Allowed methods: GET, POST, PUT, DELETE", m);
    }
}

/// Parse the handler query string for syntactic correctness (but do not analyze).
static void validateQuerySyntax(const std::string & query_str)
{
    try
    {
        ParserQuery parser(query_str.data() + query_str.size());
        parseQuery(parser, query_str, 0, 0, 0);
    }
    catch (const Exception & e)
    {
        throw Exception(
            ErrorCodes::SYNTAX_ERROR,
            "Handler query is not valid SQL: {}", e.message());
    }
}


CustomHandlersFactory & CustomHandlersFactory::instance()
{
    static CustomHandlersFactory factory;
    return factory;
}

bool CustomHandlersFactory::exists(const std::string & handler_name) const
{
    std::shared_lock lock(mutex);
    return handlers.contains(handler_name);
}

CustomHandlerDefinition CustomHandlersFactory::get(const std::string & handler_name) const
{
    std::shared_lock lock(mutex);
    auto it = handlers.find(handler_name);
    if (it == handlers.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Handler `{}` does not exist", handler_name);
    return it->second;
}

std::optional<CustomHandlerDefinition> CustomHandlersFactory::tryGet(const std::string & handler_name) const
{
    std::shared_lock lock(mutex);
    auto it = handlers.find(handler_name);
    if (it == handlers.end())
        return std::nullopt;
    return it->second;
}

std::vector<CustomHandlerDefinition> CustomHandlersFactory::getSortedSnapshot() const
{
    std::shared_lock lock(mutex);
    return sorted_snapshot;
}

void CustomHandlersFactory::rebuildSortedSnapshot()
{
    sorted_snapshot.clear();
    sorted_snapshot.reserve(handlers.size());
    for (const auto & [_, def] : handlers)
        sorted_snapshot.push_back(def);
    std::sort(sorted_snapshot.begin(), sorted_snapshot.end(),
        [](const CustomHandlerDefinition & a, const CustomHandlerDefinition & b)
        {
            return a.name < b.name;
        });
}

void CustomHandlersFactory::checkAmbiguity(const CustomHandlerDefinition & def) const
{
    if (def.url_type == HandlerURLType::Regexp)
        return;

    for (const auto & [_, existing] : handlers)
    {
        if (existing.name == def.name)
            continue;

        if (existing.url_type == HandlerURLType::Regexp)
            continue;

        bool urls_conflict = false;

        if (def.url_type == HandlerURLType::Exact && existing.url_type == HandlerURLType::Exact)
            urls_conflict = (def.url == existing.url);
        else if (def.url_type == HandlerURLType::Prefix && existing.url_type == HandlerURLType::Prefix)
            urls_conflict = (def.url.starts_with(existing.url) || existing.url.starts_with(def.url));
        else if (def.url_type == HandlerURLType::Exact && existing.url_type == HandlerURLType::Prefix)
            urls_conflict = (def.url.starts_with(existing.url));
        else if (def.url_type == HandlerURLType::Prefix && existing.url_type == HandlerURLType::Exact)
            urls_conflict = (existing.url.starts_with(def.url));

        if (!urls_conflict)
            continue;

        /// Check if there is method overlap
        bool methods_overlap = false;
        for (const auto & m : def.methods)
        {
            for (const auto & em : existing.methods)
            {
                if (m == em)
                {
                    methods_overlap = true;
                    break;
                }
            }
            if (methods_overlap)
                break;
        }

        if (methods_overlap)
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Handler `{}` conflicts with existing handler `{}`: "
                "both match URL '{}' with overlapping methods",
                def.name, existing.name, def.url);
    }
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

    if (def.url_type == HandlerURLType::Regexp)
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
    validateMethods(query.methods);
    validateQuerySyntax(query.query);

    std::unique_lock lock(mutex);

    if (handlers.contains(query.handler_name))
    {
        if (query.if_not_exists)
            return;
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Handler `{}` already exists", query.handler_name);
    }

    auto def = parseDefinition(query);
    checkAmbiguity(def);

    handlers[query.handler_name] = std::move(def);

    try
    {
        persist(query.handler_name);
    }
    catch (...)
    {
        handlers.erase(query.handler_name);
        throw;
    }

    rebuildSortedSnapshot();
}

void CustomHandlersFactory::alter(const ASTAlterHandlerQuery & query)
{
    if (!query.url && !query.methods && !query.query)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "ALTER HANDLER requires at least one of URL, METHODS, or AS clause");

    if (query.methods)
        validateMethods(*query.methods);
    if (query.query)
        validateQuerySyntax(*query.query);

    std::unique_lock lock(mutex);

    auto it = handlers.find(query.handler_name);
    if (it == handlers.end())
    {
        if (query.if_exists)
            return;
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Handler `{}` does not exist", query.handler_name);
    }

    auto old_def = it->second;
    CustomHandlerDefinition new_def = it->second;

    if (query.url)
    {
        new_def.url = *query.url;
        new_def.url_type = query.url_type.value_or(HandlerURLType::Exact);
        if (new_def.url_type == HandlerURLType::Regexp)
            new_def.compiled_regex = std::make_shared<re2::RE2>(new_def.url);
        else
            new_def.compiled_regex = nullptr;
    }

    if (query.methods)
        new_def.methods = *query.methods;

    if (query.query)
        new_def.query = *query.query;

    checkAmbiguity(new_def);

    it->second = std::move(new_def);

    try
    {
        persist(query.handler_name);
    }
    catch (...)
    {
        it->second = std::move(old_def);
        throw;
    }

    rebuildSortedSnapshot();
}

void CustomHandlersFactory::remove(const std::string & handler_name)
{
    std::unique_lock lock(mutex);

    auto it = handlers.find(handler_name);
    if (it == handlers.end())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Handler `{}` does not exist", handler_name);

    auto old_def = std::move(it->second);
    handlers.erase(it);

    try
    {
        unpersist(handler_name);
    }
    catch (...)
    {
        handlers[handler_name] = std::move(old_def);
        throw;
    }

    rebuildSortedSnapshot();
}

bool CustomHandlersFactory::removeIfExists(const std::string & handler_name)
{
    std::unique_lock lock(mutex);

    auto it = handlers.find(handler_name);
    if (it == handlers.end())
        return false;

    auto old_def = std::move(it->second);
    handlers.erase(it);

    try
    {
        unpersist(handler_name);
    }
    catch (...)
    {
        handlers[handler_name] = std::move(old_def);
        throw;
    }

    rebuildSortedSnapshot();
    return true;
}

void CustomHandlersFactory::persist(const std::string & handler_name) const
{
    if (!metadata_path.empty())
    {
        std::string content = serializeHandler(handler_name);
        if (!content.empty())
            saveToDisk(handler_name, content);
    }
}

void CustomHandlersFactory::unpersist(const std::string & handler_name) const
{
    if (!metadata_path.empty())
        removeFromDisk(handler_name);
}

void CustomHandlersFactory::loadFromConfig(const ContextPtr & context)
{
    std::unique_lock lock(mutex);

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
        /// ZooKeeper-based storage is planned but not yet implemented.
        /// Fall back to local storage so the server starts normally.
        LOG_WARNING(log, "ZooKeeper-based storage for SQL handlers is not yet implemented, falling back to local storage");

        metadata_path = fs::path(context->getPath()) / "handlers_metadata/";
        loadFromDisk(metadata_path);
    }
    else
    {
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Unknown custom_handlers_storage type '{}', expected 'local', 'zookeeper', or 'keeper'",
            storage_type);
    }

    rebuildSortedSnapshot();
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

void CustomHandlersFactory::saveToDisk(const std::string & handler_name, const std::string & content) const
{
    if (metadata_path.empty() || content.empty())
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


}
