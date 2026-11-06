#include <Access/ContextAccess.h>
#include <TableFunctions/ITableFunction.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageTableFunction.h>
#include <Access/Common/AccessFlags.h>
#include <Common/ProfileEvents.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/Context.h>

#include <cctype>


namespace ProfileEvents
{
    extern const Event TableFunctionExecute;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int ACCESS_DENIED;
}

const char * ITableFunction::getNonClusteredStorageEngineName() const
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented");
}

std::optional<AccessTypeObjects::Source> ITableFunction::getSourceAccessObject() const
{
    if (isClusterFunction())
        return StorageFactory::instance().getSourceAccessObject(getNonClusteredStorageEngineName());
    return StorageFactory::instance().getSourceAccessObject(getStorageEngineName());
}

void ITableFunction::checkSourceAccess(ContextPtr context, bool is_insert_query) const
{
    if (const auto access_object = getSourceAccessObject())
    {
        AccessType type_to_check = AccessType::READ;
        if (is_insert_query)
            type_to_check = AccessType::WRITE;

        checkSourceAccessForURI(context, type_to_check, *access_object, getFunctionURI());
    }
}

void ITableFunction::checkSourceTableAccess(const ContextPtr & context, const StorageID & table_id, AccessType access_type) const
{
    context->checkAccess(access_type, table_id);
}

void ITableFunction::checkSourceStorageAccess(
    const ContextPtr & context, const StoragePtr & storage, const StorageID & table_id, AccessType access_type) const
{
    if (const auto * alias = storage ? storage->as<StorageAlias>() : nullptr;
        alias && !alias->isTargetTableGranted(context, access_type, {}))
        throw Exception(
            ErrorCodes::ACCESS_DENIED, "Not enough privileges to access the table that {} points to", table_id.getNameForLogs());
}

void ITableFunction::checkEngineAccess(ContextPtr context) const
{
    if (!requiresTableEngineGrant())
        return;

    const char * engine = isClusterFunction() ? getNonClusteredStorageEngineName() : getStorageEngineName();
    if (engine && *engine)
        context->checkAccess(AccessType::TABLE_ENGINE, engine);
}

ColumnsDescription ITableFunction::getActualTableStructureWithAccess(ContextPtr context, bool is_insert_query) const
{
    /// Resolving table structure is always a read operation.
    checkSourceAccess(context, /*is_insert_query*/ false);
    checkEngineAccess(context);
    checkSourceObjectAccess(context, /*for_structure=*/ true);
    return getActualTableStructure(context, is_insert_query);
}

StoragePtr ITableFunction::execute(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name,
                                   ColumnsDescription cached_columns, bool use_global_context, bool is_insert_query, bool check_create_temporary_table, bool check_source_access) const
{
    ProfileEvents::increment(ProfileEvents::TableFunctionExecute);

    if (check_source_access)
        checkSourceAccess(context, is_insert_query);

    /// Deliberately not under `check_source_access`: no caller takes over these checks.
    checkEngineAccess(context);
    checkSourceObjectAccess(context, /*for_structure=*/ false);

    auto table_function_properties = TableFunctionFactory::instance().tryGetProperties(getName());
    if (check_create_temporary_table && (is_insert_query || !(table_function_properties && table_function_properties->allow_readonly)))
        context->checkAccess(AccessType::CREATE_TEMPORARY_TABLE);

    auto context_to_use = use_global_context ? context->getGlobalContext() : context;

    if (cached_columns.empty())
        return executeImpl(ast_function, context, table_name, std::move(cached_columns), is_insert_query);

    if (hasStaticStructure() && cached_columns == getActualTableStructure(context, is_insert_query))
        return executeImpl(ast_function, context_to_use, table_name, std::move(cached_columns), is_insert_query);

    auto this_table_function = shared_from_this();
    auto get_storage = [=]() -> StoragePtr
    {
        return this_table_function->executeImpl(ast_function, context_to_use, table_name, cached_columns, is_insert_query);
    };

    /// It will request actual table structure and create underlying storage lazily
    return std::make_shared<StorageTableFunctionProxy>(StorageID(getDatabaseName(), table_name), std::move(get_storage),
                                                       std::move(cached_columns), needStructureConversion(),
                                                       hasUnreplicatedTableDataOnDisk());
}

String ITableFunction::normalizeURIForAccessCheck(const String & uri)
{
    try
    {
        Poco::URI parsed(uri);
        parsed.normalize();
        return parsed.toString();
    }
    catch (const Poco::Exception &)
    {
        return "";
    }
}

namespace
{

/// Glob alternatives (`{a,..}`, `a|..`) are expanded into separate locations after the check, so they count as segments.
bool hasDotSegment(std::string_view path)
{
    size_t begin = 0;
    for (size_t i = 0; i <= path.size(); ++i)
    {
        if (i == path.size() || std::string_view("/{},|").contains(path[i]))
        {
            const std::string_view segment = path.substr(begin, i - begin);
            if (segment == "." || segment == "..")
                return true;
            begin = i + 1;
        }
    }
    return false;
}

/// `%2A`, `%3F`, `%7B`, `%7D` in any case.
bool hasEscapedWildcard(std::string_view raw_path)
{
    for (size_t i = 0; i + 2 < raw_path.size(); ++i)
    {
        if (raw_path[i] != '%')
            continue;
        const char high = raw_path[i + 1];
        const char low = static_cast<char>(std::toupper(static_cast<unsigned char>(raw_path[i + 2])));
        if ((high == '2' && low == 'A') || (high == '3' && low == 'F') || (high == '7' && (low == 'B' || low == 'D')))
            return true;
    }
    return false;
}

/// What a source grant filter is matched against for `uri`, or an empty string when no filter can be trusted
/// with it, which leaves only the grant on the whole source.
String getSourceAccessSubject(AccessTypeObjects::Source source, const String & uri)
{
    /// A blob path and a local path are names, used as written.
    if (source == AccessTypeObjects::Source::AZURE || source == AccessTypeObjects::Source::FILE)
        return hasDotSegment(uri) ? String{} : uri;

    try
    {
        const Poco::URI decoded(uri);
        const String & path = decoded.getPath();
        if (hasDotSegment(path) || path.contains("//"))
            return {};
        const String raw_path = Poco::URI(uri, /* enable_url_encoding */ false).getPath();
        if (source == AccessTypeObjects::Source::S3)
        {
            /// `S3::URI` decodes the path once, as the subject does, but folds a query into the key, where `?`
            /// matches any character, and an escaped `*`, `?` or `{` becomes a wildcard of the key.
            if (uri.contains('?') || hasEscapedWildcard(raw_path))
                return {};
        }
        else if (raw_path.contains('%'))
            return {};
    }
    catch (const Poco::Exception &)
    {
        return {};
    }
    return ITableFunction::normalizeURIForAccessCheck(uri);
}

}

void ITableFunction::checkSourceAccessForURI(
    const ContextPtr & context, const AccessFlags & flags, AccessTypeObjects::Source source, const String & uri)
{
    context->getAccess()->checkAccessWithFilter(flags, toStringSource(source), getSourceAccessSubject(source, uri));
}

}
