#include <Access/ContextAccess.h>
#include <TableFunctions/ITableFunction.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageTableFunction.h>
#include <Access/Common/AccessFlags.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/Context.h>

#include <algorithm>


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

void ITableFunction::checkEngineAccessIfRequired(ContextPtr context) const
{
    if (requiresTableEngineGrant())
        checkEngineAccess(context);
}

void ITableFunction::checkSourceTableAccess(const ContextPtr & context, const StorageID & table_id, AccessType access_type) const
{
    context->checkAccess(access_type, table_id);
}

void ITableFunction::checkCreateTemporaryTableAccess(ContextPtr context, bool is_insert_query) const
{
    auto table_function_properties = TableFunctionFactory::instance().tryGetProperties(getName());
    if (is_insert_query || !(table_function_properties && table_function_properties->allow_readonly))
        context->checkAccess(AccessType::CREATE_TEMPORARY_TABLE);
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
    const char * engine = isClusterFunction() ? getNonClusteredStorageEngineName() : getStorageEngineName();
    if (engine && *engine)
        context->checkAccess(AccessType::TABLE_ENGINE, engine);
}

void ITableFunction::checkAccessToCreateTableTarget(const ASTPtr & table_function_ast, ContextPtr context)
{
    if (table_function_ast)
        TableFunctionFactory::instance().get(table_function_ast, context)->checkAccessToCreateTable(context);
}

ColumnsDescription ITableFunction::getActualTableStructureWithAccess(ContextPtr context, bool is_insert_query) const
{
    /// Resolving table structure is always a read operation.
    checkSourceAccess(context, /*is_insert_query*/ false);
    checkEngineAccessIfRequired(context);
    checkSourceObjectAccess(context, /*for_structure=*/ true);
    /// Resolving the structure may access the source (e.g. schema inference fetches a URL), so it must be
    /// gated in the same way as `execute`: otherwise `DESCRIBE TABLE url(...)` would let a user who cannot
    /// run the table function (e.g. under `readonly = 1`) make the server request an endpoint.
    checkCreateTemporaryTableAccess(context, /*is_insert_query*/ false);
    return getActualTableStructure(context, is_insert_query);
}

StoragePtr ITableFunction::execute(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name,
                                   ColumnsDescription cached_columns, bool use_global_context, bool is_insert_query, bool check_create_temporary_table, bool check_source_access) const
{
    ProfileEvents::increment(ProfileEvents::TableFunctionExecute);

    if (check_source_access)
        checkSourceAccess(context, is_insert_query);

    /// Deliberately not under `check_source_access`: no caller takes over these checks.
    checkEngineAccessIfRequired(context);
    checkSourceObjectAccess(context, /*for_structure=*/ false);

    if (check_create_temporary_table)
        checkCreateTemporaryTableAccess(context, is_insert_query);

    auto context_to_use = use_global_context ? context->getGlobalContext() : context;

    if (!isTableDefinition(cached_columns))
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

/// Glob characters are expanded after the check, so the filter sees them as written. `toString` encodes a `%` of the
/// path as `%25`, so before the query these escapes can only come from a glob character. `url` also sends `,` as written.
String unescapeGlobCharacters(const String & subject, bool unescape_comma)
{
    const size_t query = std::min(subject.find('?'), subject.size());
    String result;
    result.reserve(subject.size());
    for (size_t i = 0; i < subject.size(); ++i)
    {
        if (subject[i] == '%' && i + 2 < query)
        {
            const std::string_view escape(subject.data() + i + 1, 2);
            char c = 0;
            if (escape == "2A")
                c = '*';
            else if (escape == "3F")
                c = '?';
            else if (escape == "7B")
                c = '{';
            else if (escape == "7D")
                c = '}';
            else if (escape == "7C")
                c = '|';
            else if (unescape_comma && escape == "2C")
                c = ',';
            if (c)
            {
                result += c;
                i += 2;
                continue;
            }
        }
        result += subject[i];
    }
    return result;
}

String escapeGlobCharacters(const String & name)
{
    String result;
    result.reserve(name.size());
    for (const char c : name)
    {
        if (c == '*')
            result += "%2A";
        else if (c == '?')
            result += "%3F";
        else if (c == '{')
            result += "%7B";
        else if (c == '}')
            result += "%7D";
        else if (c == '|')
            result += "%7C";
        else
            result += c;
    }
    return result;
}

/// Filter subjects for `uri`: glob characters as written and escaped. Both empty: only the whole-source grant passes.
std::pair<String, String> getSourceAccessSubjects(AccessTypeObjects::Source source, const String & uri)
{
    /// A blob path and a local path are names, used as written, but their dot and empty segments may be resolved on access.
    if (source == AccessTypeObjects::Source::AZURE || source == AccessTypeObjects::Source::FILE)
    {
        if (hasDotSegment(uri) || uri.contains("//"))
            return {};
        return {uri, escapeGlobCharacters(uri)};
    }

    /// A fragment is never part of the accessed location.
    if (uri.contains('#'))
        return {};

    try
    {
        const Poco::URI decoded(uri);
        const String & path = decoded.getPath();
        if (hasDotSegment(path) || path.contains("//"))
            return {};
        /// `url` expands `{}` and splits `|` failover across the whole URI, but the subject escapes them only in the path.
        if ((decoded.getScheme() + decoded.getAuthority() + decoded.getRawQuery()).find_first_of("{}|") != String::npos)
            return {};
        if (source == AccessTypeObjects::Source::S3)
        {
            /// `S3::URI` decodes the path once, as the subject does, but folds a query into the key, where `?` is a wildcard.
            if (uri.contains('?'))
                return {};
        }
        else if (source == AccessTypeObjects::Source::URL)
        {
            /// `url` sends the path as written, so a character the subject escapes, other than glob syntax, is refused.
            const String raw_path = Poco::URI(uri, /* enable_url_encoding */ false).getPath();
            if (!std::ranges::all_of(
                    raw_path, [](char c) { return isAlphaNumericASCII(c) || std::string_view("-._~/&+;=:@*{}|,").contains(c); }))
                return {};
        }
        else if (Poco::URI(uri, /* enable_url_encoding */ false).getPath().contains('%'))
            return {};
    }
    catch (const Poco::Exception &)
    {
        return {};
    }

    String escaped = ITableFunction::normalizeURIForAccessCheck(uri);
    String as_written = unescapeGlobCharacters(escaped, /* unescape_comma */ source == AccessTypeObjects::Source::URL);
    return {std::move(as_written), std::move(escaped)};
}

}

void ITableFunction::checkSourceAccessForURI(
    const ContextPtr & context, const AccessFlags & flags, AccessTypeObjects::Source source, const String & uri)
{
    const auto access = context->getAccess();
    const auto source_name = toStringSource(source);
    auto [subject, escaped_subject] = getSourceAccessSubjects(source, uri);
    /// Globs expand after the check, so they must match as written and escaped: `.` in `report.csv` matches `*`, not its expansion.
    if (subject != escaped_subject && !access->isGrantedWithFilter(flags, source_name, escaped_subject))
        subject.clear();
    access->checkAccessWithFilter(flags, source_name, subject);
}

}
