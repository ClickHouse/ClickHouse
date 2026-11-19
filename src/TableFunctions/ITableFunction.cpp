#include <Access/ContextAccess.h>
#include <TableFunctions/ITableFunction.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Functions/FunctionFactory.h>
#include <Storages/StorageAlias.h>
#include <Storages/StorageFactory.h>
#include <Storages/StorageTableFunction.h>
#include <Access/Common/AccessFlags.h>
#include <Common/ProfileEvents.h>
#include <Common/StringUtils.h>
#include <TableFunctions/TableFunctionFactory.h>
#include <Interpreters/Context.h>
#include <Poco/URI.h>

#include <algorithm>


namespace ProfileEvents
{
    extern const Event TableFunctionExecute;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
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
    if (const String engine = getEngineNameForAccessCheck(); !engine.empty())
        context->checkAccess(AccessType::TABLE_ENGINE, engine);
}

String ITableFunction::getEngineNameForAccessCheck() const
{
    /// A cluster function is checked as its non-clustered engine. It is empty when the function has no engine.
    const char * engine = isClusterFunction() ? getNonClusteredStorageEngineName() : getStorageEngineName();
    return engine ? engine : "";
}

void ITableFunction::checkAccessToCreateTableTarget(const ASTPtr & table_function_ast, ContextPtr context)
{
    if (!table_function_ast)
        return;

    /// A parameterized view is called like a table function in a query; the analyzer resolves it as a view.
    const auto & factory = TableFunctionFactory::instance();
    const auto & name = table_function_ast->as<ASTFunction &>().name;
    if (!factory.isTableFunctionName(name))
        return;

    /// The same veto as for the outermost function of a definition, see `InterpreterCreateQuery`.
    const auto unparsed = factory.tryGet(name, context);
    if (unparsed->dependsOnCurrentUserGrants())
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Table function '{}' cannot be used to create a table, neither directly nor nested in another table "
            "function or in a table engine argument",
            name);

    /// A grant on a source is matched against the argument that names the source, the first one, or the second one
    /// of a cluster function, or against an override of a named collection, `key = value` with any spelling of the
    /// key, so they must mean here what they mean where the query runs. The other arguments, e.g. a sharding key, may vary.
    if (const auto & function = table_function_ast->as<ASTFunction &>(); function.arguments && unparsed->getSourceAccessObject())
    {
        const auto & arguments = function.arguments->children;
        const size_t source_index = unparsed->isClusterFunction() ? 1 : 0;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const auto * assignment = arguments[i]->as<ASTFunction>();
            if (i == source_index || (assignment && assignment->name == "equals"))
                throwIfValueVariesByContext(*arguments[i], context);
        }
    }

    TableFunctionPtr table_function;
    try
    {
        table_function = factory.get(table_function_ast, context);
    }
    catch (Exception & e)
    {
        /// An argument that resolves only within its query, e.g. a URL bound by `WITH`, cannot be checked here.
        e.addMessage("while checking the grants to persist the table function `{}` with its arguments as written", name);
        throw;
    }
    table_function->checkAccessToCreateTable(context);
}

void ITableFunction::throwIfValueVariesByContext(const IAST & value, const ContextPtr & context)
{
    if (value.as<ASTSelectWithUnionQuery>())
        return;
    if (value.as<ASTSubquery>())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "A subquery in an argument of a table function persisted in a table cannot be checked");

    if (const auto * function = value.as<ASTFunction>())
        if (const auto resolver = FunctionFactory::instance().tryGet(function->name, context); resolver && !resolver->isDeterministic())
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "An argument of a table function persisted in a table uses `{}`, whose value depends on where the query runs, "
                "and cannot be checked",
                function->name);

    for (const auto & child : value.children)
        throwIfValueVariesByContext(*child, context);
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

void ITableFunction::checkAccessToExecute(ContextPtr context, bool is_insert_query, bool check_create_temporary_table, bool check_source_access) const
{
    if (check_source_access)
        checkSourceAccess(context, is_insert_query);

    /// Deliberately not under `check_source_access`: no caller takes over these checks.
    checkEngineAccessIfRequired(context);
    checkSourceObjectAccess(context, /*for_structure=*/ false);

    if (check_create_temporary_table)
        checkCreateTemporaryTableAccess(context, is_insert_query);
}

StoragePtr ITableFunction::execute(const ASTPtr & ast_function, ContextPtr context, const std::string & table_name,
                                   ColumnsDescription cached_columns, bool use_global_context, bool is_insert_query, bool check_create_temporary_table, bool check_source_access) const
{
    ProfileEvents::increment(ProfileEvents::TableFunctionExecute);
    checkAccessToExecute(context, is_insert_query, check_create_temporary_table, check_source_access);

    auto context_to_use = use_global_context ? context->getGlobalContext() : context;

    StoragePtr storage;
    if (!isTableDefinition(cached_columns))
    {
        storage = executeImpl(ast_function, context, table_name, std::move(cached_columns), is_insert_query);
    }
    else if (hasStaticStructure() && cached_columns == getActualTableStructure(context, is_insert_query))
    {
        storage = executeImpl(ast_function, context_to_use, table_name, std::move(cached_columns), is_insert_query);
    }
    else
    {
        auto this_table_function = shared_from_this();
        auto get_storage = [=]() -> StoragePtr
        {
            return this_table_function->executeImpl(ast_function, context_to_use, table_name, cached_columns, is_insert_query);
        };

        /// It will request actual table structure and create underlying storage lazily
        storage = std::make_shared<StorageTableFunctionProxy>(StorageID(getDatabaseName(), table_name), std::move(get_storage),
                                                              std::move(cached_columns), needStructureConversion(),
                                                              hasUnreplicatedTableDataOnDisk());
    }

    /// The source access check covered only the URI as typed, but a write can go to a destination computed at write time:
    /// each partition of `PARTITION BY`, or a key renamed by `s3_create_new_file_on_insert`. The storage checks those.
    /// Without `check_source_access`, the caller does the checks, e.g. `url('s3://...')` for its delegate.
    /// `INSERT` passes no cached columns, so the check is never set on a lazy `StorageTableFunctionProxy`, which would not pass it on.
    /// Without a URI, only an unfiltered grant passed the source access check, and it allows every destination.
    ///
    /// A table created with `CREATE TABLE ... AS s3(...)` stays after the query and survives restarts (it uses the global context),
    /// and is the same as one created with `ENGINE = S3`: users insert into it with `INSERT` on the table, not with source grants.
    /// Its creator needed `TABLE ENGINE ON S3` instead, so the check is not set on it.
    if (is_insert_query && check_source_access && !use_global_context)
    {
        const auto access_object = getSourceAccessObject();
        if (access_object && !getFunctionURI().empty())
        {
            /// The same check as for the typed URI, so that a destination is refused whenever typing it would be.
            storage->setPartitionedWriteAccessCheck(
                [source = *access_object](const ContextPtr & write_context, const String & uri)
                { checkSourceAccessForURI(write_context, AccessType::WRITE, source, uri); },
                IStorage::PartitionedWriteAccessKey{});
        }
    }

    return storage;
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
