#include <iomanip>

#include <Common/logger_useful.h>
#include <Common/SipHash.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTQueryWithOutput.h>
#include <Parsers/ASTSelectIntersectExceptQuery.h>
#include <Parsers/ASTSelectQuery.h>
#include <Parsers/ASTSelectWithUnionQuery.h>
#include <Parsers/ASTSetQuery.h>
#include <Parsers/ASTSubquery.h>
#include <Parsers/InsertQuerySettingsPushDownVisitor.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>
#include <unordered_map>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_USAGE_OF_INPUT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

void mergeSettingsAstWithOverride(ASTPtr & target_ast, const ASTPtr & source_ast)
{
    if (!source_ast)
        return;

    if (!target_ast)
    {
        target_ast = source_ast->clone();
        return;
    }

    auto & source_settings = source_ast->as<ASTSetQuery &>();
    auto & target_settings = target_ast->as<ASTSetQuery &>();

    std::unordered_map<String, size_t> target_change_positions;
    target_change_positions.reserve(target_settings.changes.size());
    for (size_t i = 0; i < target_settings.changes.size(); ++i)
        target_change_positions[target_settings.changes[i].name] = i;

    std::unordered_map<String, size_t> target_default_positions;
    target_default_positions.reserve(target_settings.default_settings.size());
    for (size_t i = 0; i < target_settings.default_settings.size(); ++i)
        target_default_positions[target_settings.default_settings[i]] = i;

    auto eraseDefault = [&](const String & name)
    {
        auto it = target_default_positions.find(name);
        if (it == target_default_positions.end())
            return;

        target_settings.default_settings.erase(target_settings.default_settings.begin() + it->second);
        target_default_positions.clear();
        for (size_t i = 0; i < target_settings.default_settings.size(); ++i)
            target_default_positions[target_settings.default_settings[i]] = i;
    };

    auto eraseChange = [&](const String & name)
    {
        auto it = target_change_positions.find(name);
        if (it == target_change_positions.end())
            return;

        target_settings.changes.erase(target_settings.changes.begin() + it->second);
        target_change_positions.clear();
        for (size_t i = 0; i < target_settings.changes.size(); ++i)
            target_change_positions[target_settings.changes[i].name] = i;
    };

    for (const auto & change : source_settings.changes)
    {
        eraseDefault(change.name);
        if (auto it = target_change_positions.find(change.name); it != target_change_positions.end())
            target_settings.changes[it->second] = change;
        else
        {
            target_settings.changes.push_back(change);
            target_change_positions[change.name] = target_settings.changes.size() - 1;
        }
    }

    for (const auto & default_setting : source_settings.default_settings)
    {
        eraseChange(default_setting);
        if (target_default_positions.contains(default_setting))
            continue;

        target_settings.default_settings.push_back(default_setting);
        target_default_positions[default_setting] = target_settings.default_settings.size() - 1;
    }
}

void collectTopLevelSourceSettings(const ASTPtr & select, ASTPtr & target_settings_ast)
{
    auto collect_top_level_impl = [&](auto && self, const ASTPtr & current, bool allow_subquery_unwrap) -> void
    {
        if (!current)
            return;

        if (const auto * select_with_union = current->as<ASTSelectWithUnionQuery>())
        {
            if (!select_with_union->list_of_selects)
                return;

            /// Match standalone SELECT precedence:
            /// set-op-level SETTINGS apply first, then the last first-order arm overrides duplicates.
            mergeSettingsAstWithOverride(target_settings_ast, select_with_union->settings_ast);
            const auto & children = select_with_union->list_of_selects->children;
            if (!children.empty())
                self(self, children.back(), true);
            return;
        }

        if (const auto * intersect_except = current->as<ASTSelectIntersectExceptQuery>())
        {
            mergeSettingsAstWithOverride(target_settings_ast, intersect_except->settings());
            auto children = intersect_except->getListOfSelects();
            if (!children.empty())
                self(self, children.back(), true);
            return;
        }

        if (allow_subquery_unwrap)
        {
            if (const auto * subquery = current->as<ASTSubquery>())
            {
                self(self, subquery->children.empty() ? ASTPtr{} : subquery->children.front(), false);
                return;
            }
        }

        if (const auto * query_with_output = dynamic_cast<const ASTQueryWithOutput *>(current.get()))
            mergeSettingsAstWithOverride(target_settings_ast, query_with_output->settings_ast);

        if (const auto * select_query = current->as<ASTSelectQuery>())
            mergeSettingsAstWithOverride(target_settings_ast, select_query->settings());
    };

    collect_top_level_impl(collect_top_level_impl, select, true);
}

void rebuildInsertReturningSourceSettings(ASTInsertQuery & query)
{
    if (!query.returning_select || !query.select)
        return;

    query.source_select_settings_runtime_ast = query.source_select_pre_returning_settings_ast
        ? query.source_select_pre_returning_settings_ast->clone()
        : ASTPtr{};
    mergeSettingsAstWithOverride(query.source_select_settings_runtime_ast, query.source_select_settings_ast);
    InsertQuerySettingsPushDownVisitor::Data visitor_data{query.source_select_settings_runtime_ast};
    InsertQuerySettingsPushDownVisitor(visitor_data).visit(query.select);

    ASTPtr source_top_level_settings_ast = query.source_select_pre_returning_settings_ast
        ? query.source_select_pre_returning_settings_ast->clone()
        : ASTPtr{};
    collectTopLevelSourceSettings(query.select, source_top_level_settings_ast);
    mergeSettingsAstWithOverride(source_top_level_settings_ast, query.source_select_settings_ast);
    query.source_select_settings_global_ast = source_top_level_settings_ast;
}

}

String ASTInsertQuery::getDatabase() const
{
    String name;
    tryGetIdentifierNameInto(database, name);
    return name;
}

String ASTInsertQuery::getTable() const
{
    String name;
    tryGetIdentifierNameInto(table, name);
    return name;
}

void ASTInsertQuery::setDatabase(const String & name)
{
    reset(database);
    if (!name.empty())
        set(database, make_intrusive<ASTIdentifier>(name));
}

void ASTInsertQuery::setTable(const String & name)
{
    reset(table);
    if (!name.empty())
        set(table, make_intrusive<ASTIdentifier>(name));
}

void ASTInsertQuery::writeJSON(WriteBuffer & out) const
{
    /// Inline data (`INSERT INTO t VALUES (1)`, `INSERT ... FORMAT ... <payload>`) is represented as a
    /// non-owning `data`..`end` view into the original query buffer and/or an external streaming `tail`
    /// `ReadBuffer`. None of it is reproduced by `formatImpl` (which only prints `FORMAT <format>`/`VALUES`),
    /// and `tail` cannot be serialized at all. Rather than emit lossy JSON, reject such queries.
    if (data || tail)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "INSERT with inline data is not supported by parseQueryToJSON during AST JSON deserialization");

    JSONObjectWriter w(out, "InsertQuery");

    if (!table_id.database_name.empty())
        w.writeString("database_name", table_id.database_name);
    if (!table_id.table_name.empty())
        w.writeString("table_name", table_id.table_name);

    w.writeChild("database", database);
    w.writeChild("table", table);

    if (!format.empty())
        w.writeString("format", format);

    if (async_insert_flush)
        w.writeBool("async_insert_flush", true);

    w.writeChild("columns", columns);
    w.writeChild("table_function", table_function);
    w.writeChild("partition_by", partition_by);
    w.writeChild("settings_ast", settings_ast);
    w.writeChild("select", select);
    w.writeChild("source_select_pre_returning_settings_ast", source_select_pre_returning_settings_ast);
    w.writeChild("returning_select", returning_select);
    w.writeChild("source_select_settings_ast", source_select_settings_ast);
    w.writeChild("infile", infile);
    w.writeChild("compression", compression);
}

void ASTInsertQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    table_id.database_name = r.getString("database_name");
    table_id.table_name = r.getString("table_name");

    /// `database`/`table` are parser-produced identifiers; `getDatabase`/`getTable` read them via
    /// `tryGetIdentifierNameInto`, so reject other node types here.
    auto db_child = r.readIdentifierChild("database");
    if (db_child)
    {
        database = db_child;
        children.push_back(database);
    }

    auto tbl_child = r.readIdentifierChild("table");
    if (tbl_child)
    {
        table = tbl_child;
        children.push_back(table);
    }

    format = r.getString("format");
    async_insert_flush = r.getBool("async_insert_flush");

    /// `columns` is parser-produced as an `ASTExpressionList` (the INSERT column list).
    /// `formatImpl` prints it and `processColumnTransformers` iterates `columns->children`,
    /// so a non-`ASTExpressionList` from malformed `clickhouse_json` (e.g. a bare identifier)
    /// would format as a column list while exposing an empty child list to execution. Reject it.
    auto child = r.readChildOfType<ASTExpressionList>("columns");
    if (child)
    {
        columns = child;
        children.push_back(columns);
    }

    /// `table_function` is parser-owned as an `ASTFunction` (`INSERT INTO FUNCTION ...`).
    /// `ClientBase::setInsertionTable` and `formatImpl` downcast it with `as<ASTFunction>()`,
    /// so a non-`ASTFunction` from malformed `clickhouse_json` must be rejected here.
    child = r.readChildOfType<ASTFunction>("table_function");
    if (child)
    {
        table_function = child;
        children.push_back(table_function);
    }

    child = r.readChild("partition_by");
    if (child)
    {
        partition_by = child;
        children.push_back(partition_by);
    }

    /// Query-local `SETTINGS` clauses are parsed as `ASTSetQuery`. `InterpreterSetQuery`,
    /// `InsertQuerySettingsPushDownVisitor`, and `DDLTask` downcast `settings_ast` with
    /// `as<ASTSetQuery>()`, so a non-`ASTSetQuery` from malformed `clickhouse_json` must be
    /// rejected here instead of reaching those downcasts.
    child = r.readChildOfType<ASTSetQuery>("settings_ast");
    if (child)
    {
        settings_ast = child;
        children.push_back(settings_ast);
    }

    /// `select` is parser-produced as an `ASTSelectWithUnionQuery` (`INSERT ... SELECT`). Insert
    /// execution downcasts it (`applyTrivialInsertSelectOptimization`, the distributed-insert paths),
    /// so reject any other node type from malformed `clickhouse_json` here.
    child = r.readChildOfType<ASTSelectWithUnionQuery>("select");
    if (child)
    {
        select = child;
        children.push_back(select);
    }

    child = r.readChildOfType<ASTSetQuery>("source_select_pre_returning_settings_ast");
    if (child)
    {
        source_select_pre_returning_settings_ast = child;
        children.push_back(source_select_pre_returning_settings_ast);
    }

    child = r.readChildOfType<ASTSelectWithUnionQuery>("returning_select");
    if (child)
    {
        returning_select = child;
        children.push_back(returning_select);
    }

    child = r.readChildOfType<ASTSetQuery>("source_select_settings_ast");
    if (child)
    {
        source_select_settings_ast = child;
        children.push_back(source_select_settings_ast);
    }

    /// `FROM INFILE`/`COMPRESSION` are both string `ASTLiteral`s in the SQL grammar, and
    /// `COMPRESSION` is only valid when `INFILE` is present. `formatImpl`, `ClientBase`,
    /// `AsynchronousInsertQueue` and `getReadBufferFromASTInsertQuery` later downcast these
    /// with `as<ASTLiteral &>()` and read them as strings, so a wrong node type or a
    /// non-string literal from malformed `clickhouse_json` must be rejected at the boundary.
    child = r.readChildOfType<ASTLiteral>("infile");
    if (child)
    {
        if (child->as<ASTLiteral &>().value.getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'infile' must be a string literal during AST JSON deserialization");
        infile = child;
        children.push_back(infile);
    }

    child = r.readChildOfType<ASTLiteral>("compression");
    if (child)
    {
        if (!infile)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'compression' is only valid together with 'infile' during AST JSON deserialization");
        if (child->as<ASTLiteral &>().value.getType() != Field::Types::String)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "'compression' must be a string literal during AST JSON deserialization");
        compression = child;
        children.push_back(compression);
    }

    /// `partition_by` is parser-produced only in the `INSERT INTO FUNCTION` branch
    /// (`ParserInsertQuery`); `formatImpl` hides it for ordinary inserts, but
    /// `InterpreterInsertQuery::execute` still applies it, so an ordinary insert carrying a hidden
    /// `partition_by` would execute against a clause the formatted SQL cannot show. Reject it.
    if (partition_by && !table_function)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'partition_by' is only valid for INSERT INTO FUNCTION (requires 'table_function') during AST JSON deserialization");

    if (source_select_settings_ast && !returning_select)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'source_select_settings_ast' is only valid together with 'returning_select' during AST JSON deserialization");
    if (source_select_settings_ast && !select)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'source_select_settings_ast' is only valid for INSERT ... SELECT ... RETURNING during AST JSON deserialization");
    if (source_select_pre_returning_settings_ast && !returning_select)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'source_select_pre_returning_settings_ast' is only valid together with 'returning_select' during AST JSON deserialization");
    if (source_select_pre_returning_settings_ast && !select)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "'source_select_pre_returning_settings_ast' is only valid for INSERT ... SELECT ... RETURNING during AST JSON deserialization");

    /// `source_select_settings_runtime_ast` / `source_select_settings_global_ast` are parser-derived
    /// execution carriers and are intentionally not serialized. Rebuild them so `clickhouse_json`
    /// execution preserves source-only semantics and fail-close checks.
    rebuildInsertReturningSourceSettings(*this);

    /// The parser produces exactly one destination form: `INSERT INTO FUNCTION f(...)` (table_function)
    /// or `INSERT INTO [db.]t` (the `database`/`table` identifiers; `table_id` is the normalized
    /// equivalent populated later). `formatImpl` picks one by precedence (function > table_id >
    /// database/table) while `getTable`/`getDatabase` and insertion context may read a different one,
    /// so multiple forms would let the displayed target diverge from the executed one. Require exactly one.
    const size_t destinations = (table_function ? 1 : 0)
        + (table_id.empty() ? 0 : 1)
        + ((database || table) ? 1 : 0);
    if (destinations != 1)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`InsertQuery` must specify exactly one destination form ('table_function', 'table_id', or 'database'/'table') during AST JSON deserialization");

    /// In the `database`/`table` form, `formatImpl` requires a table (`chassert(table)`); a bare
    /// `database` without a `table` is not a valid target.
    if (database && !table)
        throw Exception(ErrorCodes::BAD_ARGUMENTS,
            "`InsertQuery` 'database' requires a 'table' during AST JSON deserialization");
}

void ASTInsertQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    ostr << "INSERT INTO" << " ";
    if (table_function)
    {
        ostr << "FUNCTION" << " ";
        table_function->format(ostr, settings, state, frame);
        if (partition_by)
        {
            ostr << " " << "PARTITION BY" << " ";
            partition_by->format(ostr, settings, state, frame);
        }
    }
    else if (table_id)
    {
        ostr << (!table_id.database_name.empty() ? backQuoteIfNeed(table_id.database_name) + "." : "") << backQuoteIfNeed(table_id.table_name);
    }
    else
    {
        if (database)
        {
            database->format(ostr, settings, state, frame);
            ostr << '.';
        }

        chassert(table);
        table->format(ostr, settings, state, frame);
    }

    if (columns)
    {
        ostr << " (";
        columns->format(ostr, settings, state, frame);
        ostr << ")";
    }

    if (infile)
    {
        ostr
            << " "
            << "FROM INFILE"

            << " " << quoteString(infile->as<ASTLiteral &>().value.safeGet<std::string>());
        if (compression)
            ostr
                << " "
                << "COMPRESSION"

                << " " << quoteString(compression->as<ASTLiteral &>().value.safeGet<std::string>());
    }

    if (settings_ast)
    {
        ostr << settings.nl_or_ws << "SETTINGS" << " ";
        settings_ast->format(ostr, settings, state, frame);
    }

    if (returning_select && !select)
    {
        ostr << settings.nl_or_ws << "RETURNING" << " (";
        returning_select->format(ostr, settings, state, frame);
        ostr << ")";
    }

    /// Compatibility for INSERT without SETTINGS to format in oneline, i.e.:
    ///
    ///     INSERT INTO foo VALUES
    ///
    /// But
    ///
    ///     INSERT INTO foo
    ///     SETTINGS max_threads=1
    ///     VALUES
    ///
    char delim = settings_ast ? settings.nl_or_ws : ' ';

    if (select)
    {
        ostr << delim;
        /// Disable FROM-first syntax to avoid parsing ambiguity with INSERT ... FROM INFILE.
        /// Only affects the immediate SELECT, not nested subqueries.
        bool was_disable_from_first_syntax = frame.disable_from_first_syntax;
        frame.disable_from_first_syntax = true;
        select->format(ostr, settings, state, frame);
        frame.disable_from_first_syntax = was_disable_from_first_syntax;

        /// For INSERT ... SELECT ... FROM input('...') FORMAT Values,
        /// the FORMAT clause must be preserved in the formatted output.
        if (!format.empty())
        {
            ostr << delim
                << "FORMAT" << " " << format;
        }

        if (returning_select)
        {
            if (source_select_pre_returning_settings_ast)
            {
                ostr << settings.nl_or_ws << "SETTINGS" << " ";
                source_select_pre_returning_settings_ast->format(ostr, settings, state, frame);
            }

            ostr << settings.nl_or_ws << "RETURNING" << " (";
            returning_select->format(ostr, settings, state, frame);
            ostr << ")";
        }

        if (source_select_settings_ast)
        {
            ostr << settings.nl_or_ws << "SETTINGS" << " ";
            source_select_settings_ast->format(ostr, settings, state, frame);
        }
    }
    else
    {
        if (!format.empty())
        {
            ostr << delim
                << "FORMAT" << " " << format;
        }
        else if (!infile)
        {
            ostr << delim
                << "VALUES";
        }
    }
}

void ASTInsertQuery::updateTreeHashImpl(SipHash & hash_state, bool ignore_aliases) const
{
    hash_state.update(table_id.database_name);
    hash_state.update(table_id.table_name);
    hash_state.update(table_id.uuid);
    hash_state.update(format);
    if (source_select_settings_ast)
        source_select_settings_ast->updateTreeHash(hash_state, ignore_aliases);
    IAST::updateTreeHashImpl(hash_state, ignore_aliases);
}


static void tryFindInputFunctionImpl(const ASTPtr & ast, ASTPtr & input_function)
{
    if (!ast)
        return;
    for (const auto & child : ast->children)
        tryFindInputFunctionImpl(child, input_function);

    if (const auto * table_function_ast = ast->as<ASTFunction>())
    {
        if (table_function_ast->name == "input")
        {
            if (input_function)
                throw Exception(ErrorCodes::INVALID_USAGE_OF_INPUT, "You can use the `input` function only once in a query.");
            input_function = ast;
        }
    }
}


void ASTInsertQuery::tryFindInputFunction(ASTPtr & input_function) const
{
    tryFindInputFunctionImpl(select, input_function);
}

}
