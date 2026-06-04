#include <iomanip>

#include <Common/logger_useful.h>
#include <Common/SipHash.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTInsertQuery.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTLiteral.h>
#include <Common/quoteString.h>
#include <IO/WriteHelpers.h>
#include <IO/Operators.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int INVALID_USAGE_OF_INPUT;
    extern const int BAD_ARGUMENTS;
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
    w.writeChild("infile", infile);
    w.writeChild("compression", compression);
}

void ASTInsertQuery::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);

    table_id.database_name = r.getString("database_name");
    table_id.table_name = r.getString("table_name");

    auto db_child = r.readChild("database");
    if (db_child)
    {
        database = db_child;
        children.push_back(database);
    }

    auto tbl_child = r.readChild("table");
    if (tbl_child)
    {
        table = tbl_child;
        children.push_back(table);
    }

    format = r.getString("format");
    async_insert_flush = r.getBool("async_insert_flush");

    auto child = r.readChild("columns");
    if (child)
    {
        columns = child;
        children.push_back(columns);
    }

    child = r.readChild("table_function");
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

    child = r.readChild("settings_ast");
    if (child)
    {
        settings_ast = child;
        children.push_back(settings_ast);
    }

    child = r.readChild("select");
    if (child)
    {
        select = child;
        children.push_back(select);
    }

    child = r.readChild("infile");
    if (child)
    {
        infile = child;
        children.push_back(infile);
    }

    child = r.readChild("compression");
    if (child)
    {
        compression = child;
        children.push_back(compression);
    }

    /// `formatImpl` falls through to the `chassert(table); table->format(...)` path when no other target is set.
    /// Require at least one valid target so deserialized AST cannot crash on formatting.
    if (!table_function && !table_id && !table)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "`InsertQuery` must specify at least one of 'table_function', non-empty 'table_id', or 'table' during AST JSON deserialization");
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
