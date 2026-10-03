#pragma once

#include <Core/Types.h>
#include <Core/Names.h>
#include <Interpreters/Context_fwd.h>
#include <Parsers/IAST_fwd.h>
#include <Parsers/IdentifierQuotingStyle.h>
#include <Parsers/LiteralEscapingStyle.h>

#include <optional>

namespace DB
{

/** The data source of an external SQL database integration (MySQL, PostgreSQL, SQLite, ...).
  * It is either a remote table name, or a user-provided SELECT query that is passed to the external
  * database as is. In the latter case the structure is inferred from the query result and the storage
  * is read-only.
  */
class TableNameOrQuery
{
public:
    enum class Type : uint8_t
    {
        TABLE,
        QUERY,
    };

    TableNameOrQuery() : type(Type::TABLE) {}
    TableNameOrQuery(Type type_, const String & target_) : type(type_), target(target_) {}

    Type getType() const { return type; }

    const String & getTableName() const;
    const String & getQuery() const;

    bool isTable() const { return type == Type::TABLE; }
    bool isQuery() const { return type == Type::QUERY; }

    /// Human-readable representation for error messages and logging.
    String format() const;

private:
    Type type;
    String target;
};

/** Detect whether a table-function/engine argument is a query rather than a table name.
  * Two syntaxes are accepted:
  *   - a subquery:        `(SELECT ...)`
  *   - a `query` call:    `query('SELECT ...')`
  * Returns the query text if either matches, otherwise std::nullopt (the argument is an ordinary table name).
  *
  * The subquery form is parsed by the ClickHouse parser and re-serialized before being sent to the external
  * database, so it is formatted with the external database's identifier-quoting and string-literal-escaping
  * style (e.g. double quotes for PostgreSQL/SQLite, backticks for MySQL). Syntax that cannot be represented as
  * ClickHouse SQL must use the `query('...')` form instead, which is passed through verbatim.
  */
std::optional<String> tryGetExternalDatabaseQuery(
    const ASTPtr & argument,
    const ContextPtr & context,
    IdentifierQuotingStyle identifier_quoting_style,
    LiteralEscapingStyle literal_escaping_style);

/** Build `SELECT <columns> FROM (<query>) AS __subquery`, projecting the requested columns from the result
  * of a user-provided query. Identifiers are quoted according to the given style of the external database.
  */
String buildQueryForExternalDatabaseSubquery(
    const String & query, const Names & columns, IdentifierQuotingStyle identifier_quoting_style);

}
