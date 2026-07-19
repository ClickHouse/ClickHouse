#pragma once

#include <Interpreters/StorageID.h>
#include <Parsers/IAST.h>

class SipHash;

namespace DB
{

class ReadBuffer;

/* Useful for expressions like COPY table_name FROM/TO output_file.
 * This AST node is relevant only for Postgres wire protocol.
 * For more information see https://www.postgresql.org/docs/current/sql-copy.html
 */
class ASTCopyQuery : public IAST
{
public:
    enum class QueryType : uint8_t
    {
        COPY_FROM = 0,
        COPY_TO = 1,
    } type{};

    String table_name;
    /// When the query is of the form `COPY (query) TO STDOUT` - used by libpq/pqxx to stream an
    /// arbitrary result set - this holds the SQL text of the inner query to run instead of `table_name`.
    String subquery;
    Strings column_names;
    enum class Formats : uint8_t
    {
        TSV,
        CSV,
        Binary
    } format = Formats::TSV;

    /// A human-readable reason set by the parser when the `COPY` command carries a data-formatting option we
    /// cannot faithfully honor (a non-default `DELIMITER`, a non-default `NULL` marker, `HEADER`, or any
    /// option we do not interpret). The handler rejects such a command with a clean `ErrorResponse` instead
    /// of silently ignoring the option and producing output that does not match what the client asked for.
    /// Empty when there is nothing to reject.
    String unsupported_option;

    String getID(char) const override { return "CopyQuery"; }

    ASTPtr clone() const override;

    QueryKind getQueryKind() const override { return QueryKind::Copy; }

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;
};

String toString(ASTCopyQuery::Formats format);

}
