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
    /// The text is rendered with the secrets it contains (such as the password of `remote`) so that it
    /// can be run as-is, which is why it must not be logged or shown: formatting uses `subquery_ast`.
    String subquery;
    /// The inner query of `COPY (query) TO STDOUT`, null for `COPY table`. It is the only child of this
    /// node, so the secrets in it are found and hidden as in any other query.
    ASTPtr subquery_ast;
    Strings column_names;
    enum class Formats : uint8_t
    {
        TSV,
        CSV,
        Binary
    } format = Formats::TSV;

    /// A human-readable reason set by the parser when the `COPY` command carries a data-formatting option we
    /// cannot faithfully honor (a non-default `DELIMITER`, `NULL` marker or `QUOTE`, or any option
    /// we do not interpret). The handler rejects such a command with a clean `ErrorResponse` instead
    /// of silently ignoring the option and producing output that does not match what the client asked for.
    /// Empty when there is nothing to reject.
    String unsupported_option;

    /// The NULL marker to use for the CSV format, as raw bytes. PostgreSQL's CSV convention is that an
    /// empty unquoted field means NULL (a quoted empty string stays an empty string), so this defaults to
    /// the empty string; an explicit `NULL '\N'` option selects the `\N` marker instead. The handler wires
    /// it into the CSV reader and writer through the `format_csv_null_representation` setting. Unused for
    /// the text format, whose default marker (`\N`) is also ClickHouse's TSV default; any other marker is
    /// rejected via `unsupported_option`.
    String csv_null_marker;

    /// `HEADER` of the option list: the first line of the data is the column names.
    bool header = false;

    String getID(char) const override { return "CopyQuery"; }

    ASTPtr clone() const override;

    QueryKind getQueryKind() const override { return QueryKind::Copy; }

    /// Set `subquery_ast` and add it to `children`.
    void setSubquery(ASTPtr subquery_ast_);

protected:
    void formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const override;

    /// `subquery_ast` is a separate pointer to the node that `children` holds too, so a visitor that
    /// replaces the child has to be able to repair it.
    void forEachPointerToChild(std::function<void(IAST **, boost::intrusive_ptr<IAST> *)> f) override
    {
        f(nullptr, &subquery_ast);
    }
};

String toString(ASTCopyQuery::Formats format);

/// The ClickHouse input/output format the data of this `COPY` is written in.
String getFormatName(const ASTCopyQuery & query);

}
