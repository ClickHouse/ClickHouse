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
    /// The inner query of `COPY (query) TO ...`, null for `COPY table`. It is the only child of this
    /// node, so the secrets in it (such as the password of `remote`) are found and hidden when the
    /// query is formatted and logged, as in any other query.
    ASTPtr subquery_ast;
    Strings column_names;
    enum class Formats : uint8_t
    {
        TSV,
        CSV,
        Binary
    } format = Formats::TSV;

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

}
