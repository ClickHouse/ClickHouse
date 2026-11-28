#include <IO/Operators.h>
#include <Parsers/ASTCopyQuery.h>
#include <Common/Exception.h>

namespace DB
{

void ASTCopyQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (subquery_ast)
    {
        /// The inner query is a standalone query in its own parentheses. Format it on one line, with
        /// the default settings, except that its secrets are hidden unless `show_secrets` is set.
        FormatSettings subquery_settings(/*one_line_=*/true);
        subquery_settings.show_secrets = settings.show_secrets;
        ostr << "COPY (";
        subquery_ast->format(ostr, subquery_settings);
        ostr << ") TO STDOUT";
    }
    else
        ostr << table_name;
}

ASTPtr ASTCopyQuery::clone() const
{
    auto res = make_intrusive<ASTCopyQuery>(*this);
    res->children.clear();
    res->subquery_ast = nullptr;
    if (subquery_ast)
        res->setSubquery(subquery_ast->clone());
    return res;
}

void ASTCopyQuery::setSubquery(ASTPtr subquery_ast_)
{
    if (!subquery_ast_)
        return;
    subquery_ast = std::move(subquery_ast_);
    children.push_back(subquery_ast);
}

String toString(ASTCopyQuery::Formats format)
{
    switch (format)
    {
        case ASTCopyQuery::Formats::TSV:
            return "TSV";
        case ASTCopyQuery::Formats::CSV:
            return "CSV";
        case ASTCopyQuery::Formats::Binary:
            return "Binary";
    }
}

}
