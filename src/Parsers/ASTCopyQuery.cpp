#include <IO/Operators.h>
#include <Parsers/ASTCopyQuery.h>
#include <Common/Exception.h>

namespace DB
{

void ASTCopyQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    if (subquery_ast)
    {
        /// The inner query is a standalone query in its own parentheses. Format it the way the parser
        /// rendered `subquery` - on one line, with the default settings, like `table_name` and
        /// `column_names` - except that its secrets are hidden unless `show_secrets` is set.
        FormatSettings subquery_settings(/*one_line_=*/true);
        subquery_settings.show_secrets = settings.show_secrets;
        ostr << "COPY (";
        subquery_ast->format(ostr, subquery_settings);
        ostr << ')';
    }
    else
        ostr << "COPY " << table_name;

    if (!column_names.empty())
    {
        ostr << " (";
        for (size_t i = 0; i < column_names.size(); ++i)
        {
            if (i)
                ostr << ", ";
            ostr << column_names[i];
        }
        ostr << ')';
    }

    switch (type)
    {
        case QueryType::COPY_FROM:
            ostr << " FROM STDIN";
            break;
        case QueryType::COPY_TO:
            ostr << " TO STDOUT";
            break;
    }

    if (format != Formats::TSV || header)
    {
        ostr << " WITH (";
        if (format != Formats::TSV)
            ostr << "FORMAT " << toString(format);
        if (header)
        {
            if (format != Formats::TSV)
                ostr << ", ";
            ostr << "HEADER";
        }
        ostr << ')';
    }
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
    set(subquery_ast, std::move(subquery_ast_));
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

String getFormatName(const ASTCopyQuery & query)
{
    switch (query.format)
    {
        case ASTCopyQuery::Formats::TSV:
            return query.header ? "TSVWithNames" : "TSV";
        case ASTCopyQuery::Formats::CSV:
            return query.header ? "CSVWithNames" : "CSV";
        case ASTCopyQuery::Formats::Binary:
            /// PostgreSQL's own binary `COPY` format is not `RowBinary`, but that is what this
            /// protocol has always read `WITH FORMAT binary` as, so keep both directions the same.
            return "RowBinary";
    }
}

}
