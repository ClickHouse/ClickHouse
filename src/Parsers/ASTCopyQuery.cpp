#include <IO/Operators.h>
#include <Parsers/ASTCopyQuery.h>
#include <Common/Exception.h>

namespace DB
{

void ASTCopyQuery::formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    if (subquery.empty())
        ostr << "COPY " << table_name;
    else
        ostr << "COPY (" << subquery << ')';

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
    return res;
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
