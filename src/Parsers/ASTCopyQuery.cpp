#include <IO/Operators.h>
#include <Parsers/ASTCopyQuery.h>
#include <Common/Exception.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

void ASTCopyQuery::formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
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
            /// PostgreSQL binary `COPY` is rejected before we ever serialize the format name
            /// (see `PostgreSQLHandler::processCopyQuery`), so this is unreachable.
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PostgreSQL binary COPY has no backing ClickHouse format");
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
            return toString(query.format);
    }
}

}
