#include <IO/Operators.h>
#include <Parsers/ASTCopyQuery.h>
#include <Common/Exception.h>

namespace DB
{

void ASTCopyQuery::formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << table_name;
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
