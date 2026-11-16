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

}
