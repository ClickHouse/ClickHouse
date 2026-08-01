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
            /// PostgreSQL binary `COPY` is rejected before we ever serialize the format name
            /// (see `PostgreSQLHandler::processCopyQuery`), so this is unreachable.
            throw Exception(ErrorCodes::LOGICAL_ERROR, "PostgreSQL binary COPY has no backing ClickHouse format");
    }
}

}
