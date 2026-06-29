#include <Parsers/ASTDropTypeQuery.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>

namespace DB
{

String ASTDropTypeQuery::getID(char delim) const
{
    return "DropTypeQuery" + (delim + type_name);
}

ASTPtr ASTDropTypeQuery::clone() const
{
    auto res = std::make_shared<ASTDropTypeQuery>(*this);
    return res;
}

void ASTDropTypeQuery::formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << "DROP TYPE ";

    if (if_exists)
        ostr << "IF EXISTS ";

    ostr << backQuoteIfNeed(type_name);
}

}
