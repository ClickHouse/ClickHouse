#include <Parsers/Access/ASTCheckGrantQuery.h>

#include <Interpreters/CurrentDatabaseInfo.h>
#include <IO/Operators.h>

namespace DB
{

String ASTCheckGrantQuery::getID(char) const
{
    return "CheckGrantQuery";
}


ASTPtr ASTCheckGrantQuery::clone() const
{
    auto res = make_intrusive<ASTCheckGrantQuery>(*this);

    return res;
}


void ASTCheckGrantQuery::formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << "CHECK GRANT ";
    access_rights_elements.formatElementsWithoutOptions(ostr);
}


}
