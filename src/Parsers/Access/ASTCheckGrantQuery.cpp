#include <Parsers/Access/ASTCheckGrantQuery.h>

#include <IO/Operators.h>


namespace DB
{

String ASTCheckGrantQuery::getID(char) const
{
    return "CheckGrantQuery";
}


ASTPtr ASTCheckGrantQuery::clone() const
{
    auto res = std::make_shared<ASTCheckGrantQuery>(*this);

    return res;
}


void ASTCheckGrantQuery::formatImpl(WriteBuffer & ostr, const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    ostr << (settings.hilite ? IAST::hilite_keyword : "") << "CHECK GRANT"
                  << (settings.hilite ? IAST::hilite_none : "");

    ostr << " ";
    access_rights_elements.formatElementsWithoutOptions(ostr, settings.hilite);
}


void ASTCheckGrantQuery::replaceEmptyDatabase(const String & current_database)
{
    access_rights_elements.replaceEmptyDatabase(current_database);
}

}
