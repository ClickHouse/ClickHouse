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


void ASTCheckGrantQuery::formatImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? IAST::hilite_keyword : "") << "CHECK GRANT"
                  << (settings.hilite ? IAST::hilite_none : "");

    settings.ostr << " ";
    access_rights_elements.formatElementsWithoutOptions(settings.ostr, settings.hilite);
}


void ASTCheckGrantQuery::replaceEmptyDatabase(const String & current_database)
{
    access_rights_elements.replaceEmptyDatabase(current_database);
}

}
