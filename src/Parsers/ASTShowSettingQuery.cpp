#include <Parsers/ASTShowSettingQuery.h>

#include <iomanip>
#include <IO/Operators.h>
#include <Common/quoteString.h>

namespace DB
{

ASTPtr ASTShowSettingQuery::clone() const
{
    auto res = std::make_shared<ASTShowSettingQuery>(*this);
    res->children.clear();
    cloneOutputOptions(*res);
    res->setting_name = setting_name;
    return res;
}

void ASTShowSettingQuery::formatQueryImpl(const FormatSettings & settings, FormatState &, FormatStateStacked) const
{
    settings.ostr << (settings.hilite ? hilite_keyword : "") << "SHOW SETTING " << (settings.hilite ? hilite_none : "")
                  << backQuoteIfNeed(setting_name);
}

}
