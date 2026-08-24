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

void ASTShowSettingQuery::formatQueryImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    ostr << "SHOW SETTING " << backQuoteIfNeed(setting_name);
}

}
