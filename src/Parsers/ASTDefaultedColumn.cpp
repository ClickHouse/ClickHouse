#include <Parsers/ASTDefaultedColumn.h>

namespace DB
{

ASTPtr ASTDefaultedColumn::clone() const
{
    const auto res = std::make_shared<ASTDefaultedColumn>(*this);
    res->children.clear();
    res->name = name->clone();
    res->expression = expression->clone();
    res->children.push_back(res->name);
    return res;
}
void ASTDefaultedColumn::formatImpl(const DB::IAST::FormatSettings & settings, DB::IAST::FormatState & state, DB::IAST::FormatStateStacked frame) const
{
    if(settings.one_line) {
        return;
    }
    if(state.printed_asts_with_alias.size()) {
        return;
    }
    if(frame.need_parens) {
        return;
    }

    return;
}
}
