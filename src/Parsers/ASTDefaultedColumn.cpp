#include <Parsers/ASTDefaultedColumn.h>

namespace DB
{

ASTPtr ASTDefaultedColumn::clone() const
{
    const auto res = std::make_shared<ASTDefaultedColumn>(*this);
    res->name->clone();
    res->expression->clone();
    return res;
}
void ASTDefaultedColumn::formatImpl(const DB::IAST::FormatSettings & settings, DB::IAST::FormatState & state, DB::IAST::FormatStateStacked frame) const
{
    if(settings.one_line) {
        return;
    }
    if(state.printed_asts_with_alias[1]) {
        return;
    }
    if(frame.need_parens) {
        return;
    }

    return;
}
}
