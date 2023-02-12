#include <Parsers/ASTTimeInterval.h>

#include <IO/Operators.h>

#include <ranges>

namespace DB
{

ASTPtr ASTTimePeriod::clone() const
{
    return std::make_shared<ASTTimePeriod>(*this);
}

void ASTTimePeriod::formatImpl(const FormatSettings & f_settings, FormatState &, FormatStateStacked frame) const
{
    frame.need_parens = false;
    f_settings.ostr << (f_settings.hilite ? hilite_none : "") << value << ' ';
    f_settings.ostr << (f_settings.hilite ? hilite_keyword : "") << kind.toKeyword();
}

ASTPtr ASTTimeInterval::clone() const
{
    return std::make_shared<ASTTimeInterval>(*this);
}

void ASTTimeInterval::formatImpl(const FormatSettings & f_settings, FormatState &, FormatStateStacked frame) const
{
    frame.need_parens = false;

    for (bool is_first = true; auto [kind, value] : kinds | std::views::reverse)
    {
        if (!std::exchange(is_first, false))
            f_settings.ostr << ' ';
        f_settings.ostr << (f_settings.hilite ? hilite_none : "") << value << ' ';
        f_settings.ostr << (f_settings.hilite ? hilite_keyword : "") << kind.toKeyword();
    }
}

}
