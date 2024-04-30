#include <Parsers/ASTTimeInterval.h>

#include <IO/Operators.h>

#include <ranges>

namespace DB
{

ASTPtr ASTTimeInterval::clone() const
{
    return std::make_shared<ASTTimeInterval>(*this);
}

void ASTTimeInterval::formatImpl(const FormatSettings & f_settings, FormatState &, FormatStateStacked frame) const
{
    frame.need_parens = false;

    for (bool is_first = true; auto [kind, value] : interval.toIntervals())
    {
        if (!std::exchange(is_first, false))
            f_settings.ostr << ' ';
        f_settings.ostr << value << ' ';
        f_settings.ostr << (f_settings.hilite ? hilite_keyword : "") << kind.toKeyword() << (f_settings.hilite ? hilite_none : "");
    }
}

}
