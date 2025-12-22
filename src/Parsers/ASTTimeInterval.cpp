#include <Parsers/ASTTimeInterval.h>

#include <IO/Operators.h>

#include <ranges>

namespace DB
{

ASTPtr ASTTimeInterval::clone() const
{
    return std::make_shared<ASTTimeInterval>(*this);
}

void ASTTimeInterval::formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked) const
{
    for (bool is_first = true; auto [kind, value] : interval.toIntervals())
    {
        if (!std::exchange(is_first, false))
            ostr << ' ';
        ostr << value << ' ';
        ostr << kind.toKeyword();
    }
}

}
