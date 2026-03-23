#include <Parsers/ASTTimeInterval.h>
#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTJSONReadHelpers.h>

#include <IO/Operators.h>

#include <ranges>

namespace DB
{

ASTPtr ASTTimeInterval::clone() const
{
    return make_intrusive<ASTTimeInterval>(*this);
}

void ASTTimeInterval::formatImpl(WriteBuffer & ostr, const FormatSettings &, FormatState &, FormatStateStacked frame) const
{
    frame.need_parens = false;

    for (bool is_first = true; auto [kind, value] : interval.toIntervals())
    {
        if (!std::exchange(is_first, false))
            ostr << ' ';
        ostr << value << ' ';
        ostr << kind.toKeyword();
    }
}

void ASTTimeInterval::writeJSON(WriteBuffer & out) const
{
    JSONObjectWriter w(out, "TimeInterval");
    w.writeUInt("seconds", interval.seconds);
    w.writeUInt("months", interval.months);
}

void ASTTimeInterval::readJSON(const Poco::JSON::Object & json)
{
    JSONObjectReader r(json);
    interval.seconds = r.getUInt("seconds");
    interval.months = r.getUInt("months");
}

}
