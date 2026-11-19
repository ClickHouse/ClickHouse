#include <Parsers/formatSettingName.h>
#include <Common/StringUtils.h>
#include <Common/quoteString.h>
#include <base/find_symbols.h>
#include <IO/Operators.h>


namespace DB
{

void formatSettingName(const String & setting_name, WriteBuffer & out)
{
    if (isValidIdentifier(setting_name))
    {
        out << setting_name;
        return;
    }

    std::vector<std::string_view> parts;
    splitInto<'.'>(parts, setting_name);
    /// `splitInto` emits no part for a trailing dot, so joining the parts of `a.` gives back `a`.
    bool all_parts_are_identifiers = std::all_of(parts.begin(), parts.end(), isValidIdentifier);
    if (all_parts_are_identifiers && !parts.empty() && !setting_name.ends_with('.'))
    {
        bool need_dot = false;
        for (const auto & part : parts)
        {
            if (std::exchange(need_dot, true))
                out << ".";
            out << part;
        }
        return;
    }

    out << backQuote(setting_name);
}

}
