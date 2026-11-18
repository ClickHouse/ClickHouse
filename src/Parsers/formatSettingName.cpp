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
    bool all_parts_are_identifiers = std::all_of(parts.begin(), parts.end(), isValidIdentifier);
    /// `splitInto` does not return the empty part after a trailing dot.
    /// Without this check, the name `a.` is written as `a`, which is a different name.
    bool has_trailing_dot = setting_name.ends_with('.');
    if (all_parts_are_identifiers && !parts.empty() && !has_trailing_dot)
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
