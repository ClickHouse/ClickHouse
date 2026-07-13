#include <Core/IdentifierName.h>

namespace DB
{

String foldIdentifierCaseASCII(std::string_view name)
{
    String result(name);
    for (auto & c : result)
        if (c >= 'A' && c <= 'Z')
            c += 'a' - 'A';
    return result;
}

String IdentifierPart::matchingKey() const
{
    return isCaseFoldable() ? foldIdentifierCaseASCII(spelling) : spelling;
}

IdentifierName::IdentifierName(const std::vector<String> & spellings)
{
    parts.reserve(spellings.size());
    for (const auto & spelling : spellings)
        parts.push_back(IdentifierPart{spelling, IdentifierPartQuote::Unquoted});
}

std::vector<String> IdentifierName::spellings() const
{
    std::vector<String> result;
    result.reserve(parts.size());
    for (const auto & part : parts)
        result.push_back(part.spelling);
    return result;
}

bool IdentifierName::anyPartDoubleQuoted() const
{
    for (const auto & part : parts)
        if (part.quote == IdentifierPartQuote::DoubleQuoted)
            return true;
    return false;
}

String IdentifierName::toString() const
{
    String result;
    for (const auto & part : parts)
    {
        if (!result.empty())
            result += '.';
        result += part.spelling;
    }
    return result;
}

}
