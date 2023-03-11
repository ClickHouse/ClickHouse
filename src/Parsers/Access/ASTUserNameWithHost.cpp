#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Common/quoteString.h>
#include <IO/Operators.h>


namespace DB
{

void ASTUserNameWithHost::formatImpl(const FormattingBuffer & out) const
{
    out.ostr << backQuoteIfNeed(base_name);

    if (!host_pattern.empty())
        out.ostr << "@" << backQuoteIfNeed(host_pattern);
}

String ASTUserNameWithHost::toString() const
{
    String res = base_name;
    if (!host_pattern.empty())
        res += '@' + host_pattern;
    return res;
}

void ASTUserNameWithHost::concatParts()
{
    base_name = toString();
    host_pattern.clear();
}


void ASTUserNamesWithHost::formatImpl(const FormattingBuffer & out) const
{
    assert(!names.empty());
    bool need_comma = false;
    for (const auto & name : names)
    {
        if (std::exchange(need_comma, true))
            out.ostr << ", ";
        name->format(out);
    }
}

Strings ASTUserNamesWithHost::toStrings() const
{
    Strings res;
    res.reserve(names.size());
    for (const auto & name : names)
        res.emplace_back(name->toString());
    return res;
}

void ASTUserNamesWithHost::concatParts()
{
    for (auto & name : names)
        name->concatParts();
}


bool ASTUserNamesWithHost::getHostPatternIfCommon(String & out_common_host_pattern) const
{
    out_common_host_pattern.clear();

    if (names.empty())
        return true;

    for (size_t i = 1; i != names.size(); ++i)
        if (names[i]->host_pattern != names[0]->host_pattern)
            return false;

    out_common_host_pattern = names[0]->host_pattern;
    return true;
}

}
