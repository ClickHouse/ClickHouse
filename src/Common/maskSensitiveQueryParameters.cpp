#include <Common/maskSensitiveQueryParameters.h>

#include <Common/StringUtils.h>
#include <base/hex.h>

#include <array>
#include <string>
#include <string_view>


namespace DB
{

namespace
{

/// Substrings (lower case) that mark a query-parameter name as carrying a secret value.
constexpr std::array sensitive_name_substrings{
    std::string_view{"secret"},
    std::string_view{"password"},
    std::string_view{"passwd"},
    std::string_view{"token"},
    std::string_view{"credential"},
    std::string_view{"signature"},
    std::string_view{"key"},
};

/// Short names that are sensitive on their own but too prone to false positives as substrings
/// (e.g. "sig" would match "design"). Matched exactly. "sig" is the Azure SAS signature parameter.
constexpr std::array sensitive_exact_names{
    std::string_view{"sig"},
};

char asciiToLower(char c)
{
    return (c >= 'A' && c <= 'Z') ? static_cast<char>(c | 0x20) : c;
}

/// Decode a query-parameter NAME the same way the server interprets it before authentication:
/// HTMLForm::readQuery turns '+' into a space and percent-decodes ('%XX') the name. We must
/// classify on this decoded form, otherwise an encoded name like "pass%77ord" (which the server
/// accepts as "password") would slip past the match and leak its value. This is tolerant by
/// design: a malformed escape is copied through verbatim (never throws) since this only feeds
/// the sensitivity decision, never the logged output.
std::string decodeParameterName(std::string_view name)
{
    std::string decoded;
    decoded.reserve(name.size());
    for (size_t i = 0; i < name.size(); ++i)
    {
        char c = name[i];
        if (c == '+')
        {
            decoded.push_back(' ');
        }
        else if (c == '%' && i + 2 < name.size() && isHexDigit(name[i + 1]) && isHexDigit(name[i + 2]))
        {
            decoded.push_back(static_cast<char>(unhex2(name.data() + i + 1)));
            i += 2;
        }
        else
        {
            decoded.push_back(c);
        }
    }
    return decoded;
}

bool isSensitiveParameterName(std::string_view raw_name)
{
    const std::string decoded = decodeParameterName(raw_name);

    std::string lower;
    lower.reserve(decoded.size());
    for (char c : decoded)
        lower.push_back(asciiToLower(c));

    for (auto needle : sensitive_exact_names)
    {
        if (lower == needle)
            return true;
    }

    for (auto needle : sensitive_name_substrings)
    {
        if (lower.find(needle) != std::string::npos)
            return true;
    }
    return false;
}

}

std::string maskSensitiveQueryParametersInURI(const std::string & uri)
{
    const auto question = uri.find('?');
    if (question == std::string::npos)
        return uri;

    constexpr std::string_view hidden = "[HIDDEN]";

    std::string result;
    result.reserve(uri.size());
    /// Copy the path together with the '?'.
    result.append(uri, 0, question + 1);

    const std::string_view query = std::string_view{uri}.substr(question + 1);

    size_t pos = 0;
    bool first = true;
    while (true)
    {
        const size_t amp = query.find('&', pos);
        const size_t end = (amp == std::string_view::npos) ? query.size() : amp;
        const std::string_view pair = query.substr(pos, end - pos);

        if (!first)
            result += '&';
        first = false;

        const size_t eq = pair.find('=');
        if (eq == std::string_view::npos)
        {
            /// A flag without a value: nothing to redact.
            result.append(pair.data(), pair.size());
        }
        else
        {
            const std::string_view name = pair.substr(0, eq);
            result.append(name.data(), name.size());
            result += '=';
            if (isSensitiveParameterName(name))
                result.append(hidden.data(), hidden.size());
            else
                result.append(pair.data() + eq + 1, pair.size() - eq - 1);
        }

        if (amp == std::string_view::npos)
            break;
        pos = amp + 1;
    }

    return result;
}

}
