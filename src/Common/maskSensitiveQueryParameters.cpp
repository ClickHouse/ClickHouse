#include <Common/maskSensitiveQueryParameters.h>

#include <Common/HiddenSecret.h>
#include <Common/StringUtils.h>
#include <Common/Hex.h>

#include <algorithm>
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

/// The name as a peer interprets it: percent-decoded and in lower case.
std::string normalizeParameterName(std::string_view raw_name)
{
    std::string name = decodeQueryComponent(raw_name);
    for (char & c : name)
        c = asciiToLower(c);
    return name;
}

bool isSensitiveParameterName(std::string_view raw_name)
{
    const std::string lower = normalizeParameterName(raw_name);

    for (auto needle : sensitive_exact_names)
    {
        if (lower == needle)
            return true;
    }

    for (auto needle : sensitive_name_substrings)
    {
        if (lower.contains(needle))
            return true;
    }
    return false;
}

}

std::string decodeQueryComponent(std::string_view name)
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

std::string maskSensitiveQueryParametersInURI(const std::string & uri)
{
    const auto question = uri.find('?');
    if (question == std::string::npos)
        return uri;

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
                result.append(HIDDEN_SECRET);
            else
                result.append(pair.data() + eq + 1, pair.size() - eq - 1);
        }

        if (amp == std::string_view::npos)
            break;
        pos = amp + 1;
    }

    return result;
}

/// The query parameters of an S3 or GCS presigned URL. The prefixes are header names, so any case: SigV2 sends
/// `x-amz-security-token`. `presignedURLCredentials` reads the secret ones among them, so extend both together.
constexpr std::array<std::string_view, 4> presigned_url_names = {"AWSAccessKeyId", "Signature", "Expires", "GoogleAccessId"};
constexpr std::array<std::string_view, 2> presigned_url_prefixes = {"X-Amz-", "X-Goog-"};

bool isPresignedURLParameterName(std::string_view name)
{
    return std::ranges::find(presigned_url_names, name) != presigned_url_names.end()
        || std::ranges::any_of(
               presigned_url_prefixes,
               [&](std::string_view prefix) { return equalsCaseInsensitive(name.substr(0, prefix.length()), prefix); });
}

std::vector<std::string> presignedURLCredentials(std::string_view uri)
{
    /// `sig` is the signature of an Azure SAS.
    static constexpr std::array<std::string_view, 8> credential_names{
        "x-amz-signature", "x-amz-security-token", "x-amz-s3session-token", "signature", "awsaccesskeyid",
        "x-goog-signature", "googleaccessid", "sig"};

    std::vector<std::string> credentials;
    const size_t question = uri.find('?');
    if (question == std::string_view::npos)
        return credentials;

    std::string_view query = uri.substr(question + 1);
    query = query.substr(0, query.find('#'));

    auto add = [&](std::string credential)
    {
        if (!credential.empty())
            credentials.push_back(std::move(credential));
    };

    size_t pos = 0;
    while (pos <= query.size())
    {
        const size_t amp = std::min(query.find('&', pos), query.size());
        const std::string_view pair = query.substr(pos, amp - pos);
        pos = amp + 1;

        const size_t eq = pair.find('=');
        if (eq == std::string_view::npos)
            continue;
        const std::string name = normalizeParameterName(pair.substr(0, eq));
        const std::string_view value = pair.substr(eq + 1);

        if (std::ranges::find(credential_names, name) != credential_names.end())
        {
            /// An echo can reproduce the value as sent or percent-decoded.
            std::string decoded = decodeQueryComponent(value);
            if (decoded != value)
                add(std::string(value));
            add(std::move(decoded));
        }
        else if (name == "x-amz-credential" || name == "x-goog-credential")
        {
            /// `<access key id>/<scope>`: the scope is not a secret, and the key id is echoed on its own.
            std::string decoded = decodeQueryComponent(value);
            std::string key_id = decoded.substr(0, decoded.find('/'));
            /// As sent, the key id ends at the first '/' or at its escape "%2F".
            size_t raw_end = value.find('/');
            for (size_t i = 0; i + 2 < value.size() && i < raw_end; ++i)
            {
                if (value[i] == '%' && value[i + 1] == '2' && (value[i + 2] == 'F' || value[i + 2] == 'f'))
                    raw_end = i;
            }
            if (const std::string_view raw_key_id = value.substr(0, raw_end); raw_key_id != key_id)
                add(std::string(raw_key_id));
            add(std::move(key_id));
        }
    }
    return credentials;
}

}
