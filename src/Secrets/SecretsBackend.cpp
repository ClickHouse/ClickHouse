#include <Secrets/SecretsBackend.h>

#include <Common/Exception.h>
#include <common/types.h>
#include <Common/hex.h>
#include <Common/StringUtils/StringUtils.h>

#include <Poco/Util/AbstractConfiguration.h>
#include <deque>

namespace DB
{
using namespace std::literals::string_view_literals;

namespace ErrorCodes
{
    extern const int UNKNOWN_IDENTIFIER;
    extern const int INCORRECT_DATA;
}

namespace
{
// empty key is used for top-level config value.
constexpr auto ROOT_KEY = std::string_view();
constexpr auto HEX_ENCODING = "hex"sv;

// since we don't want incorretly encoded secrets to be just silently altered.
inline void validateHexChar(const char c, size_t pos)
{
    if (!isHexDigit(c))
        throw Exception(ErrorCodes::INCORRECT_DATA, "Invalid hex character '{}'({}) at pos {}", c, static_cast<size_t>(c), pos);
}

// Decodes hex-encoded string inplace, leading 0 can be omitted.
void decodeHexInplace(String & val)
{
    size_t j = 0;
    size_t i = 0;
    if (val.size() % 2)
    {
        validateHexChar(val[i], i);
        val[j++] = unhex(val[i++]);
    }

    for (; i < val.size(); i += 2)
    {
        validateHexChar(val[i], i);
        validateHexChar(val[i + 1], i + 1);
        val[j++] = unhex2(&val[i]);
    }

    val.resize(j);
}

}

ISecretBackend::~ISecretBackend() = default;

Secret::Secret(const Poco::Util::AbstractConfiguration & secret_config)
{
    // reads all the leaf keys and stores those into a map with full config path, like:
    // local.mysql.password, local.mysql.username.

    std::deque<String> keys_to_traverse{1, String(ROOT_KEY)};

    while (!keys_to_traverse.empty())
    {
        const auto key_to_travere = std::move(keys_to_traverse.front());
        keys_to_traverse.pop_front();

        Poco::Util::AbstractConfiguration::Keys nested_keys;
        secret_config.keys(key_to_travere, nested_keys);
        if (nested_keys.empty())
        {
            auto val = secret_config.getString(key_to_travere);
            if (secret_config.getString(key_to_travere + "[@encoding]", std::string{}) == HEX_ENCODING)
            {
                decodeHexInplace(val);
            }
            values.emplace(std::move(key_to_travere), std::move(val));
        }
        else
        {
            const auto path_prefix = key_to_travere.empty() ? String{} : key_to_travere + ".";
            for (const auto & key : nested_keys)
            {
                const String path_to_key = path_prefix + key;
                keys_to_traverse.push_back(std::move(path_to_key));
            }
        }
    }
}

const String & Secret::getValue() const
{
    return getNestedValue(ROOT_KEY);
}

const String & Secret::getNestedValue(std::string_view path) const
{
    const auto p = values.find(ext::heterogeneousKey(path));
    if (p == values.end())
    {
        if (path.empty())
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "no plaintext value, only nested");
        else
            throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "couldn't find nested secret by path: {}", path);
    }

    return p->second;
}

ConfigSecretBackend::~ConfigSecretBackend() = default;

std::shared_ptr<const Secret> ConfigSecretBackend::getSecret(const Poco::Util::AbstractConfiguration & secret_config) const
{
    return std::make_shared<Secret>(secret_config);
};

}
