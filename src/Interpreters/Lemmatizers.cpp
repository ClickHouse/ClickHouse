#include "config.h"

#if USE_NLP

#include <Common/Exception.h>
#include <Interpreters/Lemmatizers.h>
#include <RdrLemmatizer.h>

#include <vector>
#include <filesystem>

namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
}


class Lemmatizer : public ILemmatizer
{
private:
    RdrLemmatizer lemmatizer;

public:
    explicit Lemmatizer(const String & path) : lemmatizer(path.data()) {}

    TokenPtr lemmatize(const char * token) override
    {
        return TokenPtr(lemmatizer.Lemmatize(token));
    }
};

/// Duplicate of code from StringUtils.h. Copied here for less dependencies.
static bool startsWith(const std::string & s, const char * prefix)
{
    return s.size() >= strlen(prefix) && 0 == memcmp(s.data(), prefix, strlen(prefix));
}

Lemmatizers::Lemmatizers(const Poco::Util::AbstractConfiguration & config)
{
    String prefix = "lemmatizers";
    Poco::Util::AbstractConfiguration::Keys keys;

    if (!config.has(prefix))
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "No lemmatizers specified in server config on prefix '{}'", prefix);

    config.keys(prefix, keys);

    for (const auto & key : keys)
    {
        if (startsWith(key, "lemmatizer"))
        {
            const auto & lemm_name = config.getString(prefix + "." + key + ".lang", "");
            const auto & lemm_path = config.getString(prefix + "." + key + ".path", "");

            if (lemm_name.empty())
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Lemmatizer language in config is not specified here: "
                    "{}.{}.lang", prefix, key);
            if (lemm_path.empty())
                throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Path to lemmatizer in config is not specified here: {}.{}.path",
                    prefix, key);

            paths[lemm_name] = lemm_path;
        }
        else
            throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG, "Unknown element in config: {}.{}, must be 'lemmatizer'",
                prefix, key);
    }
}

Lemmatizers::LemmPtr Lemmatizers::getLemmatizer(const String & name)
{
    std::lock_guard guard(mutex);

    if (lemmatizers.find(name) != lemmatizers.end())
        return lemmatizers[name];

    if (paths.find(name) != paths.end())
    {
        if (!std::filesystem::exists(paths[name]))
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Incorrect path to lemmatizer: {}", paths[name]);

        lemmatizers[name] = std::make_shared<Lemmatizer>(paths[name]);
        return lemmatizers[name];
    }

    throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Lemmatizer named: '{}' is not found", name);
}

}

#endif
