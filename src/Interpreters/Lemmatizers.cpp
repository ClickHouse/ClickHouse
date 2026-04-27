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

Lemmatizers::Lemmatizers(const Poco::Util::AbstractConfiguration & config)
{
    const String prefix = "lemmatizers";

    if (!config.has(prefix))
        return;

    Poco::Util::AbstractConfiguration::Keys keys;
    config.keys(prefix, keys);

    for (const auto & key : keys)
    {
        if (key.starts_with("lemmatizer"))
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
            throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Path to lemmatizer does not exist: {}", paths[name]);

        lemmatizers[name] = std::make_shared<Lemmatizer>(paths[name]);
        return lemmatizers[name];
    }

    throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER, "Lemmatizer with the name '{}' was not found in the configuration", name);
}

}

#endif
