#include "config_core.h"

#if USE_NLP

#include <Common/Exception.h>
#include <Interpreters/SynonymsExtensions.h>

#include <fstream>
#include <list>

#include <boost/algorithm/string.hpp>
#include <wnb/core/wordnet.hh>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
    extern const int INVALID_CONFIG_PARAMETER;
}

class PlainSynonymsExtension : public ISynonymsExtension
{
private:
    using Container = std::list<Synset>;
    using LookupTable = std::unordered_map<std::string_view, Synset *>;

    Container synsets;
    LookupTable table;

public:
    explicit PlainSynonymsExtension(const String & path)
    {
        std::ifstream file(path);
        if (!file.is_open())
            throw Exception("Cannot find synonyms extension at: " + path,
                ErrorCodes::INVALID_CONFIG_PARAMETER);

        String line;
        while (std::getline(file, line))
        {
            Synset synset;
            boost::split(synset, line, boost::is_any_of("\t "));
            if (!synset.empty())
            {
                synsets.emplace_back(std::move(synset));

                for (const auto &word : synsets.back())
                    table[word] = &synsets.back();
            }
        }
    }

    const Synset * getSynonyms(std::string_view token) const override
    {
        auto it = table.find(token);

        if (it != table.end())
            return (*it).second;

        return nullptr;
    }
};

class WordnetSynonymsExtension : public ISynonymsExtension
{
private:
    wnb::wordnet wn;

public:
    explicit WordnetSynonymsExtension(const String & path) : wn(path) {}

    const Synset * getSynonyms(std::string_view token) const override
    {
        return wn.get_synset(std::string(token));
    }
};

/// Duplicate of code from StringUtils.h. Copied here for less dependencies.
static bool startsWith(const std::string & s, const char * prefix)
{
    return s.size() >= strlen(prefix) && 0 == memcmp(s.data(), prefix, strlen(prefix));
}

SynonymsExtensions::SynonymsExtensions(const Poco::Util::AbstractConfiguration & config)
{
    String prefix = "synonyms_extensions";
    Poco::Util::AbstractConfiguration::Keys keys;

    if (!config.has(prefix))
        throw Exception(ErrorCodes::INVALID_CONFIG_PARAMETER,
                        "No synonims extensions specified in server config on prefix '{}'", prefix);

    config.keys(prefix, keys);

    for (const auto & key : keys)
    {
        if (startsWith(key, "extension"))
        {
            const auto & ext_name = config.getString(prefix + "." + key + ".name", "");
            const auto & ext_path = config.getString(prefix + "." + key + ".path", "");
            const auto & ext_type = config.getString(prefix + "." + key + ".type", "");

            if (ext_name.empty())
                throw Exception("Extension name in config is not specified here: " + prefix + "." + key + ".name",
                    ErrorCodes::INVALID_CONFIG_PARAMETER);
            if (ext_path.empty())
                throw Exception("Extension path in config is not specified here: " + prefix + "." + key + ".path",
                    ErrorCodes::INVALID_CONFIG_PARAMETER);
            if (ext_type.empty())
                throw Exception("Extension type in config is not specified here: " + prefix + "." + key + ".type",
                    ErrorCodes::INVALID_CONFIG_PARAMETER);
            if (ext_type != "plain" && ext_type != "wordnet")
                throw Exception("Unknown extension type in config: " + prefix + "." + key + ".type, must be 'plain' or 'wordnet'",
                    ErrorCodes::INVALID_CONFIG_PARAMETER);

            info[ext_name].path = ext_path;
            info[ext_name].type = ext_type;
        }
        else
            throw Exception("Unknown element in config: " + prefix + "." + key + ", must be 'extension'",
                ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG);
    }
}

SynonymsExtensions::ExtPtr SynonymsExtensions::getExtension(const String & name)
{
    std::lock_guard guard(mutex);

    if (extensions.find(name) != extensions.end())
        return extensions[name];

    if (info.find(name) != info.end())
    {
        const Info & ext_info = info[name];

        if (ext_info.type == "plain")
            extensions[name] = std::make_shared<PlainSynonymsExtension>(ext_info.path);
        else if (ext_info.type == "wordnet")
            extensions[name] = std::make_shared<WordnetSynonymsExtension>(ext_info.path);
        else
            throw Exception("Unknown extension type: " + ext_info.type, ErrorCodes::LOGICAL_ERROR);

        return extensions[name];
    }

    throw Exception("Extension named: '" + name + "' is not found",
        ErrorCodes::INVALID_CONFIG_PARAMETER);
}

}

#endif
