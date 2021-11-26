#pragma once

#include "config_core.h"

#if USE_NLP

#include <base/types.h>
#include <Poco/Util/Application.h>

#include <memory>
#include <mutex>
#include <string_view>
#include <vector>
#include <unordered_map>

namespace DB
{

class ISynonymsExtension
{
public:
    using Synset = std::vector<String>;

    virtual const Synset * getSynonyms(std::string_view token) const = 0;

    virtual ~ISynonymsExtension() = default;
};

class SynonymsExtensions
{
public:
    using ExtPtr = std::shared_ptr<ISynonymsExtension>;

    explicit SynonymsExtensions(const Poco::Util::AbstractConfiguration & config);

    ExtPtr getExtension(const String & name);

private:
    struct Info
    {
        String path;
        String type;
    };

    using ExtContainer = std::unordered_map<String, ExtPtr>;
    using InfoContainer = std::unordered_map<String, Info>;

    std::mutex mutex;
    ExtContainer extensions;
    InfoContainer info;
};

}

#endif
