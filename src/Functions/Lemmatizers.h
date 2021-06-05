#pragma once

#include <common/types.h>
#include <Poco/Util/Application.h>

#include <mutex>
#include <unordered_map>

namespace DB
{

class Lemmatizer;

class Lemmatizers {
public:
    using LemmPtr = std::shared_ptr<Lemmatizer>;

private:
    
    std::mutex mutex;
    std::unordered_map<String, LemmPtr> lemmatizers;
    std::unordered_map<String, String> paths;     

public:
    Lemmatizers(const Poco::Util::AbstractConfiguration & config);

    LemmPtr getLemmatizer(const String & name);
};

}
