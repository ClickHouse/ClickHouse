#pragma once

#include <base/types.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/Logger.h>
#include <Interpreters/Context.h>

namespace DB
{

class HashiCorpVault
{
public:
    HashiCorpVault()
    {
        log = getLogger("HashiCorpVault");
        reset();
    }

    static HashiCorpVault & instance();

    /// Load data and throw exception if something went wrong.
    void load(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, ContextPtr context_);

    String readSecret(const String & secret, const String & key);

    bool isLoaded() { return loaded; }

private:
    void reset()
    {
        url = "";
        token = "";
        loaded = false;
    }
    LoggerPtr log;
    bool loaded = false;
    String url;
    String token;
    ContextPtr context;
};

}
