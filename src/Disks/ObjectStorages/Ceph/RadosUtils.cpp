#include "RadosUtils.h"
#include <Common/Exception.h>
#include <Common/logger_useful.h>

#if USE_CEPH

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void RadosOptions::loadFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    resetToDefaultOptions();
    Strings keys;
    config.keys(config_prefix, keys);
    for (const auto & key : keys)
        (*this)[key] = config.getRawString(config_prefix + "." + key);
}

void RadosOptions::resetToDefaultOptions()
{
    clear();
    /// TODO: Ceph has many options, we can have a default set of options here.
}

void RadosOptions::validate()
{
    for (const auto & [key, value] : *this)
        LOG_DEBUG(getLogger("RadosOptions"), "RadosOptions: key: {}, value: {}", key, value);
    /// Auth: either we have: (1) user and key or keyfile, or (2) keyring
    bool has_user_based_auth = !user.empty() && (count("key") || count("keyfile"));
    bool has_keyring_based_auth = count("keyring");
    if (!has_user_based_auth && !has_keyring_based_auth)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "RadosOptions: either user + key/keyfile or keyring must be specified");
}
}

#endif
