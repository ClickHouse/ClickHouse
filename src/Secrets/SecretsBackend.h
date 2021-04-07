#pragma once

#include <common/types.h>

#include <ext/unordered_map_helper.h>

#include <memory>
#include <unordered_map>
#include <string>
#include <string_view>
#include <deque>

namespace Poco
{
namespace Util
{
class AbstractConfiguration;
}
}

namespace DB
{
class SecretsManager;

class Secret
{
public:
    Secret(const Poco::Util::AbstractConfiguration & secret_config);

    const String & getValue() const;
    const String & getNestedValue(std::string_view path) const;

friend class SecretsManager;

private:
    std::unordered_map<String, String, ext::string_hash, ext::string_equal> values;
};

class ISecretBackend
{
public:
    virtual ~ISecretBackend() = 0;

    void resetWithConfig(const Poco::Util::AbstractConfiguration & /*backend*/) const {}
    // The secret doesn't change ever, ownership is passed to the caller.
    virtual std::shared_ptr<const Secret> getSecret(const Poco::Util::AbstractConfiguration & secret_config) const = 0;
};

class ConfigSecretBackend : public ISecretBackend
{
public:
    ~ConfigSecretBackend() override;

    std::shared_ptr<const Secret> getSecret(const Poco::Util::AbstractConfiguration & secret_config) const override;
};


}
