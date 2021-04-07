#pragma once

#include <Secrets/SecretsProvider.h>

#include <common/types.h>
#include <ext/unordered_map_helper.h>

#include <unordered_map>
#include <shared_mutex>
#include <string_view>
#include <memory>

namespace Poco
{
namespace Util
{
class AbstractConfiguration;
}
}

namespace DB
{

class Secret;
class ISecretBackend;

/** Manages the secrets and ISecretBackend instances.
 *
 *  Creates ISecretBackend from config, creates and caches secrets loaded by various
 *  ISecretBackend instances to a local map.
 *
 *  Demands that values stored inside Secret instances are never changed and hence it is safe to share
 *  Secrets with any number of threads by just a shared pointer.
 */
class SecretsManager final : public ISecretsProvider
{
public:
    SecretsManager();
    ~SecretsManager();

    void loadScrets(const Poco::Util::AbstractConfiguration & secrets_config);
    void registerSecretsBackend(const String & backend_name, std::unique_ptr<ISecretBackend> backend);

    SecretValue getSecretValue(std::string_view path_to_secret) const override;
    String redactOutAllSecrets(const String & text_with_secrets, ReplacamentFunc replacement_func) const override;

private:
    const ISecretBackend & getBackend(const String & name) const;
    const ISecretBackend * getBackendOrNull(const String & name) const;

    std::shared_ptr<const Secret> getSecret(std::string_view name) const;

    std::unordered_map<String, std::unique_ptr<ISecretBackend>> all_backends;
    using SecretMap = std::unordered_map<String /* secret name */, std::shared_ptr<const Secret>, ext::string_hash, ext::string_equal>;
    SecretMap loaded_secrets;
    mutable std::shared_mutex mutex;
};

}
