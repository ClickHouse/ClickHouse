#include <Secrets/SecretsManager.h>

#include <Secrets/SecretsBackend.h>

#include <Common/Exception.h>

#include <Poco/AutoPtr.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <algorithm>

namespace
{
const String DEFAULT_BACKEND{};

struct PocoRefCountedObjectDeleter
{
    void operator()(const Poco::RefCountedObject * object) const
    {
        object->release();
    }
};

template <typename T>
using UniquePtrPocoRefCounted = std::unique_ptr<T, PocoRefCountedObjectDeleter>;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int UNKNOWN_IDENTIFIER;
}

SecretValue::SecretValue(std::shared_ptr<const Secret> secret_, std::string_view path)
    : secret(secret_),
      value(path.empty() ? secret->getValue() : secret->getNestedValue(path))
{}

SecretValue::~SecretValue() = default;
SecretsManager::~SecretsManager() = default;

SecretsManager::SecretsManager()
{
    registerSecretsBackend(DEFAULT_BACKEND, std::make_unique<ConfigSecretBackend>());
}

void SecretsManager::loadScrets(const Poco::Util::AbstractConfiguration & secrets_config)
{
    const String secrets_section{"secrets"};
    const String backends_section{"secrets_backends"};

    // Backend-side configs
    if (secrets_config.has(backends_section))
    {
        Poco::Util::AbstractConfiguration::Keys backends;
        secrets_config.keys(backends_section, backends);
        for (const auto & backend_name : backends)
        {
            UniquePtrPocoRefCounted<const Poco::Util::AbstractConfiguration> backend_config(
                    secrets_config.createView(backends_section + "." + backend_name));
            getBackend(backend_name).resetWithConfig(*backend_config);
        }
    }
    std::decay_t<decltype(loaded_secrets)> new_secrets;

    Poco::Util::AbstractConfiguration::Keys all_secrets;
    secrets_config.keys(secrets_section, all_secrets);
    for (const auto & secret_name : all_secrets)
    {
        const auto secret_path = secrets_section + "." + secret_name;
        const auto backend_key = secret_path + ".secret_backend";
        const auto backend_name = secrets_config.getString(backend_key, DEFAULT_BACKEND);
        const auto & backend = getBackend(backend_name);
        const auto nested_secret_path = secret_path + "." + backend_name;

        // By convention default backend uses all nodes inside secret config,
        // while others use nodes from special sub-node with the name as backend.
        const auto & secret_config_path = secrets_config.has(nested_secret_path) ? nested_secret_path : secret_path;

        // Can't use Poco::AutoPtr with 'const T*'
        UniquePtrPocoRefCounted<const Poco::Util::AbstractConfiguration> secret_config(secrets_config.createView(secret_config_path));
        try
        {
            auto secret = backend.getSecret(*secret_config);
            new_secrets.emplace(std::move(secret_name), std::move(secret));
        }
        catch (const Exception & e)
        {
            throw Exception(fmt::format("Failed to load secret '{}' : {}", secret_name, e.what()), e, e.code());
        }
    }

    {
        std::lock_guard lock(mutex);
        loaded_secrets.swap(new_secrets);
    }
}

void SecretsManager::registerSecretsBackend(const String & name, std::unique_ptr<ISecretBackend> backend)
{
    if (getBackendOrNull(name) != nullptr)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Secret backend is already registered: {}", name);

    all_backends.emplace(name, std::move(backend));
}

std::shared_ptr<const Secret> SecretsManager::getSecret(std::string_view secret_name) const
{
    std::shared_lock lock(mutex);
    const auto & p = loaded_secrets.find(ext::heterogeneousKey(secret_name));

    if (p == loaded_secrets.end())
        throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown secret name: {}", secret_name);

    return p->second;
}

SecretValue SecretsManager::getSecretValue(std::string_view path_to_secret) const
{
    try
    {
        const auto separator_pos = path_to_secret.find('.');
        const auto secret_name = path_to_secret.substr(0, separator_pos);
        const auto path = separator_pos == std::string::npos ? std::string_view() : path_to_secret.substr(separator_pos + 1);
        return SecretValue(getSecret(secret_name), path);
    }
    catch (const Exception & e)
    {
        throw Exception(fmt::format("Can't get secret value by path: {}", path_to_secret), e, e.code());
    }
}

String SecretsManager::redactOutAllSecrets(const String & text_with_secrets, ReplacamentFunc replacement_func) const
{
    std::shared_lock lock(mutex);

    String result = text_with_secrets;

    for (const auto & secret : loaded_secrets)
    {
        for (const auto & vals : secret.second->values)
        {
            auto p = result.find(vals.second);
            if (p != std::string::npos)
            {
                const std::string full_secret_name = vals.first.empty() ? secret.first : secret.first + "." + vals.first;
                const std::string substitution = replacement_func(full_secret_name);

                while (p != std::string::npos)
                {
                    result.replace(p, vals.second.size(), substitution);
                    p = result.find(vals.second, p + substitution.size());
                }
            }
        }
    }

    return result;
}

const ISecretBackend * SecretsManager::getBackendOrNull(const String & name) const
{
    // assuming that mutex is locked
    if (const auto p = all_backends.find(name); p != all_backends.end())
        return p->second.get();

    return nullptr;
}

const ISecretBackend & SecretsManager::getBackend(const String & name) const
{
    // assuming that mutex is locked
    if (const auto * backend = getBackendOrNull(name))
        return *backend;

    throw Exception(ErrorCodes::UNKNOWN_IDENTIFIER, "Unknown secret backend name: {}", name);
}

}
