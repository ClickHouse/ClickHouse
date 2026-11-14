#include <IO/FileEncryptionKeys.h>

#if USE_SSL
#include <Common/Exception.h>
#include <Common/Hex.h>
#include <boost/algorithm/hex.hpp>

#if CLICKHOUSE_CLOUD
#include <Core/KMS.h>
#endif

#include <map>
#include <optional>
#include <vector>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DATA_ENCRYPTION_ERROR;
}

namespace FileEncryption
{

namespace
{
    constexpr Algorithm DEFAULT_ENCRYPTION_ALGORITHM = Algorithm::AES_128_CTR;

    String unhexKey(const String & hex)
    {
        try
        {
            return boost::algorithm::unhex(hex);
        }
        catch (const std::exception &)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot read key_hex, check for valid characters [0-9a-fA-F] and length");
        }
    }

    struct Key
    {
        String plain;
        std::optional<String> encrypted;
    };

    /// A handful of keys read once from the configuration: no memory tracking needed.
    using KeysById = std::map<UInt64, Key>; /// STYLE_CHECK_ALLOW_STD_CONTAINERS
    using KeysWithoutId = std::vector<Key>; /// STYLE_CHECK_ALLOW_STD_CONTAINERS

    /// Reads encryption keys from the configuration.
    void getKeysFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix,
                           KeysById & out_keys_by_id, KeysWithoutId & out_keys_without_id)
    {
        Strings config_keys;
        config.keys(config_prefix, config_keys);

        /// The keys are picked by a pattern of their names (`key`, `key[1]`, ...). Look up the bare names,
        /// so that a configuration tracking the read keys (`ConfigurationWithUsageTracking`, used for
        /// a definition of a disk) knows that the elements with these names are read even when they are absent.
        config.has(config_prefix + ".key");
        config.has(config_prefix + ".key_hex");
#if CLICKHOUSE_CLOUD
        config.has(config_prefix + ".key_aws");
        config.has(config_prefix + ".key_gcp");
        config.has(config_prefix + ".key_azure");
        /// The options of a key management service are read only to decrypt a key, so they are read only
        /// when there is such a key. Look them up as well, so that they are known for a disk that gets
        /// such a key on a configuration reload.
        config.has(config_prefix + ".no_sign_request");
        config.has(config_prefix + ".aws_kms_endpoint");
#endif

        for (const std::string & config_key : config_keys)
        {
            Key key;
            std::optional<UInt64> key_id;

            if ((config_key == "key") || config_key.starts_with("key["))
            {
                String key_path = config_prefix + "." + config_key;
                key.plain = config.getString(key_path);
                String key_id_path = key_path + "[@id]";
                if (config.has(key_id_path))
                    key_id = config.getUInt64(key_id_path);
            }
            else if ((config_key == "key_hex") || config_key.starts_with("key_hex["))
            {
                String key_path = config_prefix + "." + config_key;
                key.plain = unhexKey(config.getString(key_path));
                String key_id_path = key_path + "[@id]";
                if (config.has(key_id_path))
                    key_id = config.getUInt64(key_id_path);
            }
#if CLICKHOUSE_CLOUD
            else if ((config_key == "key_aws") || config_key.starts_with("key_aws[") || (config_key == "key_gcp") || config_key.starts_with("key_gcp[") ||
                     (config_key == "key_azure") || config_key.starts_with("key_azure["))
            {
                String key_path = config_prefix + "." + config_key;
                String key_id_path = key_path + "[@id]";
                if (config.has(key_id_path))
                    key_id = config.getUInt64(key_id_path);
                key.plain = decryptKeyUsingCloudKeyManagementService(config,config_prefix, key_path, config_key);
                key.encrypted = config.getString(key_path);
            }
#endif
            else
                continue;

            if (key_id)
            {
                if (!out_keys_by_id.contains(*key_id))
                    out_keys_by_id[*key_id] = key;
                else
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple keys specified for same ID {}", *key_id);
            }
            else
                out_keys_without_id.push_back(key);
        }

        if (out_keys_by_id.empty() && out_keys_without_id.empty())
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "No encryption keys found");

        if (out_keys_by_id.empty() && (out_keys_without_id.size() == 1))
        {
            out_keys_by_id[0] = out_keys_without_id.front();
            out_keys_without_id.clear();
        }
    }

    /// Reads the current encryption key from the configuration.
    const Key & getCurrentKeyFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix,
                                        const KeysById & keys_by_id, const KeysWithoutId & keys_without_id)
    {
        String key_path = config_prefix + ".current_key";
        String key_hex_path = config_prefix + ".current_key_hex";
        String key_id_path = config_prefix + ".current_key_id";

        if (config.has(key_path) + config.has(key_hex_path) + config.has(key_id_path) > 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The current key is specified multiple times");

        auto find_current_key = [&](const String & current_key_) -> const Key &
        {
            for (const auto & [_, key] : keys_by_id)
            {
                if (key.plain == current_key_)
                    return key;

                if (std::string_view(key.encrypted.value_or("")) == std::string_view(current_key_))
                    return key;
            }
            for (const auto & key : keys_without_id)
            {
                if (key.plain == current_key_)
                    return key;

                if (std::string_view(key.encrypted.value_or("")) == std::string_view(current_key_))
                    return key;
            }
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "The current key is not found in keys");
        };

        if (config.has(key_path))
        {
            String current_key = config.getString(key_path);
            return find_current_key(current_key);
        }
        if (config.has(key_hex_path))
        {
            String current_key = unhexKey(config.getString(key_hex_path));
            return find_current_key(current_key);
        }
        if (config.has(key_id_path))
        {
            UInt64 current_key_id = config.getUInt64(key_id_path);
            auto it = keys_by_id.find(current_key_id);
            if (it == keys_by_id.end())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found a key with the current ID {}", current_key_id);
            return it->second;
        }
        if (keys_by_id.size() == 1 && keys_without_id.empty() && keys_by_id.begin()->first == 0)
        {
            /// There is only a single key defined with id=0, so we can choose it as current.
            return keys_by_id.begin()->second;
        }

        throw Exception(ErrorCodes::BAD_ARGUMENTS, "The current key is not specified");
    }

    /// Reads the current encryption algorithm from the configuration.
    Algorithm getCurrentAlgorithmFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
    {
        String path = config_prefix + ".algorithm";
        if (!config.has(path))
            return DEFAULT_ENCRYPTION_ALGORITHM;
        return parseAlgorithmFromString(config.getString(path));
    }
}


String Keys::findKeyByFingerprint(UInt128 key_fingerprint, const String & what_for_logs) const
{
    auto it = all_keys.find(key_fingerprint);
    if (it == all_keys.end())
    {
        throw Exception(
            ErrorCodes::DATA_ENCRYPTION_ERROR,
            "Not found an encryption key required to decipher {}: it is encrypted with a key with fingerprint {}",
            what_for_logs,
            getHexUIntLowercase(key_fingerprint));
    }
    return it->second;
}


Keys parseKeysFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    Keys res;

    KeysById keys_by_id;
    KeysWithoutId keys_without_id;
    getKeysFromConfig(config, config_prefix, keys_by_id, keys_without_id);

    for (const auto & [key_id, key_entry] : keys_by_id)
    {
        const auto key = key_entry.plain;
        auto fingerprint = calculateKeyFingerprint(key);
        res.all_keys[fingerprint] = key;

        /// Version 1 used key fingerprints based on the key id.
        /// We have to add such fingerprints to the map too to support reading files encrypted by version 1.
        auto v1_fingerprint = calculateV1KeyFingerprint(key, key_id);
        res.all_keys[v1_fingerprint] = key;
    }

    for (const auto & key : keys_without_id)
    {
        auto fingerprint = calculateKeyFingerprint(key.plain);
        res.all_keys[fingerprint] = key.plain;
    }

    const Key & current_key = getCurrentKeyFromConfig(config, config_prefix, keys_by_id, keys_without_id);
    res.current_key = current_key.plain;
    res.current_key_fingerprint = calculateKeyFingerprint(current_key.plain);

    res.current_algorithm = getCurrentAlgorithmFromConfig(config, config_prefix);

    checkKeySize(res.current_key.size(), res.current_algorithm);

    return res;
}

}

}

#endif
