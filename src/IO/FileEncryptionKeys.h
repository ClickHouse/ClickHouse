#pragma once

#include "config.h"

#if USE_SSL
#include <IO/FileEncryptionCommon.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <unordered_map>

namespace DB::FileEncryption
{

/// The encryption keys of a configuration section: an encrypted disk, or the encrypted storage for
/// named collections. Several keys can be configured so that a key can be rotated: the current key
/// encrypts new data, and any of the keys decrypts data whose header names its fingerprint.
struct Keys
{
    String current_key;
    UInt128 current_key_fingerprint{};
    Algorithm current_algorithm{};
    std::unordered_map<UInt128 /* fingerprint */, String /* key */> all_keys; /// STYLE_CHECK_ALLOW_STD_CONTAINERS

    /// Returns the key with the given fingerprint. Throws if there is none; `what_for_logs` names
    /// the encrypted data in the message, e.g. "file 'path'".
    String findKeyByFingerprint(UInt128 key_fingerprint, const String & what_for_logs) const;
};

/// Reads the keys of a configuration section:
/// - `key` (plain text) and `key_hex` (hexadecimal) elements; each may carry an `id` attribute, which
///   allows several keys: `<key_hex id="0">...</key_hex> <key_hex id="1">...</key_hex>`;
/// - `current_key`, `current_key_hex` or `current_key_id` selecting the key used for encryption,
///   not needed with a single key;
/// - `algorithm`, `aes_128_ctr` by default.
/// Throws if there are no keys, two keys share an id, the current key is not specified or not among
/// the keys, or the size of the current key does not fit the algorithm.
Keys parseKeysFromConfig(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

}

#endif
