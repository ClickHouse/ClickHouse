#include <Functions/FunctionsAES.h>

#if USE_SSL

#include <openssl/evp.h>
#include <openssl/err.h>
#if USE_INTERNAL_SSL_LIBRARY
#  include <openssl/hkdf.h>
#endif

#include <string>
#include <cassert>


namespace DB
{
namespace ErrorCodes
{
    extern const int OPENSSL_ERROR;
}
}

namespace OpenSSLDetails
{
void onError(std::string error_message)
{
    error_message += ". OpenSSL error code: " + std::to_string(ERR_get_error());
    throw DB::Exception(error_message, DB::ErrorCodes::OPENSSL_ERROR);
}

StringRef foldEncryptionKeyInMySQLCompatitableMode(size_t cipher_key_size, const StringRef & key, std::array<char, EVP_MAX_KEY_LENGTH> & folded_key)
{
    assert(cipher_key_size <= EVP_MAX_KEY_LENGTH);
    memcpy(folded_key.data(), key.data, cipher_key_size);

    for (size_t i = cipher_key_size; i < key.size; ++i)
    {
        folded_key[i % cipher_key_size] ^= key.data[i];
    }

    return StringRef(folded_key.data(), cipher_key_size);
}

const EVP_CIPHER * getCipherByName(const StringRef & cipher_name)
{
    const auto * evp_cipher = EVP_get_cipherbyname(cipher_name.data);
    if (evp_cipher == nullptr)
    {
        // For some reasons following ciphers can't be found by name.
        if (cipher_name == "aes-128-cfb128")
            evp_cipher = EVP_aes_128_cfb128();
        else if (cipher_name == "aes-192-cfb128")
            evp_cipher = EVP_aes_192_cfb128();
        else if (cipher_name == "aes-256-cfb128")
            evp_cipher = EVP_aes_256_cfb128();
    }

    // NOTE: cipher obtained not via EVP_CIPHER_fetch() would cause extra work on each context reset
    // with EVP_CIPHER_CTX_reset() or EVP_EncryptInit_ex(), but using EVP_CIPHER_fetch()
    // causes data race, so we stick to the slower but safer alternative here.
    return evp_cipher;
}

// This depends on BoringSSL-specific API.
#if USE_INTERNAL_SSL_LIBRARY
void ServerKey::setMasterKey(const std::string_view & master_key_)
{
    master_key = std::unique_ptr<const ServerKey>(new ServerKey(master_key_));
}

std::unique_ptr<const ServerKey> & ServerKey::getMasterKey()
{
    return master_key;
}

StringRef ServerKey::derive(size_t octets) const
{
    assert(octets <= derived_key.size());
    return StringRef(derived_key.data(), octets);
}

ServerKey::ServerKey(const std::string_view & master_key_)
{
    std::string_view salt(""); // No salt: derive keys in a deterministic manner.
    std::string_view info(""); // No info: nothing is applicable.
    std::array<char, 32> result;

    // BoringSSL and OpenSSL have vastly different API for HKDF. Maybe
    // it's possible to support both, but for now we only support the
    // former.
    const int ok = HKDF(reinterpret_cast<uint8_t *>(result.data()), result.size(),
                        EVP_sha256(),
                        reinterpret_cast<const uint8_t *>(master_key_.data()), master_key_.size(),
                        reinterpret_cast<const uint8_t *>(salt.data()), salt.size(),
                        reinterpret_cast<const uint8_t *>(info.data()), info.size());
    if (!ok)
        onError("Failed to derive a key from the master key");

    derived_key = std::string(result.data(), result.size());
}
#endif

}

#endif
