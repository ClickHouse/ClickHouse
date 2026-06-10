#include <Functions/FunctionsAES.h>
#include <Interpreters/Context.h>
#include <Common/OpenSSLHelpers.h>

#if USE_SSL

#include <openssl/evp.h>
#include <openssl/err.h>

#include <string>

namespace DB
{
namespace ErrorCodes
{
    extern const int OPENSSL_ERROR;
}

namespace OpenSSLDetails
{
void onError(std::string error_message)
{
    throw Exception(ErrorCodes::OPENSSL_ERROR, "{} failed: {}", error_message, getOpenSSLErrors());
}

std::string_view foldEncryptionKeyInMySQLCompatitableMode(size_t cipher_key_size, std::string_view key, std::array<char, EVP_MAX_KEY_LENGTH> & folded_key)
{
    chassert(cipher_key_size <= EVP_MAX_KEY_LENGTH);
    memcpy(folded_key.data(), key.data(), cipher_key_size);

    for (size_t i = cipher_key_size; i < key.size(); ++i)
    {
        folded_key[i % cipher_key_size] ^= key[i];
    }

    return std::string_view(folded_key.data(), cipher_key_size);
}

const EVP_CIPHER * getCipherByName(std::string_view cipher_name)
{
    /// NOTE: the cipher obtained via EVP_get_cipherbyname has prov == NULL.
    /// When such cipher is passed to EVP_EncryptInit_ex / EVP_DecryptInit_ex,
    /// OpenSSL 3.x implicitly calls EVP_CIPHER_fetch on every invocation,
    /// which involves locking and provider lookups.
    /// Use `fetchCipher` to obtain a provider-backed cipher that avoids this overhead.

    /// We need zero-terminated string here:
    return EVP_get_cipherbyname(std::string{cipher_name}.c_str());
}

EVP_CIPHER_ptr fetchCipher(std::string_view cipher_name)
{
    auto * fetched = EVP_CIPHER_fetch(nullptr, std::string{cipher_name}.c_str(), nullptr);
    if (!fetched)
        onError("EVP_CIPHER_fetch");
    return EVP_CIPHER_ptr(fetched, EVP_CIPHER_free);
}

}

}

#endif
