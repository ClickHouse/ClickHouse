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

EVP_CIPHER_ptr fetchCipher(std::string_view cipher_name)
{
    /// A cipher obtained via EVP_get_cipherbyname has prov == NULL, which makes OpenSSL 3.x
    /// implicitly call EVP_CIPHER_fetch (with locking and provider lookups) on every
    /// EVP_EncryptInit_ex / EVP_DecryptInit_ex invocation. Fetching the cipher explicitly
    /// here returns a provider-backed cipher and avoids that per-row overhead.
    /// Returns nullptr for an unknown cipher name; the caller reports it as an invalid mode.
    /// We need a zero-terminated string here:
    auto * fetched = EVP_CIPHER_fetch(nullptr, std::string{cipher_name}.c_str(), nullptr);
    if (!fetched)
        /// For an unknown name EVP_CIPHER_fetch pushes an "unsupported" entry onto the
        /// thread-local OpenSSL error queue. The caller turns nullptr into a BAD_ARGUMENTS
        /// "Invalid mode" without draining the queue via getOpenSSLErrors, so clear it here to
        /// avoid leaking this stale entry into an unrelated OpenSSL error later reported on the
        /// same thread (unlike EVP_get_cipherbyname, which never touched the queue).
        ERR_clear_error();
    return EVP_CIPHER_ptr(fetched, EVP_CIPHER_free);
}

std::string_view ecbEquivalentCipherName(std::string_view mode)
{
    /// Only plain AES block ciphers. The list is intentionally exact: e.g.
    /// aes-128-cbc-hmac-sha1 also reports EVP_CIPH_CBC_MODE but is not a plain
    /// block cipher and must not take the block-composed fast path.
    if (mode == "aes-128-ecb" || mode == "aes-128-cbc")
        return "aes-128-ecb";
    if (mode == "aes-192-ecb" || mode == "aes-192-cbc")
        return "aes-192-ecb";
    if (mode == "aes-256-ecb" || mode == "aes-256-cbc")
        return "aes-256-ecb";
    return {};
}

}

}

#endif
