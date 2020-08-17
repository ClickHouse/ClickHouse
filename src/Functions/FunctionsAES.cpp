#include <Functions/FunctionsAES.h>

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
    memcpy(folded_key.data(), key.data, cipher_key_size);

    for (size_t i = cipher_key_size; i < key.size; ++i)
    {
        folded_key[i % cipher_key_size] ^= key.data[i];
    }

    return StringRef(folded_key.data(), cipher_key_size);
}

CipherPtr getCipherByName(const StringRef & cipher_name)
{
    const auto *evp_cipher = EVP_get_cipherbyname(cipher_name.data);
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

    // HACK: To speed up context initialization with EVP_EncryptInit_ex (which is called at least once per row)
    // Apparently cipher from EVP_get_cipherbyname may require additional initialization of context,
    // while cipher from EVP_CIPHER_fetch causes less operations => faster context initialization.
    return CipherPtr{EVP_CIPHER_fetch(nullptr, EVP_CIPHER_name(evp_cipher), nullptr), &EVP_CIPHER_free};
}

}

#endif
