#include <Functions/FunctionsAES.h>

#if USE_SSL

#include <openssl/evp.h>
#include <openssl/err.h>

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

StringRef foldEncryptionKeyInMySQLCompatitableMode(size_t cipher_key_size, StringRef key, std::array<char, EVP_MAX_KEY_LENGTH> & folded_key)
{
    assert(cipher_key_size <= EVP_MAX_KEY_LENGTH);
    memcpy(folded_key.data(), key.data, cipher_key_size);

    for (size_t i = cipher_key_size; i < key.size; ++i)
    {
        folded_key[i % cipher_key_size] ^= key.data[i];
    }

    return StringRef(folded_key.data(), cipher_key_size);
}

const EVP_CIPHER * getCipherByName(StringRef cipher_name)
{
    // NOTE: cipher obtained not via EVP_CIPHER_fetch() would cause extra work on each context reset
    // with EVP_CIPHER_CTX_reset() or EVP_EncryptInit_ex(), but using EVP_CIPHER_fetch()
    // causes data race, so we stick to the slower but safer alternative here.
    return EVP_get_cipherbyname(cipher_name.data);
}

}

#endif
