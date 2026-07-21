#include <Common/Crypto/KeyPair.h>

#if USE_SSL

#include <base/scope_guard.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int OPENSSL_ERROR;
}

KeyPair::KeyPair(EVP_PKEY * key_)
    : key(key_)
{
}

KeyPair::operator EVP_PKEY *() const
{
    return key;
}

KeyPair::KeyPair(KeyPair && other) noexcept
{
    key = other.key;
    other.key = nullptr;
}

KeyPair & KeyPair::operator=(KeyPair && other) noexcept
{
    if (this == &other)
        return *this;

    if (key)
        EVP_PKEY_free(key);

    key = other.key;
    other.key = nullptr;

    return *this;
}

KeyPair KeyPair::fromFile(const std::string & path, const std::string & password)
{
    EVP_PKEY * key = nullptr;

    /// Try to load a private key.
    {
        BIO_ptr file(BIO_new_file(path.c_str(), "r"), BIO_free);

        if (!file)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "BIO_new_file failed: {}", getOpenSSLErrors());

        key = PEM_read_bio_PrivateKey(file.get(), nullptr, nullptr, password.empty() ? nullptr : const_cast<char *>(password.c_str()));
    }

    /// Maybe it is a public key.
    if (!key)
    {
        BIO_ptr file(BIO_new_file(path.c_str(), "r"), BIO_free);

        if (!file)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "BIO_new_file failed: {}", getOpenSSLErrors());

        key = PEM_read_bio_PUBKEY(file.get(), nullptr, nullptr, nullptr);
    }

    if (!key)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "Failed to load key from a file: {}", getOpenSSLErrors());

    return KeyPair(key);
}

KeyPair KeyPair::generateRSA(uint32_t bits, uint32_t exponent)
{
    EVP_PKEY * key = nullptr;

    using EVP_PKEY_CTX_ptr = std::unique_ptr<EVP_PKEY_CTX, decltype(&EVP_PKEY_CTX_free)>;
    EVP_PKEY_CTX_ptr ctx(EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr), EVP_PKEY_CTX_free);

    if (!ctx)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_PKEY_CTX_new_id failed: {}", getOpenSSLErrors());

    if (EVP_PKEY_keygen_init(ctx.get()) <= 0)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_PKEY_keygen_init failed: {}", getOpenSSLErrors());

    BIGNUM * bn = BN_new();
    SCOPE_EXIT({ BN_free(bn); });

    if (!bn || !BN_set_word(bn, exponent))
        throw Exception(ErrorCodes::OPENSSL_ERROR, "BN_set_word failed: {}", getOpenSSLErrors());

    size_t exp_len = BN_num_bytes(bn);
    std::vector<unsigned char> exp_buf(exp_len);
    BN_bn2bin(bn, exp_buf.data());

    OSSL_PARAM params[] = {OSSL_PARAM_int("bits", &bits), OSSL_PARAM_BN("pubexp", exp_buf.data(), exp_buf.size()), OSSL_PARAM_END};

    if (EVP_PKEY_CTX_set_params(ctx.get(), params) <= 0)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_PKEY_CTX_set_params failed: {}", getOpenSSLErrors());

    if (EVP_PKEY_keygen(ctx.get(), &key) <= 0)
    {
        if (key)
            EVP_PKEY_free(key);
        throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_PKEY_keygen failed: {}", getOpenSSLErrors());
    }

    return KeyPair(key);
}

KeyPair::~KeyPair()
{
    if (key)
        EVP_PKEY_free(key);
}

std::string KeyPair::publicKey() const
{
    BIO_ptr bio(BIO_new(BIO_s_mem()), BIO_free);

    if (!bio)
        throw Exception(ErrorCodes::OPENSSL_ERROR, "BIO_new failed: {}", getOpenSSLErrors());

    if (!PEM_write_bio_PUBKEY(bio.get(), key))
        throw Exception(ErrorCodes::OPENSSL_ERROR, "PEM_write_bio_PUBKEY failed: {}", getOpenSSLErrors());

    char * data;
    uint64_t len = BIO_get_mem_data(bio.get(), &data);
    std::string result(data, len);

    return result;
}

}
#endif
