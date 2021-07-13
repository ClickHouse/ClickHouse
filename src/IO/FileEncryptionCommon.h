#pragma once

#if !defined(ARCADIA_BUILD)
#include <Common/config.h>
#endif

#if USE_SSL
#include <Core/Types.h>
#include <openssl/evp.h>

namespace DB
{
class ReadBuffer;
class WriteBuffer;

namespace FileEncryption
{

constexpr size_t kIVSize = sizeof(UInt128);

class InitVector
{
public:
    InitVector(const String & iv_);
    const String & str() const;
    void inc() { ++counter; }
    void inc(size_t n) { counter += n; }
    void set(size_t n) { counter = n; }

private:
    UInt128 iv;
    UInt128 counter = 0;
    mutable String local;
};


class EncryptionKey
{
public:
    EncryptionKey(const String & key_) : key(key_) { }
    size_t size() const { return key.size(); }
    const String & str() const { return key; }

private:
    String key;
};


class Encryption
{
public:
    Encryption(const String & iv_, const EncryptionKey & key_, size_t offset_);

protected:
    size_t blockOffset(size_t pos) const { return pos % block_size; }
    size_t blocks(size_t pos) const { return pos / block_size; }
    size_t partBlockSize(size_t size, size_t off) const;
    const EVP_CIPHER * get() const { return evp_cipher; }

    const EVP_CIPHER * evp_cipher;
    const String init_vector;
    const EncryptionKey key;
    size_t block_size;

    /// absolute offset
    size_t offset = 0;
};


class Encryptor : public Encryption
{
public:
    using Encryption::Encryption;
    void encrypt(const char * plaintext, WriteBuffer & buf, size_t size);

private:
    String encryptPartialBlock(const char * partial_block, size_t size, const InitVector & iv, size_t off) const;
    String encryptNBytes(const char * data, size_t bytes, const InitVector & iv) const;
};


class Decryptor : public Encryption
{
public:
    Decryptor(const String & iv_, const EncryptionKey & key_) : Encryption(iv_, key_, 0) { }
    void decrypt(const char * ciphertext, char * buf, size_t size, size_t off);

private:
    void decryptPartialBlock(char *& to, const char * partial_block, size_t size, const InitVector & iv, size_t off) const;
    void decryptNBytes(char *& to, const char * data, size_t bytes, const InitVector & iv) const;
};


String readIV(size_t size, ReadBuffer & in);
String randomString(size_t size);
void writeIV(const String & iv, WriteBuffer & out);
size_t cipherKeyLength(const EVP_CIPHER * evp_cipher);
size_t cipherIVLength(const EVP_CIPHER * evp_cipher);
const EVP_CIPHER * defaultCipher();

}
}

#endif
