#pragma once

#include <openssl/evp.h>
#include <openssl/engine.h>

#include <IO/ReadBuffer.h>
#include <IO/WriteBuffer.h>

#include <IO/WriteHelpers.h>

#include <common/logger_useful.h>
#include <sys/random.h>

namespace DB
{

class InitVector
{
public:
    InitVector(String iv_) : iv(iv_) { }

    size_t Size() const { return iv.size(); }
    String Data() const { return iv; }

private:
    String iv;
};

class EncryptionKey
{
public:
    EncryptionKey(String key_) : key(key_) { }
    String Get() const { return key; }

private:
    String key;
};


InitVector ReadIV(size_t size, ReadBuffer & in)
{
    Poco::Logger * log = &Poco::Logger::get("FileEncryption");
    LOG_WARNING(log, "ReadInitVector {}", size);
    String iv;
    iv.resize(size);
    in.readStrict(reinterpret_cast<char *>(iv.data()), size);
    LOG_WARNING(log, "read iv = {}", iv);
    return InitVector(iv);
}

InitVector GetRandomIV(size_t size)
{
    Poco::Logger * log = &Poco::Logger::get("FileEncryption");
    LOG_WARNING(log, "GetRandomIV {}", size);
    String iv;
    iv.resize(size);
    getrandom(iv.data(), size, GRND_NONBLOCK);
    LOG_WARNING(log, "generated iv = {}", iv);
    return InitVector(iv);
}

void WriteIV(const InitVector & iv, WriteBuffer & out)
{
    Poco::Logger * log = &Poco::Logger::get("FileEncryption");
    LOG_WARNING(log, "WriteInitVector {}", iv.Data());
    writeText(iv.Data(), out);
}

class Encryption
{
public:
    Encryption(const InitVector & iv_, const EncryptionKey & key_, const EVP_CIPHER * evp_cipher_, size_t offset_ = 0)
        : evp_cipher(evp_cipher_)
        , iv(iv_)
        , key(key_)
        , block_size(static_cast<size_t>(EVP_CIPHER_block_size(evp_cipher_)))
    {
        blocks = Blocks(offset_);
        offset = BlockOffset(offset_);
    }

    size_t SizeByInputSize(size_t input_size) const { return input_size; }
    Poco::Logger * log = &Poco::Logger::get("FileEncryption");

private:

    size_t Blocks(size_t pos) { return pos / block_size; }

    size_t BlockOffset(size_t pos) const { return pos % block_size; }

//    size_t BlocksAlign(size_t pos) const { return pos - BlockOffset(pos); }

//    size_t BlockStartPos(size_t pos) const { return iv.Size() + pos - BlockOffset(pos); }

    const EVP_CIPHER * get() const { return evp_cipher; }

    const EVP_CIPHER * evp_cipher;
    InitVector iv;
    EncryptionKey key;
    size_t blocks = 0;
    size_t block_size;
    size_t offset = 0;
};

class Encryptor : public Encryption
{
public:
    Encryptor(const InitVector & iv_, const EncryptionKey & key_, const EVP_CIPHER * evp_cipher_, size_t offset_)
        : Encryption(iv_, key_, evp_cipher_, offset_) { }

    void Encrypt(const char * plaintext, WriteBuffer & buf, size_t size)
    {
        if (!size)
            return;
        LOG_WARNING(log, "Encrypt with size {}", size);
        buf.write(plaintext, size);
    }
};

class Decryptor : public Encryption
{
public:
    Decryptor(const InitVector & iv_, const EncryptionKey & key_, const EVP_CIPHER * evp_cipher_)
        : Encryption(iv_, key_, evp_cipher_) { }

    void Decrypt(const char * ciphertext, const BufferBase::Buffer & buf, size_t size)
    {
        if (!size)
            return;
        LOG_WARNING(log, "Decrypt with size {}", size);
        WriteBuffer(buf.begin(), buf.size()).write(ciphertext, size);
    }
};

}
