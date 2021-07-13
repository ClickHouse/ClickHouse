#pragma once

#if USE_SSL

#include <openssl/evp.h>
#include <openssl/engine.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <sys/random.h>

#include <limits>
#include <cassert>
#include <cmath>

#include <errno.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int DATA_ENCRYPTION_ERROR;
}
}

namespace FileEncryption
{

constexpr size_t kIVSize = sizeof(DB::UInt128);


class InitVector
{
public:
    InitVector(String iv_) : iv(FromBigEndianString(iv_)) { }

    const char * GetRef() const
    {
        local = ToBigEndianString(iv + counter);
        return local.data();
    }

    void Inc() { ++counter; }
    void Inc(size_t n) { counter += n; }
    void Set(size_t n) { counter = n; }

private:
    String ToBigEndianString(DB::UInt128 value) const
    {
        WriteBufferFromOwnString out;
        writeBinaryBigEndian(value, out);
        return std::move(out.str());
    }

    DB::UInt128 FromBigEndianString(const String & str) const
    {
        ReadBufferFromMemory in{str.data(), str.length()};
        DB::UInt128 result;
        readBinaryBigEndian(result, in);
        return result;
    }

    DB::UInt128 iv;
    DB::UInt128 counter = 0;
    mutable String local;
};


class EncryptionKey
{
public:
    EncryptionKey(String key_) : key(key_) { }
    size_t Size() const { return key.size(); }
    const char * GetRef() const { return key.data(); }

private:
    String key;
};


String ReadIV(size_t size, DB::ReadBuffer & in)
{
    String iv(size, 0);
    in.readStrict(reinterpret_cast<char *>(iv.data()), size);
    return iv;
}

String GetRandomString(size_t size)
{
    String iv(size, 0);
    int ret = 0;
    size_t cur = 0;

    while (cur < size)
    {
        ret = getrandom(iv.data() + cur, size - cur, 0);
        if (ret < 0)
        {
            if (errno == EINTR) continue;
            else break;
        }
        cur += ret;
        ret = 0;
    }

    if (ret < 0)
        throw DB::Exception("Failed to generate IV string with size " + std::to_string(size),
                            DB::ErrorCodes::DATA_ENCRYPTION_ERROR);
    return iv;
}

void WriteIV(const String & iv, DB::WriteBuffer & out)
{
    for (auto i : iv)
        DB::writeChar(i, out);
}

size_t CipherKeyLength(const EVP_CIPHER * evp_cipher)
{
    return static_cast<size_t>(EVP_CIPHER_key_length(evp_cipher));
}

size_t CipherIVLength(const EVP_CIPHER * evp_cipher)
{
    return static_cast<size_t>(EVP_CIPHER_iv_length(evp_cipher));
}

const EVP_CIPHER * DefaultCipher()
{
    return EVP_aes_128_ctr();
}


class Encryption
{
public:
    Encryption(const String & iv_, const EncryptionKey & key_, size_t offset_ = 0)
        : evp_cipher(DefaultCipher())
        , init_vector(iv_)
        , key(key_)
        , block_size(CipherIVLength(evp_cipher))
    {
        if (iv_.size() != CipherIVLength(evp_cipher))
            throw DB::Exception("Expected iv with size " + std::to_string(CipherIVLength(evp_cipher)) + ", got iv with size " + std::to_string(iv_.size()),
                                DB::ErrorCodes::DATA_ENCRYPTION_ERROR);
        if (key_.Size() != CipherKeyLength(evp_cipher))
            throw DB::Exception("Expected key with size " + std::to_string(CipherKeyLength(evp_cipher)) + ", got iv with size " + std::to_string(key_.Size()),
                                DB::ErrorCodes::DATA_ENCRYPTION_ERROR);

        offset = offset_;
    }

protected:

    size_t BlockOffset(size_t pos) const { return pos % block_size; }

    size_t Blocks(size_t pos) const { return pos / block_size; }

    size_t PartBlockSize(size_t size, size_t off) const
    {
        assert(off < block_size);
        /// write the part as usual block
        if (off == 0)
            return 0;
        return off + size <= block_size ? size : (block_size - off) % block_size;
    }

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
    Encryptor(const String & iv_, const EncryptionKey & key_, size_t off)
        : Encryption(iv_, key_, off) { }

    void Encrypt(const char * plaintext, DB::WriteBuffer & buf, size_t size)
    {
        if (!size)
            return;

        auto iv = InitVector(init_vector);
        auto off = BlockOffset(offset);
        iv.Set(Blocks(offset));

        size_t part_size = PartBlockSize(size, off);
        if (off)
        {
            buf.write(EncryptPartialBlock(plaintext, part_size, iv, off).data(), part_size);
            offset += part_size;
            size -= part_size;
            iv.Inc();
        }

        if (size)
        {
            buf.write(EncryptNBytes(plaintext + part_size, size, iv).data(), size);
            offset += size;
        }
    }

private:
    String EncryptPartialBlock(const char * partial_block, size_t size, const InitVector & iv, size_t off) const
    {
        if (size > block_size)
            throw DB::Exception("Expected partial block, got block with size > block_size: size = " + std::to_string(size) + " and offset = " + std::to_string(off),
                                DB::ErrorCodes::DATA_ENCRYPTION_ERROR);

        String plaintext(block_size, '\0');
        for (size_t i = 0; i < size; ++i)
            plaintext[i + off] = partial_block[i];

        return String(EncryptNBytes(plaintext.data(), block_size, iv), off, size);
    }

    String EncryptNBytes(const char * data, size_t bytes, const InitVector & iv) const
    {
        String ciphertext(bytes, '\0');
        auto * ciphertext_ref = ciphertext.data();

        auto evp_ctx_ptr = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>(EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free);
        auto * evp_ctx = evp_ctx_ptr.get();

        if (EVP_EncryptInit_ex(evp_ctx, evp_cipher, nullptr, nullptr, nullptr) != 1)
            throw DB::Exception("Failed to initialize encryption context with cipher", DB::ErrorCodes::DATA_ENCRYPTION_ERROR);

        if (EVP_EncryptInit_ex(evp_ctx, nullptr, nullptr,
            reinterpret_cast<const unsigned char*>(key.GetRef()),
            reinterpret_cast<const unsigned char*>(iv.GetRef())) != 1)
            throw DB::Exception("Failed to set key and IV for encryption", DB::ErrorCodes::DATA_ENCRYPTION_ERROR);

        int output_len = 0;
        if (EVP_EncryptUpdate(evp_ctx,
            reinterpret_cast<unsigned char*>(ciphertext_ref), &output_len,
            reinterpret_cast<const unsigned char*>(data), static_cast<int>(bytes)) != 1)
            throw DB::Exception("Failed to encrypt", DB::ErrorCodes::DATA_ENCRYPTION_ERROR);

        ciphertext_ref += output_len;

        int final_output_len = 0;
        if (EVP_EncryptFinal_ex(evp_ctx,
            reinterpret_cast<unsigned char*>(ciphertext_ref), &final_output_len) != 1)
            throw DB::Exception("Failed to fetch ciphertext", DB::ErrorCodes::DATA_ENCRYPTION_ERROR);

        if (output_len < 0 || final_output_len < 0 || static_cast<size_t>(output_len) + static_cast<size_t>(final_output_len) != bytes)
            throw DB::Exception("Only part of the data was encrypted", DB::ErrorCodes::DATA_ENCRYPTION_ERROR);

        return ciphertext;
    }
};


class Decryptor : public Encryption
{
public:
    Decryptor(const String & iv_, const EncryptionKey & key_)
        : Encryption(iv_, key_) { }

    void Decrypt(const char * ciphertext, DB::BufferBase::Position buf, size_t size, size_t off)
    {
        if (!size)
            return;

        auto iv = InitVector(init_vector);
        iv.Set(Blocks(off));
        off = BlockOffset(off);

        size_t part_size = PartBlockSize(size, off);
        if (off)
        {
            DecryptPartialBlock(buf, ciphertext, part_size, iv, off);
            size -= part_size;
            if (part_size + off == block_size)
                iv.Inc();
        }

        if (size)
            DecryptNBytes(buf, ciphertext + part_size, size, iv);
    }

private:
    void DecryptPartialBlock(DB::BufferBase::Position & to, const char * partial_block, size_t size, const InitVector & iv, size_t off) const
    {
        if (size > block_size)
            throw DB::Exception("Expecter partial block, got block with size > block_size: size = " + std::to_string(size) + " and offset = " + std::to_string(off),
                                DB::ErrorCodes::DATA_ENCRYPTION_ERROR);

        String ciphertext(block_size, '\0');
        String plaintext(block_size, '\0');
        for (size_t i = 0; i < size; ++i)
            ciphertext[i + off] = partial_block[i];

        auto * plaintext_ref = plaintext.data();
        DecryptNBytes(plaintext_ref, ciphertext.data(), off + size, iv);

        for (size_t i = 0; i < size; ++i)
            *(to++) = plaintext[i + off];
    }

    void DecryptNBytes(DB::BufferBase::Position & to, const char * data, size_t bytes, const InitVector & iv) const
    {
        auto evp_ctx_ptr = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>(EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free);
        auto * evp_ctx = evp_ctx_ptr.get();

        if (EVP_DecryptInit_ex(evp_ctx, evp_cipher, nullptr, nullptr, nullptr) != 1)
            throw DB::Exception("Failed to initialize decryption context with cipher", DB::ErrorCodes::DATA_ENCRYPTION_ERROR);

        if (EVP_DecryptInit_ex(evp_ctx, nullptr, nullptr,
            reinterpret_cast<const unsigned char*>(key.GetRef()),
            reinterpret_cast<const unsigned char*>(iv.GetRef())) != 1)
            throw DB::Exception("Failed to set key and IV for decryption", DB::ErrorCodes::DATA_ENCRYPTION_ERROR);

        int output_len = 0;
        if (EVP_DecryptUpdate(evp_ctx,
            reinterpret_cast<unsigned char*>(to), &output_len,
            reinterpret_cast<const unsigned char*>(data), static_cast<int>(bytes)) != 1)
            throw DB::Exception("Failed to decrypt", DB::ErrorCodes::DATA_ENCRYPTION_ERROR);

        to += output_len;

        int final_output_len = 0;
        if (EVP_DecryptFinal_ex(evp_ctx,
            reinterpret_cast<unsigned char*>(to), &final_output_len) != 1)
            throw DB::Exception("Failed to fetch plaintext", DB::ErrorCodes::DATA_ENCRYPTION_ERROR);

        if (output_len < 0 || final_output_len < 0 || static_cast<size_t>(output_len) + static_cast<size_t>(final_output_len) != bytes)
            throw DB::Exception("Only part of the data was decrypted", DB::ErrorCodes::DATA_ENCRYPTION_ERROR);
    }
};

}


#endif
