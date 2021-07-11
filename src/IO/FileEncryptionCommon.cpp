#include <IO/FileEncryptionCommon.h>

#if USE_SSL
#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBuffer.h>
#include <IO/WriteHelpers.h>

#include <cassert>
#include <random>


namespace DB
{

namespace ErrorCodes
{
    extern const int DATA_ENCRYPTION_ERROR;
}

namespace FileEncryption
{

namespace
{
    constexpr const size_t kBlockSize = 16;

    size_t blockOffset(size_t pos) { return pos % kBlockSize; }
    size_t blocks(size_t pos) { return pos / kBlockSize; }

    size_t partBlockSize(size_t size, size_t off)
    {
        assert(off < kBlockSize);
        /// write the part as usual block
        if (off == 0)
            return 0;
        return off + size <= kBlockSize ? size : (kBlockSize - off) % kBlockSize;
    }

    size_t encryptBlocks(EVP_CIPHER_CTX * evp_ctx, const char * data, size_t size, WriteBuffer & out)
    {
        const uint8_t * in = reinterpret_cast<const uint8_t *>(data);
        size_t in_size = 0;
        size_t out_size = 0;

        while (in_size < size)
        {
            out.nextIfAtEnd();
            size_t part_size = std::min(size - in_size, out.available());
            uint8_t * ciphertext = reinterpret_cast<uint8_t *>(out.position());
            int ciphertext_size = 0;
            if (!EVP_EncryptUpdate(evp_ctx, ciphertext, &ciphertext_size, &in[in_size], part_size))
                throw Exception("Failed to encrypt", ErrorCodes::DATA_ENCRYPTION_ERROR);

            in_size += part_size;
            if (ciphertext_size)
            {
                out.position() += ciphertext_size;
                out_size += ciphertext_size;
            }
        }

        return out_size;
    }

    size_t encryptBlockWithPadding(EVP_CIPHER_CTX * evp_ctx, const char * data, size_t size, size_t pad_left, WriteBuffer & out)
    {
        assert((size <= kBlockSize) && (size + pad_left <= kBlockSize));
        uint8_t padded_data[kBlockSize] = {};
        memcpy(&padded_data[pad_left], data, size);
        size_t padded_data_size = pad_left + size;

        uint8_t ciphertext[kBlockSize];
        int ciphertext_size = 0;
        if (!EVP_EncryptUpdate(evp_ctx, ciphertext, &ciphertext_size, padded_data, padded_data_size))
            throw Exception("Failed to encrypt", ErrorCodes::DATA_ENCRYPTION_ERROR);

        if (!ciphertext_size)
            return 0;

        if (static_cast<size_t>(ciphertext_size) < pad_left)
            throw Exception(ErrorCodes::DATA_ENCRYPTION_ERROR, "Unexpected size of encrypted data: {} < {}", ciphertext_size, pad_left);

        uint8_t * ciphertext_begin = &ciphertext[pad_left];
        ciphertext_size -= pad_left;
        out.write(reinterpret_cast<const char *>(ciphertext_begin), ciphertext_size);
        return ciphertext_size;
    }

    size_t encryptFinal(EVP_CIPHER_CTX * evp_ctx, WriteBuffer & out)
    {
        uint8_t ciphertext[kBlockSize];
        int ciphertext_size = 0;
        if (!EVP_EncryptFinal_ex(evp_ctx,
                                 ciphertext, &ciphertext_size))
            throw Exception("Failed to finalize encrypting", ErrorCodes::DATA_ENCRYPTION_ERROR);
        if (ciphertext_size)
            out.write(reinterpret_cast<const char *>(ciphertext), ciphertext_size);
        return ciphertext_size;
    }

    size_t decryptBlocks(EVP_CIPHER_CTX * evp_ctx, const char * data, size_t size, char * out)
    {
        const uint8_t * in = reinterpret_cast<const uint8_t *>(data);
        uint8_t * plaintext = reinterpret_cast<uint8_t *>(out);
        int plaintext_size = 0;
        if (!EVP_DecryptUpdate(evp_ctx, plaintext, &plaintext_size, in, size))
            throw Exception("Failed to decrypt", ErrorCodes::DATA_ENCRYPTION_ERROR);
        return plaintext_size;
    }

    size_t decryptBlockWithPadding(EVP_CIPHER_CTX * evp_ctx, const char * data, size_t size, size_t pad_left, char * out)
    {
        assert((size <= kBlockSize) && (size + pad_left <= kBlockSize));
        uint8_t padded_data[kBlockSize] = {};
        memcpy(&padded_data[pad_left], data, size);
        size_t padded_data_size = pad_left + size;

        uint8_t plaintext[kBlockSize];
        int plaintext_size = 0;
        if (!EVP_DecryptUpdate(evp_ctx, plaintext, &plaintext_size, padded_data, padded_data_size))
            throw Exception("Failed to decrypt", ErrorCodes::DATA_ENCRYPTION_ERROR);

        if (!plaintext_size)
            return 0;

        if (static_cast<size_t>(plaintext_size) < pad_left)
            throw Exception(ErrorCodes::DATA_ENCRYPTION_ERROR, "Unexpected size of decrypted data: {} < {}", plaintext_size, pad_left);

        const uint8_t * plaintext_begin = &plaintext[pad_left];
        plaintext_size -= pad_left;
        memcpy(out, plaintext_begin, plaintext_size);
        return plaintext_size;
    }

    size_t decryptFinal(EVP_CIPHER_CTX * evp_ctx, char * out)
    {
        uint8_t plaintext[kBlockSize];
        int plaintext_size = 0;
        if (!EVP_DecryptFinal_ex(evp_ctx, plaintext, &plaintext_size))
            throw Exception("Failed to finalize decrypting", ErrorCodes::DATA_ENCRYPTION_ERROR);
        if (plaintext_size)
            memcpy(out, plaintext, plaintext_size);
        return plaintext_size;
    }
}

String InitVector::toString() const
{
    static_assert(sizeof(counter) == InitVector::kSize);
    WriteBufferFromOwnString out;
    writeBinaryBigEndian(counter, out);
    return std::move(out.str());
}

InitVector InitVector::fromString(const String & str)
{
    if (str.length() != InitVector::kSize)
        throw Exception(ErrorCodes::DATA_ENCRYPTION_ERROR, "Expected iv with size {}, got iv with size {}", InitVector::kSize, str.length());
    ReadBufferFromMemory in{str.data(), str.length()};
    UInt128 counter;
    readBinaryBigEndian(counter, in);
    return InitVector{counter};
}

void InitVector::read(ReadBuffer & in)
{
    readBinaryBigEndian(counter, in);
}

void InitVector::write(WriteBuffer & out) const
{
    writeBinaryBigEndian(counter, out);
}

InitVector InitVector::random()
{
    std::random_device rd;
    std::mt19937 gen{rd()};
    std::uniform_int_distribution<UInt128::base_type> dis;
    UInt128 counter;
    for (size_t i = 0; i != std::size(counter.items); ++i)
        counter.items[i] = dis(gen);
    return InitVector{counter};
}


Encryptor::Encryptor(const String & key_, const InitVector & iv_)
    : key(key_)
    , init_vector(iv_)
{
    if (key_.length() == 16)
        evp_cipher = EVP_aes_128_ctr();
    else if (key_.length() == 24)
        evp_cipher = EVP_aes_192_ctr();
    else if (key_.length() == 32)
        evp_cipher = EVP_aes_256_ctr();
    else
        throw Exception(ErrorCodes::DATA_ENCRYPTION_ERROR, "Key length {} is not supported, supported only keys of length 128, 192, or 256 bits", key_.length());

    size_t cipher_key_length = static_cast<size_t>(EVP_CIPHER_key_length(evp_cipher));
    if (cipher_key_length != key_.length())
        throw Exception(ErrorCodes::DATA_ENCRYPTION_ERROR, "Got unexpected key length from cipher: {} != {}", cipher_key_length, key_.length());

    size_t cipher_iv_length = static_cast<size_t>(EVP_CIPHER_iv_length(evp_cipher));
    if (cipher_iv_length != InitVector::kSize)
        throw Exception(ErrorCodes::DATA_ENCRYPTION_ERROR, "Got unexpected init vector's length from cipher: {} != {}", cipher_iv_length, InitVector::kSize);
}

void Encryptor::encrypt(const char * data, size_t size, WriteBuffer & out)
{
    if (!size)
        return;

    auto current_iv = (init_vector + blocks(offset)).toString();

    auto evp_ctx_ptr = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>(EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free);
    auto * evp_ctx = evp_ctx_ptr.get();

    if (!EVP_EncryptInit_ex(evp_ctx, evp_cipher, nullptr, nullptr, nullptr))
        throw Exception("Failed to initialize encryption context with cipher", ErrorCodes::DATA_ENCRYPTION_ERROR);

    if (!EVP_EncryptInit_ex(evp_ctx, nullptr, nullptr,
                            reinterpret_cast<const uint8_t*>(key.c_str()), reinterpret_cast<const uint8_t*>(current_iv.c_str())))
        throw Exception("Failed to set key and IV for encryption", ErrorCodes::DATA_ENCRYPTION_ERROR);

    size_t in_size = 0;
    size_t out_size = 0;

    auto off = blockOffset(offset);
    if (off)
    {
        size_t in_part_size = partBlockSize(size, off);
        size_t out_part_size = encryptBlockWithPadding(evp_ctx, &data[in_size], in_part_size, off, out);
        in_size += in_part_size;
        out_size += out_part_size;
    }

    if (in_size < size)
    {
        size_t in_part_size = size - in_size;
        size_t out_part_size = encryptBlocks(evp_ctx, &data[in_size], in_part_size, out);
        in_size += in_part_size;
        out_size += out_part_size;
    }

    out_size += encryptFinal(evp_ctx, out);

    if (out_size != in_size)
        throw Exception("Only part of the data was encrypted", ErrorCodes::DATA_ENCRYPTION_ERROR);
    offset += in_size;
}

void Encryptor::decrypt(const char * data, size_t size, char * out)
{
    if (!size)
        return;

    auto current_iv = (init_vector + blocks(offset)).toString();

    auto evp_ctx_ptr = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>(EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free);
    auto * evp_ctx = evp_ctx_ptr.get();

    if (!EVP_DecryptInit_ex(evp_ctx, evp_cipher, nullptr, nullptr, nullptr))
        throw Exception("Failed to initialize decryption context with cipher", ErrorCodes::DATA_ENCRYPTION_ERROR);

    if (!EVP_DecryptInit_ex(evp_ctx, nullptr, nullptr,
                            reinterpret_cast<const uint8_t*>(key.c_str()), reinterpret_cast<const uint8_t*>(current_iv.c_str())))
        throw Exception("Failed to set key and IV for decryption", ErrorCodes::DATA_ENCRYPTION_ERROR);

    size_t in_size = 0;
    size_t out_size = 0;

    auto off = blockOffset(offset);
    if (off)
    {
        size_t in_part_size = partBlockSize(size, off);
        size_t out_part_size = decryptBlockWithPadding(evp_ctx, &data[in_size], in_part_size, off, &out[out_size]);
        in_size += in_part_size;
        out_size += out_part_size;
    }

    if (in_size < size)
    {
        size_t in_part_size = size - in_size;
        size_t out_part_size = decryptBlocks(evp_ctx, &data[in_size], in_part_size, &out[out_size]);
        in_size += in_part_size;
        out_size += out_part_size;
    }

    out_size += decryptFinal(evp_ctx, &out[out_size]);

    if (out_size != in_size)
        throw Exception("Only part of the data was decrypted", ErrorCodes::DATA_ENCRYPTION_ERROR);
    offset += in_size;
}

bool isKeyLengthSupported(size_t key_length)
{
    return (key_length == 16) || (key_length == 24) || (key_length == 32);
}

}
}

#endif
