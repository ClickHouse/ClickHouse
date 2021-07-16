#include <IO/FileEncryptionCommon.h>

#if USE_SSL
#include <IO/ReadHelpers.h>
#include <IO/ReadBuffer.h>
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
    String toBigEndianString(UInt128 value)
    {
        WriteBufferFromOwnString out;
        writeBinaryBigEndian(value, out);
        return std::move(out.str());
    }

    UInt128 fromBigEndianString(const String & str)
    {
        ReadBufferFromMemory in{str.data(), str.length()};
        UInt128 result;
        readBinaryBigEndian(result, in);
        return result;
    }
}

InitVector::InitVector(const String & iv_) : iv(fromBigEndianString(iv_)) {}

const String & InitVector::str() const
{
    local = toBigEndianString(iv + counter);
    return local;
}

Encryption::Encryption(const String & iv_, const EncryptionKey & key_, size_t offset_)
    : evp_cipher(defaultCipher())
    , init_vector(iv_)
    , key(key_)
    , block_size(cipherIVLength(evp_cipher))
{
    if (iv_.size() != cipherIVLength(evp_cipher))
        throw DB::Exception("Expected iv with size " + std::to_string(cipherIVLength(evp_cipher)) + ", got iv with size " + std::to_string(iv_.size()),
                            DB::ErrorCodes::DATA_ENCRYPTION_ERROR);
    if (key_.size() != cipherKeyLength(evp_cipher))
        throw DB::Exception("Expected key with size " + std::to_string(cipherKeyLength(evp_cipher)) + ", got iv with size " + std::to_string(key_.size()),
                            DB::ErrorCodes::DATA_ENCRYPTION_ERROR);

    offset = offset_;
}

size_t Encryption::partBlockSize(size_t size, size_t off) const
{
    assert(off < block_size);
    /// write the part as usual block
    if (off == 0)
        return 0;
    return off + size <= block_size ? size : (block_size - off) % block_size;
}

void Encryptor::encrypt(const char * plaintext, WriteBuffer & buf, size_t size)
{
    if (!size)
        return;

    auto iv = InitVector(init_vector);
    auto off = blockOffset(offset);
    iv.set(blocks(offset));

    size_t part_size = partBlockSize(size, off);
    if (off)
    {
        buf.write(encryptPartialBlock(plaintext, part_size, iv, off).data(), part_size);
        offset += part_size;
        size -= part_size;
        iv.inc();
    }

    if (size)
    {
        buf.write(encryptNBytes(plaintext + part_size, size, iv).data(), size);
        offset += size;
    }
}

String Encryptor::encryptPartialBlock(const char * partial_block, size_t size, const InitVector & iv, size_t off) const
{
    if (size > block_size)
        throw Exception("Expected partial block, got block with size > block_size: size = " + std::to_string(size) + " and offset = " + std::to_string(off),
                            ErrorCodes::DATA_ENCRYPTION_ERROR);

    String plaintext(block_size, '\0');
    for (size_t i = 0; i < size; ++i)
        plaintext[i + off] = partial_block[i];

    return String(encryptNBytes(plaintext.data(), block_size, iv), off, size);
}

String Encryptor::encryptNBytes(const char * data, size_t bytes, const InitVector & iv) const
{
    String ciphertext(bytes, '\0');
    auto * ciphertext_ref = ciphertext.data();

    auto evp_ctx_ptr = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>(EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free);
    auto * evp_ctx = evp_ctx_ptr.get();

    if (EVP_EncryptInit_ex(evp_ctx, evp_cipher, nullptr, nullptr, nullptr) != 1)
        throw Exception("Failed to initialize encryption context with cipher", ErrorCodes::DATA_ENCRYPTION_ERROR);

    if (EVP_EncryptInit_ex(evp_ctx, nullptr, nullptr,
        reinterpret_cast<const unsigned char*>(key.str().data()),
        reinterpret_cast<const unsigned char*>(iv.str().data())) != 1)
        throw Exception("Failed to set key and IV for encryption", ErrorCodes::DATA_ENCRYPTION_ERROR);

    int output_len = 0;
    if (EVP_EncryptUpdate(evp_ctx,
        reinterpret_cast<unsigned char*>(ciphertext_ref), &output_len,
        reinterpret_cast<const unsigned char*>(data), static_cast<int>(bytes)) != 1)
        throw Exception("Failed to encrypt", ErrorCodes::DATA_ENCRYPTION_ERROR);

    ciphertext_ref += output_len;

    int final_output_len = 0;
    if (EVP_EncryptFinal_ex(evp_ctx,
        reinterpret_cast<unsigned char*>(ciphertext_ref), &final_output_len) != 1)
        throw Exception("Failed to fetch ciphertext", ErrorCodes::DATA_ENCRYPTION_ERROR);

    if (output_len < 0 || final_output_len < 0 || static_cast<size_t>(output_len) + static_cast<size_t>(final_output_len) != bytes)
        throw Exception("Only part of the data was encrypted", ErrorCodes::DATA_ENCRYPTION_ERROR);

    return ciphertext;
}

void Decryptor::decrypt(const char * ciphertext, BufferBase::Position buf, size_t size, size_t off)
{
    if (!size)
        return;

    auto iv = InitVector(init_vector);
    iv.set(blocks(off));
    off = blockOffset(off);

    size_t part_size = partBlockSize(size, off);
    if (off)
    {
        decryptPartialBlock(buf, ciphertext, part_size, iv, off);
        size -= part_size;
        if (part_size + off == block_size)
            iv.inc();
    }

    if (size)
        decryptNBytes(buf, ciphertext + part_size, size, iv);
}

void Decryptor::decryptPartialBlock(BufferBase::Position & to, const char * partial_block, size_t size, const InitVector & iv, size_t off) const
{
    if (size > block_size)
        throw Exception("Expecter partial block, got block with size > block_size: size = " + std::to_string(size) + " and offset = " + std::to_string(off),
                            ErrorCodes::DATA_ENCRYPTION_ERROR);

    String ciphertext(block_size, '\0');
    String plaintext(block_size, '\0');
    for (size_t i = 0; i < size; ++i)
        ciphertext[i + off] = partial_block[i];

    auto * plaintext_ref = plaintext.data();
    decryptNBytes(plaintext_ref, ciphertext.data(), off + size, iv);

    for (size_t i = 0; i < size; ++i)
        *(to++) = plaintext[i + off];
}

void Decryptor::decryptNBytes(BufferBase::Position & to, const char * data, size_t bytes, const InitVector & iv) const
{
    auto evp_ctx_ptr = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>(EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free);
    auto * evp_ctx = evp_ctx_ptr.get();

    if (EVP_DecryptInit_ex(evp_ctx, evp_cipher, nullptr, nullptr, nullptr) != 1)
        throw Exception("Failed to initialize decryption context with cipher", ErrorCodes::DATA_ENCRYPTION_ERROR);

    if (EVP_DecryptInit_ex(evp_ctx, nullptr, nullptr,
        reinterpret_cast<const unsigned char*>(key.str().data()),
        reinterpret_cast<const unsigned char*>(iv.str().data())) != 1)
        throw Exception("Failed to set key and IV for decryption", ErrorCodes::DATA_ENCRYPTION_ERROR);

    int output_len = 0;
    if (EVP_DecryptUpdate(evp_ctx,
        reinterpret_cast<unsigned char*>(to), &output_len,
        reinterpret_cast<const unsigned char*>(data), static_cast<int>(bytes)) != 1)
        throw Exception("Failed to decrypt", ErrorCodes::DATA_ENCRYPTION_ERROR);

    to += output_len;

    int final_output_len = 0;
    if (EVP_DecryptFinal_ex(evp_ctx,
        reinterpret_cast<unsigned char*>(to), &final_output_len) != 1)
        throw Exception("Failed to fetch plaintext", ErrorCodes::DATA_ENCRYPTION_ERROR);

    if (output_len < 0 || final_output_len < 0 || static_cast<size_t>(output_len) + static_cast<size_t>(final_output_len) != bytes)
        throw Exception("Only part of the data was decrypted", ErrorCodes::DATA_ENCRYPTION_ERROR);
}

String readIV(size_t size, ReadBuffer & in)
{
    String iv(size, 0);
    in.readStrict(reinterpret_cast<char *>(iv.data()), size);
    return iv;
}

String randomString(size_t size)
{
    String iv(size, 0);

    std::random_device rd;
    std::mt19937 gen{rd()};
    std::uniform_int_distribution<size_t> dis;

    char * ptr = iv.data();
    while (size)
    {
        auto value = dis(gen);
        size_t n = std::min(size, sizeof(value));
        memcpy(ptr, &value, n);
        ptr += n;
        size -= n;
    }

    return iv;
}

void writeIV(const String & iv, WriteBuffer & out)
{
    out.write(iv.data(), iv.length());
}

size_t cipherKeyLength(const EVP_CIPHER * evp_cipher)
{
    return static_cast<size_t>(EVP_CIPHER_key_length(evp_cipher));
}

size_t cipherIVLength(const EVP_CIPHER * evp_cipher)
{
    return static_cast<size_t>(EVP_CIPHER_iv_length(evp_cipher));
}

const EVP_CIPHER * defaultCipher()
{
    return EVP_aes_128_ctr();
}

}
}

#endif
