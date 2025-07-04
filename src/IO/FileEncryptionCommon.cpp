#include <IO/FileEncryptionCommon.h>
#include "Common/JemallocNodumpSTLAllocator.h"

#if USE_SSL
#    include <base/MemorySanitizer.h>
#    include <IO/ReadBuffer.h>
#    include <IO/ReadHelpers.h>
#    include <IO/WriteBuffer.h>
#    include <IO/WriteHelpers.h>
#    include <Common/SipHash.h>
#    include <Common/OpenSSLHelpers.h>
#    include <Common/safe_cast.h>

#    include <cassert>
#    include <boost/algorithm/string/predicate.hpp>

#    include <openssl/err.h>
#    include <openssl/rand.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int DATA_ENCRYPTION_ERROR;
    extern const int OPENSSL_ERROR;
}

namespace FileEncryption
{

namespace
{
    const EVP_CIPHER * getCipher(Algorithm algorithm)
    {
        switch (algorithm)
        {
            case Algorithm::AES_128_CTR: return EVP_aes_128_ctr();
            case Algorithm::AES_192_CTR: return EVP_aes_192_ctr();
            case Algorithm::AES_256_CTR: return EVP_aes_256_ctr();
            case Algorithm::MAX: break;
        }
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Encryption algorithm {} is not supported, specify one of the following: aes_128_ctr, aes_192_ctr, aes_256_ctr",
            static_cast<int>(algorithm));
    }

    void checkKeySize(const EVP_CIPHER * evp_cipher, size_t key_size)
    {
        if (!key_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Encryption key must not be empty");
        size_t expected_key_size = static_cast<size_t>(EVP_CIPHER_key_length(evp_cipher));
        if (key_size != expected_key_size)
            throw Exception(
                            ErrorCodes::BAD_ARGUMENTS,
                            "Got an encryption key with unexpected size {}, the size should be {}",
                            key_size, expected_key_size);
    }

    void checkInitVectorSize(const EVP_CIPHER * evp_cipher)
    {
        size_t expected_iv_length = static_cast<size_t>(EVP_CIPHER_iv_length(evp_cipher));
        if (InitVector::kSize != expected_iv_length)
            throw Exception(
                ErrorCodes::DATA_ENCRYPTION_ERROR,
                "Got an initialization vector with unexpected size {}, the size should be {}",
                InitVector::kSize,
                expected_iv_length);
    }

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
            part_size = std::min<size_t>(part_size, INT_MAX);

            uint8_t * ciphertext = reinterpret_cast<uint8_t *>(out.position());
            int ciphertext_size = 0;
            if (EVP_EncryptUpdate(evp_ctx, ciphertext, &ciphertext_size, &in[in_size], static_cast<int>(part_size)) != 1)
                throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_EncryptUpdate failed: {}", getOpenSSLErrors());

            __msan_unpoison(ciphertext, ciphertext_size); /// OpenSSL uses assembly which evades msans analysis

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
        if (EVP_EncryptUpdate(evp_ctx, ciphertext, &ciphertext_size, padded_data, safe_cast<int>(padded_data_size)) != 1)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_EncryptUpdate failed: {}", getOpenSSLErrors());

        if (!ciphertext_size)
            return 0;

        if (static_cast<size_t>(ciphertext_size) < pad_left)
            throw Exception(ErrorCodes::DATA_ENCRYPTION_ERROR, "Unexpected size of encrypted data: {} < {}", ciphertext_size, pad_left);

        uint8_t * ciphertext_begin = &ciphertext[pad_left];
        ciphertext_size -= pad_left;
        __msan_unpoison(ciphertext_begin, ciphertext_size); /// OpenSSL uses assembly which evades msans analysis
        out.write(reinterpret_cast<const char *>(ciphertext_begin), ciphertext_size);
        return ciphertext_size;
    }

    size_t encryptFinal(EVP_CIPHER_CTX * evp_ctx, WriteBuffer & out)
    {
        uint8_t ciphertext[kBlockSize];
        int ciphertext_size = 0;
        if (EVP_EncryptFinal_ex(evp_ctx, ciphertext, &ciphertext_size) != 1)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_EncryptFinal_ex: {}", getOpenSSLErrors());
        __msan_unpoison(ciphertext, ciphertext_size); /// OpenSSL uses assembly which evades msans analysis
        if (ciphertext_size)
            out.write(reinterpret_cast<const char *>(ciphertext), ciphertext_size);
        return ciphertext_size;
    }

    size_t decryptBlocks(EVP_CIPHER_CTX * evp_ctx, const char * data, size_t size, char * out)
    {
        const uint8_t * in = reinterpret_cast<const uint8_t *>(data);
        uint8_t * plaintext = reinterpret_cast<uint8_t *>(out);
        int plaintext_size = 0;
        if (EVP_DecryptUpdate(evp_ctx, plaintext, &plaintext_size, in, safe_cast<int>(size)) != 1)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_DecryptUpdate: {}", getOpenSSLErrors());
        __msan_unpoison(plaintext, plaintext_size); /// OpenSSL uses assembly which evades msans analysis
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
        if (EVP_DecryptUpdate(evp_ctx, plaintext, &plaintext_size, padded_data, safe_cast<int>(padded_data_size)) != 1)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_DecryptUpdate: {}", getOpenSSLErrors());

        if (!plaintext_size)
            return 0;

        if (static_cast<size_t>(plaintext_size) < pad_left)
            throw Exception(ErrorCodes::DATA_ENCRYPTION_ERROR, "Unexpected size of decrypted data: {} < {}", plaintext_size, pad_left);

        const uint8_t * plaintext_begin = &plaintext[pad_left];
        plaintext_size -= pad_left;
        __msan_unpoison(plaintext_begin, plaintext_size); /// OpenSSL uses assembly which evades msans analysis
        memcpy(out, plaintext_begin, plaintext_size);
        return plaintext_size;
    }

    size_t decryptFinal(EVP_CIPHER_CTX * evp_ctx, char * out)
    {
        uint8_t plaintext[kBlockSize];
        int plaintext_size = 0;
        if (EVP_DecryptFinal_ex(evp_ctx, plaintext, &plaintext_size) != 1)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_DecryptFinal_ex: {}", getOpenSSLErrors());
        __msan_unpoison(plaintext, plaintext_size); /// OpenSSL uses assembly which evades msans analysis
        if (plaintext_size)
            memcpy(out, plaintext, plaintext_size);
        return plaintext_size;
    }

    constexpr const std::string_view kHeaderSignature = "ENC";

    UInt128 calculateV1KeyFingerprint(UInt8 small_key_hash, UInt64 key_id)
    {
        /// In the version 1 we stored {key_id, very_small_hash(key)} instead of a fingerprint.
        return static_cast<UInt128>(key_id) | (static_cast<UInt128>(small_key_hash) << 64);
    }
}

String toString(Algorithm algorithm)
{
    switch (algorithm)
    {
        case Algorithm::AES_128_CTR: return "aes_128_ctr";
        case Algorithm::AES_192_CTR: return "aes_192_ctr";
        case Algorithm::AES_256_CTR: return "aes_256_ctr";
        case Algorithm::MAX: break;
    }
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Encryption algorithm {} is not supported, specify one of the following: aes_128_ctr, aes_192_ctr, aes_256_ctr",
        static_cast<int>(algorithm));
}

Algorithm parseAlgorithmFromString(const String & str)
{
    if (boost::iequals(str, "aes_128_ctr"))
        return Algorithm::AES_128_CTR;
    if (boost::iequals(str, "aes_192_ctr"))
        return Algorithm::AES_192_CTR;
    if (boost::iequals(str, "aes_256_ctr"))
        return Algorithm::AES_256_CTR;
    throw Exception(
        ErrorCodes::BAD_ARGUMENTS,
        "Encryption algorithm '{}' is not supported, specify one of the following: aes_128_ctr, aes_192_ctr, aes_256_ctr",
        str);
}

void checkKeySize(size_t key_size, Algorithm algorithm) { checkKeySize(getCipher(algorithm), key_size); }


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
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Expected iv with size {}, got iv with size {}", InitVector::kSize, str.length());
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
    UInt128 counter;
    auto * buf = reinterpret_cast<unsigned char *>(counter.items);
    if (RAND_bytes(buf, sizeof(counter.items)) != 1)
        throw Exception(DB::ErrorCodes::OPENSSL_ERROR, "RAND_bytes failed: {}", getOpenSSLErrors());
    return InitVector{counter};
}


Encryptor::Encryptor(Algorithm algorithm_, const NoDumpString & key_, const InitVector & iv_)
    : key(key_)
    , init_vector(iv_)
    , evp_cipher(getCipher(algorithm_))
{
    checkKeySize(evp_cipher, key.size());
    checkInitVectorSize(evp_cipher);
}

void Encryptor::encrypt(const char * data, size_t size, WriteBuffer & out)
{
    if (!size)
        return;

    auto current_iv = (init_vector + blocks(offset)).toString();

    auto evp_ctx_ptr = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>(EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free);
    auto * evp_ctx = evp_ctx_ptr.get();

    if (EVP_EncryptInit_ex(evp_ctx, evp_cipher, nullptr, nullptr, nullptr) != 1)
        throw Exception(DB::ErrorCodes::OPENSSL_ERROR, "EVP_EncryptInit_ex failed: {}", getOpenSSLErrors());

    if (EVP_EncryptInit_ex(evp_ctx, nullptr, nullptr,
                            reinterpret_cast<const uint8_t*>(key.c_str()), reinterpret_cast<const uint8_t*>(current_iv.c_str())) != 1)
        throw Exception(DB::ErrorCodes::OPENSSL_ERROR, "EVP_EncryptInit_ex failed: {}", getOpenSSLErrors());

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
        throw Exception(ErrorCodes::DATA_ENCRYPTION_ERROR, "Only part of the data was encrypted: {} out of {} bytes", out_size, in_size);
    offset += in_size;
}

void Encryptor::decrypt(const char * data, size_t size, char * out)
{
    if (!size)
        return;

    auto current_iv = (init_vector + blocks(offset)).toString();

    auto evp_ctx_ptr = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>(EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free);
    auto * evp_ctx = evp_ctx_ptr.get();

    if (EVP_DecryptInit_ex(evp_ctx, evp_cipher, nullptr, nullptr, nullptr) != 1)
        throw Exception(DB::ErrorCodes::OPENSSL_ERROR, "EVP_DecryptInit_ex failed: {}", getOpenSSLErrors());

    if (EVP_DecryptInit_ex(evp_ctx, nullptr, nullptr,
                            reinterpret_cast<const uint8_t*>(key.c_str()), reinterpret_cast<const uint8_t*>(current_iv.c_str())) != 1)
        throw Exception(DB::ErrorCodes::OPENSSL_ERROR, "EVP_DecryptInit_ex failed: {}", getOpenSSLErrors());

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
        throw Exception(ErrorCodes::DATA_ENCRYPTION_ERROR, "Only part of the data was decrypted: {} out of {} bytes", out_size, in_size);
    offset += in_size;
}


void Header::read(ReadBuffer & in)
{
    char signature[kHeaderSignature.length()];
    in.readStrict(signature, kHeaderSignature.length());
    if (memcmp(signature, kHeaderSignature.data(), kHeaderSignature.length()) != 0)
        throw Exception(ErrorCodes::DATA_ENCRYPTION_ERROR, "Wrong signature, this is not an encrypted file");

    /// The endianness of how the header is written.
    /// Starting from version 2 the header is always in little endian.
    std::endian endian = std::endian::little;

    readBinaryLittleEndian(version, in);

    if (version == 0x0100ULL)
    {
        /// Version 1 could write the header of an encrypted file in either little-endian or big-endian.
        /// So now if we read the version as little-endian and it's 256 that means two things: the version is actually 1 and the whole header is in big endian.
        endian = std::endian::big;
        version = 1;
    }

    if (version < 1 || version > kCurrentVersion)
        throw Exception(ErrorCodes::DATA_ENCRYPTION_ERROR, "Version {} of the header is not supported", version);

    UInt16 algorithm_u16;
    readPODBinary(algorithm_u16, in);
    if (std::endian::native != endian)
        algorithm_u16 = std::byteswap(algorithm_u16);
    if (algorithm_u16 >= static_cast<UInt16>(Algorithm::MAX))
        throw Exception(ErrorCodes::DATA_ENCRYPTION_ERROR, "Algorithm {} is not supported", algorithm_u16);
    algorithm = static_cast<Algorithm>(algorithm_u16);

    size_t bytes_to_skip = kSize - kHeaderSignature.length() - sizeof(version) - sizeof(algorithm_u16) - InitVector::kSize;

    if (version < 2)
    {
        UInt64 key_id;
        UInt8 small_key_hash;
        readPODBinary(key_id, in);
        readPODBinary(small_key_hash, in);
        bytes_to_skip -= sizeof(key_id) + sizeof(small_key_hash);
        if (std::endian::native != endian)
            key_id = std::byteswap(key_id);
        key_fingerprint = calculateV1KeyFingerprint(small_key_hash, key_id);
    }
    else
    {
        readBinaryLittleEndian(key_fingerprint, in);
        bytes_to_skip -= sizeof(key_fingerprint);
    }

    init_vector.read(in);

    chassert(bytes_to_skip < kSize);
    in.ignore(bytes_to_skip);
}

void Header::write(WriteBuffer & out) const
{
    writeString(kHeaderSignature, out);

    writeBinaryLittleEndian(version, out);

    UInt16 algorithm_u16 = static_cast<UInt16>(algorithm);
    writeBinaryLittleEndian(algorithm_u16, out);

    writeBinaryLittleEndian(key_fingerprint, out);

    init_vector.write(out);

    constexpr size_t reserved_size = kSize - kHeaderSignature.length() - sizeof(version) - sizeof(algorithm_u16) - sizeof(key_fingerprint) - InitVector::kSize;
    static_assert(reserved_size < kSize);
    char zero_bytes[reserved_size] = {};
    out.write(zero_bytes, reserved_size);
}

UInt128 calculateKeyFingerprint(const NoDumpString & key)
{
    const UInt64 seed0 = 0x4368456E63727970ULL; // ChEncryp
    const UInt64 seed1 = 0x7465644469736B46ULL; // tedDiskF
    return sipHash128Keyed(seed0, seed1, key.data(), key.size());
}

UInt128 calculateV1KeyFingerprint(const NoDumpString & key, UInt64 key_id)
{
    /// In the version 1 we stored {key_id, very_small_hash(key)} instead of a fingerprint.
    UInt8 small_key_hash = sipHash64(key.data(), key.size()) & 0x0F;
    return calculateV1KeyFingerprint(small_key_hash, key_id);
}

}
}

#endif
