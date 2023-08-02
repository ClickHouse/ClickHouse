#pragma once

#include <Common/config.h>

#if USE_SSL
#include <Core/Types.h>
#include <openssl/evp.h>

namespace DB
{
class ReadBuffer;
class WriteBuffer;

namespace FileEncryption
{

/// Encryption algorithm.
/// We chose to use CTR cipther algorithms because they have the following features which are important for us:
/// - No right padding, so we can append encrypted files without deciphering;
/// - One byte is always ciphered as one byte, so we get random access to encrypted files easily.
enum class Algorithm
{
    AES_128_CTR, /// Size of key is 16 bytes.
    AES_192_CTR, /// Size of key is 24 bytes.
    AES_256_CTR, /// Size of key is 32 bytes.
};

String toString(Algorithm algorithm);
void parseFromString(Algorithm & algorithm, const String & str);

/// Throws an exception if a specified key size doesn't correspond a specified encryption algorithm.
void checkKeySize(Algorithm algorithm, size_t key_size);


/// Initialization vector. Its size is always 16 bytes.
class InitVector
{
public:
    static constexpr const size_t kSize = 16;

    InitVector() = default;
    explicit InitVector(const UInt128 & counter_) { set(counter_); }

    void set(const UInt128 & counter_) { counter = counter_; }
    UInt128 get() const { return counter; }

    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;

    /// Write 16 bytes of the counter to a string in big endian order.
    /// We need big endian because the used cipher algorithms treat an initialization vector as a counter in big endian.
    String toString() const;

    /// Converts a string of 16 bytes length in big endian order to a counter.
    static InitVector fromString(const String & str_);

    /// Adds a specified offset to the counter.
    InitVector & operator++() { ++counter; return *this; }
    InitVector operator++(int) { InitVector res = *this; ++counter; return res; } /// NOLINT
    InitVector & operator+=(size_t offset) { counter += offset; return *this; }
    InitVector operator+(size_t offset) const { InitVector res = *this; return res += offset; }

    /// Generates a random initialization vector.
    static InitVector random();

private:
    UInt128 counter = 0;
};


/// Encrypts or decrypts data.
class Encryptor
{
public:
    /// The `key` should have size 16 or 24 or 32 bytes depending on which `algorithm` is specified.
    Encryptor(Algorithm algorithm_, const String & key_, const InitVector & iv_);

    /// Sets the current position in the data stream from the very beginning of data.
    /// It affects how the data will be encrypted or decrypted because
    /// the initialization vector is increased by an index of the current block
    /// and the index of the current block is calculated from this offset.
    void setOffset(size_t offset_) { offset = offset_; }
    size_t getOffset() const { return offset; }

    /// Encrypts some data.
    /// Also the function moves `offset` by `size` (for successive encryptions).
    void encrypt(const char * data, size_t size, WriteBuffer & out);

    /// Decrypts some data.
    /// The used cipher algorithms generate the same number of bytes in output as they were in input,
    /// so the function always writes `size` bytes of the plaintext to `out`.
    /// Also the function moves `offset` by `size` (for successive decryptions).
    void decrypt(const char * data, size_t size, char * out);

private:
    const String key;
    const InitVector init_vector;
    const EVP_CIPHER * const evp_cipher;

    /// The current position in the data stream from the very beginning of data.
    size_t offset = 0;
};


/// File header which is stored at the beginning of encrypted files.
struct Header
{
    Algorithm algorithm = Algorithm::AES_128_CTR;

    /// Identifier of the key to encrypt or decrypt this file.
    UInt64 key_id = 0;

    /// Hash of the key to encrypt or decrypt this file.
    UInt8 key_hash = 0;

    InitVector init_vector;

    /// The size of this header in bytes, including reserved bytes.
    static constexpr const size_t kSize = 64;

    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;
};

/// Calculates the hash of a passed key.
/// 1 byte is enough because this hash is used only for the first check.
UInt8 calculateKeyHash(const String & key);

}
}

#endif
