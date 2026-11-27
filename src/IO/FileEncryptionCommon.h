#pragma once

#include "config.h"

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
enum class Algorithm : uint8_t
{
    AES_128_CTR, /// Size of key is 16 bytes.
    AES_192_CTR, /// Size of key is 24 bytes.
    AES_256_CTR, /// Size of key is 32 bytes.
    MAX
};

String toString(Algorithm algorithm);
Algorithm parseAlgorithmFromString(const String & str);

/// Throws an exception if a specified key size doesn't correspond a specified encryption algorithm.
void checkKeySize(size_t key_size, Algorithm algorithm);


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
///
/// The format of that header is following:
/// +--------+------+--------------------------------------------------------------------------+
/// | offset | size | description                                                              |
/// +--------+------+--------------------------------------------------------------------------+
/// |      0 |    3 | 'E', 'N', 'C' (file's signature)                                         |
/// |      3 |    2 | version of this header (1..2)                                            |
/// |      5 |    2 | encryption algorithm (0..2, 0=AES_128_CTR, 1=AES_192_CTR, 2=AES_256_CTR) |
/// |      7 |   16 | fingerprint of encryption key (SipHash)                                  |
/// |     23 |   16 | initialization vector (randomly generated)                               |
/// |     39 |   25 | reserved for future use                                                  |
/// +--------+------+--------------------------------------------------------------------------+
///
struct Header
{
    /// Versions:
    /// 1 - Initial version
    /// 2 - The header of an encrypted file contains the fingerprint of a used encryption key instead of a pair {key_id, very_small_hash(key)}.
    ///     The header is always stored in little endian.
    static constexpr const UInt16 kCurrentVersion = 2;

    UInt16 version = kCurrentVersion;

    /// Encryption algorithm.
    Algorithm algorithm = Algorithm::AES_128_CTR;

    /// Fingerprint of a key.
    UInt128 key_fingerprint = 0;

    InitVector init_vector;

    /// The size of this header in bytes, including reserved bytes.
    static constexpr const size_t kSize = 64;

    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;
};

/// Calculates the fingerprint of a passed encryption key.
UInt128 calculateKeyFingerprint(const String & key);

/// Calculates kind of the fingerprint of a passed encryption key & key ID as it was implemented in version 1.
UInt128 calculateV1KeyFingerprint(const String & key, UInt64 key_id);

}
}

#endif
