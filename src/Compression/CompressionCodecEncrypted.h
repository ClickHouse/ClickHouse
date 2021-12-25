#pragma once

// This depends on BoringSSL-specific API, notably <openssl/aead.h>.
#include <Common/config.h>
#if USE_SSL && USE_INTERNAL_SSL_LIBRARY && !defined(ARCADIA_BUILD)

#include <Compression/ICompressionCodec.h>
#include <boost/noncopyable.hpp>
#include <openssl/aead.h> // Y_IGNORE
#include <optional>

namespace DB
{
    /** This codec encrypts and decrypts blocks with AES-128 in
      * GCM-SIV mode (RFC-8452), which is the only cipher currently
      * supported. Although it is implemented as a compression codec
      * it doesn't actually compress data. In fact encrypted data will
      * no longer be compressible in any meaningful way. This means if
      * you want to apply both compression and encryption to your
      * columns, you need to put this codec at the end of the chain
      * like "column Int32 Codec(Delta, LZ4,
      * Encrypted('AES-128-GCM-SIV'))".
      *
      * The key is obtained by executing a command specified in the
      * configuration file at startup, and if it doesn't specify a
      * command the codec refuses to process any data. The command is
      * expected to write a Base64-encoded key of any length, and we
      * apply HKDF-SHA-256 to derive a 128-bit key-generation key
      * (only the first half of the result is used). We then encrypt
      * blocks in AES-128-GCM-SIV with a universally fixed nonce (12
      * repeated NUL characters).
      *
      * This construct has a weakness due to the nonce being fixed at
      * all times: when the same data block is encrypted twice, the
      * resulting ciphertext will be exactly the same. We have to live
      * with this weakness because ciphertext must be deterministic,
      * as otherwise our engines like ReplicatedMergeTree cannot
      * deduplicate data blocks.
      */
    class CompressionCodecEncrypted : public ICompressionCodec
    {
    public:
        /** If a master key is available, the server is supposed to
          * invoke this static method at the startup. The codec will
          * refuse to compress or decompress any data until that. The
          * key can be an arbitrary octet string, but it is
          * recommended that the key is at least 16 octets long.
          *
          * Note that the master key is currently not guarded by a
          * mutex. This method should be invoked no more than once.
          */
        static void setMasterKey(const std::string_view & master_key);

        explicit CompressionCodecEncrypted(const std::string_view & cipher);

        uint8_t getMethodByte() const override;
        void updateHash(SipHash & hash) const override;

        bool isCompression() const override
        {
            return false;
        }

        bool isGenericCompression() const override
        {
            return false;
        }

        bool isPostProcessing() const override
        {
            return true;
        }

    protected:
        UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

        UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
        void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;

    private:
        static std::string lastErrorString();
        static std::string deriveKey(const std::string_view & master_key);
        static void encrypt(const std::string_view & plaintext, char * ciphertext_and_tag);
        static void decrypt(const std::string_view & ciphertext_and_tag, char * plaintext);

        /** A private class that holds keys derived from the master
          * key.
          */
        struct KeyHolder : private boost::noncopyable
        {
            explicit KeyHolder(const std::string_view & master_key);
            ~KeyHolder();

            std::string keygen_key;
            EVP_AEAD_CTX ctx;
        };

        static const KeyHolder & getKeys();

        static inline std::optional<KeyHolder> keys;
    };

    inline CompressionCodecPtr getCompressionCodecEncrypted(const std::string_view & master_key)
    {
        return std::make_shared<CompressionCodecEncrypted>(master_key);
    }
}

#endif /* USE_SSL && USE_INTERNAL_SSL_LIBRARY */
