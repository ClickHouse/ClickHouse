#pragma once

// This depends on BoringSSL-specific API, notably <openssl/aead.h>.
#include <string_view>
#include <unordered_map>
#include "common/types.h"
#include <Common/config.h>
#if USE_SSL && USE_INTERNAL_SSL_LIBRARY && !defined(ARCADIA_BUILD)

#include <Compression/ICompressionCodec.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <boost/noncopyable.hpp>
#include <openssl/aead.h> // Y_IGNORE
#include <optional>
#include <Common/MultiVersion.h>

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
    * AES_128_GCM_SIV)".
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
    /** If a key is available, the server is supposed to
        * invoke this static method at the startup. The codec will
        * refuse to compress or decompress any data until that. The
        * key can be an arbitrary octet string, but it is
        * recommended that the key is at least 16 octets long.
        *
        * Note that the key is currently not guarded by a
        * mutex. This method should be invoked no more than once.
        */
    explicit CompressionCodecEncrypted(CompressionMethodByte Algorithm);

    /**
        * This is utility class. It holds information about encryption configuration.
        */
    class Configuration
    {
    public:
        static Configuration & instance();

        void load(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);
        String getKey(UInt64 id) const;
        String getCurrentKey() const;
        UInt64 getCurrentKeyID() const;
        String getNonce() const;
        CompressionMethodByte getAlgorithmByte() const;

    private:
        struct Params
        {
            std::unordered_map<UInt64, String> keys_storage;
            UInt64 current_key_id;
            String nonce;
            CompressionMethodByte algorithm; 
        };

        // used to read data from config and create Params
        static Params loadImpl(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

        MultiVersion<Params> params;
    };

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

    bool isEncryption() const override
    {
        return true;
    }
protected:
    UInt32 getMaxCompressedDataSize(UInt32 uncompressed_size) const override;

    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;
};
}

#endif /* USE_SSL && USE_INTERNAL_SSL_LIBRARY */
