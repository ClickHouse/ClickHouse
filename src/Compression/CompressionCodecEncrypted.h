#pragma once

#include <string_view>
#include <unordered_map>
#include <base/types.h>
#include <Compression/ICompressionCodec.h>
#include <Poco/Util/LayeredConfiguration.h>
#include <Common/MultiVersion.h>

namespace DB
{

/// Now we have two algorithms.
enum EncryptionMethod
{
    AES_128_GCM_SIV,
    AES_256_GCM_SIV,
    MAX_ENCRYPTION_METHOD
};

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
    explicit CompressionCodecEncrypted(EncryptionMethod Method);

    /**
        * This is utility class. It holds information about encryption configuration.
        */
    class Configuration
    {
    public:
        /// Configuration should be singleton. Instance method
        static Configuration & instance();

        /// Try to load data from config.
        bool tryLoad(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

        /// Load data and throw exception if something went wrong.
        void load(const Poco::Util::AbstractConfiguration & config, const String & config_prefix);

        /// Get current key and nonce (they will be set in variables, which are pass in this function).
        /// All data sets at the same time to prevent situations,
        /// when config changes and key and nonce are read from different versions
        /// If nonce is empty, it will return 12 null bytes.
        void getCurrentKeyAndNonce(EncryptionMethod method, UInt64 & current_key_id, String & current_key, String & nonce) const;

        /// Same as getCurrentKeyAndNonce. It is used to get key. (need for correct decryption, that is why nonce is not necessary)
        String getKey(EncryptionMethod method, const UInt64 & key_id) const;
    private:
        /// struct Params consists of:
        /// 1) hash-table of keys and their ids
        /// 2) current key for encryption
        /// 3) nonce for encryption
        /// All this parameters have MAX_ENCRYPTION_METHOD count of versions,
        /// because all algorithms can be described in config and used for different tables.
        struct Params
        {
            std::unordered_map<UInt64, String> keys_storage[MAX_ENCRYPTION_METHOD];
            UInt64 current_key_id[MAX_ENCRYPTION_METHOD] = {0, 0};
            String nonce[MAX_ENCRYPTION_METHOD];
        };

        // used to read data from config and create Params
        static void loadImpl(const Poco::Util::AbstractConfiguration & config, const String & config_prefix, EncryptionMethod method, std::unique_ptr<Params>& new_params);

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

    /// Encrypt data with chosen method.
    /// Throws exception if encryption is impossible or size of encrypted text is incorrect
    UInt32 doCompressData(const char * source, UInt32 source_size, char * dest) const override;

    /// Decrypt data with chosen method
    /// Throws exception if decryption is impossible or size of decrypted text is incorrect
    void doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const override;
private:
    EncryptionMethod encryption_method;
};

}
