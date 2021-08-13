
#include <string>
#include <Common/config.h>
#include "IO/VarInt.h"
#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif
#include <Compression/CompressionFactory.h>
#include <Poco/Util/AbstractConfiguration.h>
#if USE_SSL && USE_INTERNAL_SSL_LIBRARY

#include <Compression/CompressionCodecEncrypted.h>
#include <Parsers/ASTLiteral.h>
#include <cassert>
#include <openssl/digest.h> // Y_IGNORE
#include <openssl/err.h>
#include <openssl/hkdf.h> // Y_IGNORE
#include <string_view>
#include <boost/algorithm/hex.hpp>


namespace DB
{
    std::unordered_map<UInt64, String> CompressionCodecEncrypted::keys_storage;
    UInt64 CompressionCodecEncrypted::current_key_id;

    namespace ErrorCodes
    {
        extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
        extern const int NO_ELEMENTS_IN_CONFIG;
        extern const int OPENSSL_ERROR;
        extern const int BAD_ARGUMENTS;
    }

    void CompressionCodecEncrypted::setMasterKey(const std::string_view & master_key)
    {
        keys.emplace(master_key);
    }

    CompressionCodecEncrypted::KeyHolder::KeyHolder(const std::string_view & master_key)
    {
        // EVP_AEAD_CTX is not stateful so we can create an
        // instance now.
        EVP_AEAD_CTX_zero(&ctx);
        const int ok = EVP_AEAD_CTX_init(&ctx, EVP_aead_aes_128_gcm_siv(),
                                         reinterpret_cast<const uint8_t*>(master_key.data()), master_key.size(),
                                         16 /* tag size */, nullptr);
        if (!ok)
            throw Exception(lastErrorString(), ErrorCodes::OPENSSL_ERROR);
    }

    CompressionCodecEncrypted::KeyHolder::~KeyHolder()
    {
        EVP_AEAD_CTX_cleanup(&ctx);
    }

    const CompressionCodecEncrypted::KeyHolder & CompressionCodecEncrypted::getKeys()
    {
        if (keys)
            return *keys;
        else
            throw Exception("There is no configuration for encryption in the server config",
                            ErrorCodes::NO_ELEMENTS_IN_CONFIG);
    }

    CompressionCodecEncrypted::CompressionCodecEncrypted()
    {
        setCodecDescription("AES_128_GCM_SIV");
    }

    uint8_t CompressionCodecEncrypted::getMethodByte() const
    {
        return static_cast<uint8_t>(CompressionMethodByte::Encrypted);
    }

    void CompressionCodecEncrypted::updateHash(SipHash & hash) const
    {
        getCodecDesc()->updateTreeHash(hash);
    }

    UInt32 CompressionCodecEncrypted::getMaxCompressedDataSize(UInt32 uncompressed_size) const
    {
        // The GCM mode is a stream cipher. No paddings are
        // involved. There will be a tag at the end of ciphertext (16
        // octets).
        return uncompressed_size + 24;
    }

    UInt32 CompressionCodecEncrypted::doCompressData(const char * source, UInt32 source_size, char * dest) const
    {
        // Generate an IV out of the data block and the key-generation
        // key. It is completely deterministic, but does not leak any
        // information about the data block except for equivalence of
        // identical blocks (under the same master key). The IV will
        // be used as an authentication tag. The ciphertext and the
        // tag will be written directly in the dest buffer.
        const std::string_view plaintext = std::string_view(source, source_size);

        encrypt(plaintext, dest);
        return source_size + 24;
    }

    void CompressionCodecEncrypted::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size [[maybe_unused]]) const
    {
        UInt64 encrypted_text_key_id;
        source = readVarUInt(encrypted_text_key_id, source, 8);
        source_size -= 8;
        // Extract the IV from the encrypted data block. Decrypt the
        // block with the extracted IV, and compare the tag. Throw an
        // exception if tags don't match.
        const std::string_view ciphertext_and_tag = std::string_view(source, source_size);
        assert(ciphertext_and_tag.size() == uncompressed_size + 16);

        decrypt(ciphertext_and_tag, dest, encrypted_text_key_id);
    }

    std::string CompressionCodecEncrypted::lastErrorString()
    {
        std::array<char, 1024> buffer;
        ERR_error_string_n(ERR_get_error(), buffer.data(), buffer.size());
        return std::string(buffer.data());
    }

    void CompressionCodecEncrypted::encrypt(const std::string_view & plaintext, char * ciphertext_and_tag)
    {
        // randomly generated nonce. Split 12 bytes, because it is too big value and cannot be converted.
        // Some values are to large for char, so static_cast resolves this problem
        std::string random_nonce {static_cast<char>(0x3a), 0x3b, static_cast<char>(0xcb), static_cast<char>(0x9d),
         static_cast<char>(0x7b), 0x43, 0x47, static_cast<char>(0x99), static_cast<char>(0x8c), 0x42, 0x4f, 0x27};
        std::string_view nonce(random_nonce);
        char * start_of_encrypted = writeVarUInt(current_key_id, ciphertext_and_tag);

        size_t out_len;
        const int ok = EVP_AEAD_CTX_seal(&getKeys().ctx,
                                         reinterpret_cast<uint8_t *>(start_of_encrypted),
                                         &out_len, plaintext.size() + 16,
                                         reinterpret_cast<const uint8_t *>(nonce.data()), nonce.size(),
                                         reinterpret_cast<const uint8_t *>(plaintext.data()), plaintext.size(),
                                         nullptr, 0);
        if (!ok)
            throw Exception(lastErrorString(), ErrorCodes::OPENSSL_ERROR);

        assert(out_len == plaintext.size() + 16);
    }

    void CompressionCodecEncrypted::decrypt(const std::string_view & ciphertext, char * plaintext, UInt64 key_id)
    {
        std::string random_nonce {static_cast<char>(0x3a), 0x3b, static_cast<char>(0xcb), static_cast<char>(0x9d),
         static_cast<char>(0x7b), 0x43, 0x47, static_cast<char>(0x99), static_cast<char>(0x8c), 0x42, 0x4f, 0x27};
        std::string_view nonce(random_nonce);

        size_t out_len;
        if (key_id == current_key_id)
        {
            const int ok = EVP_AEAD_CTX_open(&getKeys().ctx,
                                            reinterpret_cast<uint8_t *>(plaintext),
                                            &out_len, ciphertext.size(),
                                            reinterpret_cast<const uint8_t *>(nonce.data()), nonce.size(),
                                            reinterpret_cast<const uint8_t *>(ciphertext.data()), ciphertext.size(),
                                            nullptr, 0);
            if (!ok)
                throw Exception(lastErrorString(), ErrorCodes::OPENSSL_ERROR);
        } else
        {
            KeyHolder decrypt_key(keys_storage[key_id]);

            const int ok = EVP_AEAD_CTX_open(&decrypt_key.ctx,
                                            reinterpret_cast<uint8_t *>(plaintext),
                                            &out_len, ciphertext.size(),
                                            reinterpret_cast<const uint8_t *>(nonce.data()), nonce.size(),
                                            reinterpret_cast<const uint8_t *>(ciphertext.data()), ciphertext.size(),
                                            nullptr, 0);
            if (!ok)
                throw Exception(lastErrorString(), ErrorCodes::OPENSSL_ERROR);
        }

        assert(out_len == ciphertext.size() - 16);
    }

    String unhexKey(const String & hex)
    {
        try
        {
            return boost::algorithm::unhex(hex);
        }
        catch (const std::exception &)
        {
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Cannot read key_hex, check for valid characters [0-9a-fA-F] and length");
        }
    }

    void CompressionCodecEncrypted::loadEncryptionKey(const Poco::Util::AbstractConfiguration & config, const String &config_prefix)
    {
        try
        {
            Strings config_keys;
            config.keys(config_prefix, config_keys);
            for (const std::string & config_key : config_keys)
            {
                String key;
                UInt64 key_id;

                if ((config_key == "key") || config_key.starts_with("key["))
                {
                    key = config.getString(config_prefix + "." + config_key, "");
                    key_id = config.getUInt64(config_prefix + "." + config_key + "[@id]", 0);
                }
                else if ((config_key == "key_hex") || config_key.starts_with("key_hex["))
                {
                    key = unhexKey(config.getString(config_prefix + "." + config_key, ""));
                    key_id = config.getUInt64(config_prefix + "." + config_key + "[@id]", 0);
                }
                else
                    continue;

                if (keys_storage.contains(key_id))
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple keys have the same ID {}", key_id);
                keys_storage[key_id] = key;
            }

            if (keys_storage.empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "No keys, an encrypted disk needs keys to work");

            current_key_id = config.getUInt64(config_prefix + ".current_key_id", 0);
            if (!keys_storage.contains(current_key_id))
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found a key with the current ID {}", current_key_id);
            if (keys_storage[current_key_id].size() != 16)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Got an encryption key with unexpected size {}, the size should be 16", keys_storage[current_key_id].size());
            setMasterKey(keys_storage[current_key_id]);
        }
        catch (Exception & e)
        {
            e.addMessage("Exception in loadEncryptedKey.");
            throw;
        }
    }

    // Updates encryption keys from config.
    void CompressionCodecEncrypted::updateEncryptionKeys(const Poco::Util::AbstractConfiguration & config, const String &config_prefix)
    {
        keys_storage.clear();
        if (config.has("encryption_codecs.key_hex") || config.has("encryption_codecs.key"))
            loadEncryptionKey(config, config_prefix);
    }

    void registerCodecEncrypted(CompressionCodecFactory & factory)
    {
        const auto method_code = uint8_t(CompressionMethodByte::Encrypted);
        factory.registerCompressionCodec("AES_128_GCM_SIV", method_code, [&](const ASTPtr & arguments) -> CompressionCodecPtr
        {
            if (arguments)
            {
                if (!arguments->children.empty())
                    throw Exception("Codec AES_128_GCM_SIV must have 0 parameter, given " +
                                    std::to_string(arguments->children.size()),
                                    ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);

                return std::make_shared<CompressionCodecEncrypted>();
            }
            else
            {
                /* The factory is asking us to construct the codec
                 * only from the method code. How can that be
                 * possible? For now we only support a single cipher
                 * so it's not really a problem, but if we were to
                 * support more ciphers it would be catastrophic. */
                return std::make_shared<CompressionCodecEncrypted>();
            }
        });
    }
}

#else /* USE_SSL && USE_INTERNAL_SSL_LIBRARY */

namespace DB
{
    void registerCodecEncrypted(CompressionCodecFactory &)
    {
    }
}

#endif /* USE_SSL && USE_INTERNAL_SSL_LIBRARY */
