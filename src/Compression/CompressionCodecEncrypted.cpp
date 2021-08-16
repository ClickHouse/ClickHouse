#include <algorithm>
#include <memory>
#include <string>
#include <Common/config.h>
#include "Common/Exception.h"
#include "common/types.h"
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
UInt8 length_of_varint = 8;

namespace ErrorCodes
{
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int OPENSSL_ERROR;
    extern const int BAD_ARGUMENTS;
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

CompressionCodecEncrypted::Configuration & CompressionCodecEncrypted::Configuration::instance()
{
    static CompressionCodecEncrypted::Configuration ret;
    return ret;
}

void CompressionCodecEncrypted::Configuration::load(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    Params new_params;
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

        if (new_params.keys_storage.contains(key_id))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple keys have the same ID {}", key_id);
        new_params.keys_storage[key_id] = key;
    }

    if (new_params.keys_storage.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No keys, an encrypted disk needs keys to work");

    new_params.current_key_id = config.getUInt64(config_prefix + ".current_key_id", 0);
    if (!new_params.keys_storage.contains(new_params.current_key_id))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found a key with the current ID {}", new_params.current_key_id);
    if (new_params.keys_storage[new_params.current_key_id].size() != 16)
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Got an encryption key with unexpected size {}, the size should be 16",
            new_params.keys_storage[new_params.current_key_id].size());

    if (config.has(config_prefix + ".nonce_hex"))
        new_params.nonce = unhexKey(config.getString(config_prefix + ".nonce_hex"));
    else
        new_params.nonce = config.getString(config_prefix + ".nonce", "");

    if (new_params.nonce.size() != 12 && !new_params.nonce.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Got nonce with unexpected size {}, the size should be 12", new_params.nonce.size());

    params.set(std::make_unique<Params>(new_params));
}

String CompressionCodecEncrypted::Configuration::getKey(UInt64 id) const
{
    return params.get()->keys_storage.at(id);
}

String CompressionCodecEncrypted::Configuration::getCurrentKey() const
{
    return params.get()->keys_storage.at(params.get()->current_key_id);
}

UInt64 CompressionCodecEncrypted::Configuration::getCurrentID() const
{
    return params.get()->current_key_id;
}

String CompressionCodecEncrypted::Configuration::getNonce() const
{
    return params.get()->nonce;
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
    source_size -= length_of_varint;
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
    auto& conf = Configuration::instance();
    String nonce_from_config = conf.getNonce();
    std::string_view nonce("\0\0\0\0\0\0\0\0\0\0\0\0", 12);
    if (!nonce_from_config.empty())
        nonce = nonce_from_config;

    char * start_of_encrypted = writeVarUInt(conf.getCurrentID(), ciphertext_and_tag);

    EVP_AEAD_CTX encrypt_ctx;
    EVP_AEAD_CTX_zero(&encrypt_ctx);
    const int ok_init = EVP_AEAD_CTX_init(&encrypt_ctx, EVP_aead_aes_128_gcm_siv(),
                                            reinterpret_cast<const uint8_t*>(conf.getCurrentKey().data()), conf.getCurrentKey().size(),
                                            16 /* tag size */, nullptr);
    if (!ok_init)
        throw Exception(lastErrorString(), ErrorCodes::OPENSSL_ERROR);
    size_t out_len;
    const int ok_open = EVP_AEAD_CTX_seal(&encrypt_ctx,
                                            reinterpret_cast<uint8_t *>(start_of_encrypted),
                                            &out_len, plaintext.size() + 16,
                                            reinterpret_cast<const uint8_t *>(nonce.data()), nonce.size(),
                                            reinterpret_cast<const uint8_t *>(plaintext.data()), plaintext.size(),
                                            nullptr, 0);
    if (!ok_open)
        throw Exception(lastErrorString(), ErrorCodes::OPENSSL_ERROR);

    assert(out_len == plaintext.size() + 16);
}

void CompressionCodecEncrypted::decrypt(const std::string_view & ciphertext, char * plaintext, UInt64 key_id)
{
    auto& conf = Configuration::instance();
    String nonce_from_config = conf.getNonce();
    std::string_view nonce("\0\0\0\0\0\0\0\0\0\0\0\0", 12);
    if (!nonce_from_config.empty())
        nonce = nonce_from_config;

    size_t out_len;
    EVP_AEAD_CTX decrypt_ctx;
    EVP_AEAD_CTX_zero(&decrypt_ctx);

    const int ok_init = EVP_AEAD_CTX_init(&decrypt_ctx, EVP_aead_aes_128_gcm_siv(),
                                          reinterpret_cast<const uint8_t*>(conf.getKey(key_id).data()), conf.getKey(key_id).size(),
                                          16 /* tag size */, nullptr);
    if (!ok_init)
        throw Exception(lastErrorString(), ErrorCodes::OPENSSL_ERROR);

    const int ok_open = EVP_AEAD_CTX_open(&decrypt_ctx,
                                          reinterpret_cast<uint8_t *>(plaintext),
                                          &out_len, ciphertext.size(),
                                          reinterpret_cast<const uint8_t *>(nonce.data()), nonce.size(),
                                          reinterpret_cast<const uint8_t *>(ciphertext.data()), ciphertext.size(),
                                          nullptr, 0);
    if (!ok_open)
        throw Exception(lastErrorString(), ErrorCodes::OPENSSL_ERROR);

    assert(out_len == ciphertext.size() - 16);
}

bool CompressionCodecEncrypted::tryLoadEncryptionKey(const Poco::Util::AbstractConfiguration & config, const String &config_prefix)
{
    try
    {
        Configuration::instance().load(config, config_prefix);
    }
    catch (Exception & e)
    {
        std::cout << e.message() << std::endl;
        return false;
    }
    return true;
}

// Updates encryption keys from config.
void CompressionCodecEncrypted::updateEncryptionKeys(const Poco::Util::AbstractConfiguration & config, const String &config_prefix)
{
    if (config.has(config_prefix + ".key_hex") || config.has(config_prefix + ".key"))
        tryLoadEncryptionKey(config, config_prefix);
}

void registerCodecEncrypted(CompressionCodecFactory & factory)
{
    const auto method_code = uint8_t(CompressionMethodByte::Encrypted);
    factory.registerCompressionCodec("AES_128_GCM_SIV", method_code, [&](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        if (arguments)
        {
            if (!arguments->children.empty())
                throw Exception("Codec AES_128_GCM_SIV must not have parameters, given " +
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
