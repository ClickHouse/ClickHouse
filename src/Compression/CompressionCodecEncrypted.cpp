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
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
}

namespace
{

String getMethodName(EncryptionMethod Method)
{
    if (Method == AES_128_GCM_SIV)
    {
        return "AES_128_GCM_SIV";
    }
    else if (Method == AES_256_GCM_SIV)
    {
        return "AES_256_GCM_SIV";
    }
    else
    {
        return "";
    }
}

auto getMethod(EncryptionMethod Method)
{
    if (Method == AES_128_GCM_SIV)
    {
        return EVP_aead_aes_128_gcm_siv;
    }
    else if (Method == AES_256_GCM_SIV)
    {
        return EVP_aead_aes_256_gcm_siv;
    }
    else
    {
        throw Exception("Wrong encryption Method. Got " + getMethodName(Method), ErrorCodes::BAD_ARGUMENTS);
    }
}

UInt64 methodKeySize(EncryptionMethod Method)
{
    if (Method == AES_128_GCM_SIV)
    {
        return 16;
    }
    else if (Method == AES_256_GCM_SIV)
    {
        return 32;
    }
    else
    {
        throw Exception("Wrong encryption Method. Got " + getMethodName(Method), ErrorCodes::BAD_ARGUMENTS);
    }
}

std::string lastErrorString()
{
    std::array<char, 1024> buffer;
    ERR_error_string_n(ERR_get_error(), buffer.data(), buffer.size());
    return std::string(buffer.data());
}

size_t encrypt(const std::string_view & plaintext, char * ciphertext_and_tag, EncryptionMethod method, const String& current_key, const String& nonce)
{
    EVP_AEAD_CTX encrypt_ctx;
    EVP_AEAD_CTX_zero(&encrypt_ctx);
    const int ok_init = EVP_AEAD_CTX_init(&encrypt_ctx, getMethod(method)(),
                                            reinterpret_cast<const uint8_t*>(current_key.data()), current_key.size(),
                                            16 /* tag size */, nullptr);
    if (!ok_init)
        throw Exception(lastErrorString(), ErrorCodes::OPENSSL_ERROR);

    size_t out_len;
    const int ok_open = EVP_AEAD_CTX_seal(&encrypt_ctx,
                                            reinterpret_cast<uint8_t *>(ciphertext_and_tag),
                                            &out_len, plaintext.size() + 16,
                                            reinterpret_cast<const uint8_t *>(nonce.data()), nonce.size(),
                                            reinterpret_cast<const uint8_t *>(plaintext.data()), plaintext.size(),
                                            nullptr, 0);
    if (!ok_open)
        throw Exception(lastErrorString(), ErrorCodes::OPENSSL_ERROR);

    return out_len;
}

size_t decrypt(const std::string_view & ciphertext, char * plaintext, EncryptionMethod method, const String& current_key, const String& nonce)
{
    EVP_AEAD_CTX decrypt_ctx;
    EVP_AEAD_CTX_zero(&decrypt_ctx);

    const int ok_init = EVP_AEAD_CTX_init(&decrypt_ctx, getMethod(method)(),
                                          reinterpret_cast<const uint8_t*>(current_key.data()), current_key.size(),
                                          16 /* tag size */, nullptr);
    if (!ok_init)
        throw Exception(lastErrorString(), ErrorCodes::OPENSSL_ERROR);

    size_t out_len;
    const int ok_open = EVP_AEAD_CTX_open(&decrypt_ctx,
                                          reinterpret_cast<uint8_t *>(plaintext),
                                          &out_len, ciphertext.size(),
                                          reinterpret_cast<const uint8_t *>(nonce.data()), nonce.size(),
                                          reinterpret_cast<const uint8_t *>(ciphertext.data()), ciphertext.size(),
                                          nullptr, 0);
    if (!ok_open)
        throw Exception(lastErrorString(), ErrorCodes::OPENSSL_ERROR);

    return out_len;
}

uint8_t getMethodCode(EncryptionMethod Method)
{
    if (Method == AES_128_GCM_SIV)
    {
        return uint8_t(CompressionMethodByte::AES_128_GCM_SIV);
    }
    else if (Method == AES_256_GCM_SIV)
    {
        return uint8_t(CompressionMethodByte::AES_256_GCM_SIV);
    }
    else
    {
        throw Exception("Wrong encryption Method. Got " + getMethodName(Method), ErrorCodes::BAD_ARGUMENTS);
    }
}

void registerEncryptionCodec(CompressionCodecFactory & factory, EncryptionMethod Method)
{
    const auto method_code = getMethodCode(Method);
    factory.registerCompressionCodec(getMethodName(Method), method_code, [&, Method](const ASTPtr & arguments) -> CompressionCodecPtr
    {
        if (arguments)
        {
            if (!arguments->children.empty())
                throw Exception("Codec " + getMethodName(Method) + " must not have parameters, given " +
                                std::to_string(arguments->children.size()),
                                ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);
        }
        return std::make_shared<CompressionCodecEncrypted>(Method);
    });
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

}

CompressionCodecEncrypted::Configuration & CompressionCodecEncrypted::Configuration::instance()
{
    static CompressionCodecEncrypted::Configuration ret;
    return ret;
}

void CompressionCodecEncrypted::Configuration::loadImpl(
    const Poco::Util::AbstractConfiguration & config, const String & config_prefix, EncryptionMethod method, std::unique_ptr<Params>& new_params)
{
    if (method == MAX_ENCRYPTION_METHOD)
        throw Exception("Wrong argument for loading configurations.", ErrorCodes::BAD_ARGUMENTS);

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

        if (new_params->keys_storage[method].contains(key_id))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple keys have the same ID {}", key_id);
        new_params->keys_storage[method][key_id] = key;
    }

    if (new_params->keys_storage[method].empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No keys, an encrypted disk needs keys to work");

    new_params->current_key_id[method] = config.getUInt64(config_prefix + ".current_key_id", 0);
    if (!new_params->keys_storage[method].contains(new_params->current_key_id[method]))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found a key with the current ID {}", new_params->current_key_id[method]);
    if (new_params->keys_storage[method][new_params->current_key_id[method]].size() != methodKeySize(method))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS,
            "Got an encryption key with unexpected size {}, the size should be {}",
            new_params->keys_storage[method][new_params->current_key_id[method]].size(), methodKeySize(method));

    if (config.has(config_prefix + ".nonce_hex"))
        new_params->nonce[method] = unhexKey(config.getString(config_prefix + ".nonce_hex"));
    else
        new_params->nonce[method] = config.getString(config_prefix + ".nonce", "");

    if (new_params->nonce[method].size() != 12 && !new_params->nonce[method].empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Got nonce with unexpected size {}, the size should be 12", new_params->nonce[method].size());
}

void CompressionCodecEncrypted::Configuration::tryload(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    try
    {
        std::unique_ptr<Params> new_params(new Params);
        if (config.has(config_prefix + ".aes_128_gcm_siv"))
        {
            loadImpl(config, config_prefix + ".aes_128_gcm_siv", AES_128_GCM_SIV, new_params);
        }
        if (config.has(config_prefix + ".aes_256_gcm_siv"))
        {
        loadImpl(config, config_prefix + ".aes_256_gcm_siv", AES_256_GCM_SIV, new_params);
        }

        params.set(std::move(new_params));
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }
}

void CompressionCodecEncrypted::Configuration::getCurrentKeyAndNonce(EncryptionMethod method, UInt64 &current_key_id, String &current_key, String &nonce) const
{
    if (!params.get())
        throw Exception("Empty params in CompressionCodecEncrypted configuration", ErrorCodes::BAD_ARGUMENTS);
    const auto current_params = params.get();
    current_key_id = current_params->current_key_id[method];
    current_key = current_params->keys_storage[method].at(current_key_id);
    nonce = current_params->nonce[method];
    if (nonce.empty())
        nonce = {"\0\0\0\0\0\0\0\0\0\0\0\0", 12};
}

void CompressionCodecEncrypted::Configuration::getKeyAndNonce(EncryptionMethod method, const UInt64 &key_id, String &key, String &nonce) const
{
    if (!params.get())
        throw Exception("Empty params in CompressionCodecEncrypted configuration", ErrorCodes::BAD_ARGUMENTS);
    const auto current_params = params.get();
    key = current_params->keys_storage[method].at(key_id);
    nonce = current_params->nonce[method];
    if (nonce.empty())
        nonce = {"\0\0\0\0\0\0\0\0\0\0\0\0", 12};
}


CompressionCodecEncrypted::CompressionCodecEncrypted(EncryptionMethod Method): encryption_method(Method)
{
    setCodecDescription(getMethodName(encryption_method));
}

uint8_t CompressionCodecEncrypted::getMethodByte() const
{
    return getMethodCode(encryption_method);
}

void CompressionCodecEncrypted::updateHash(SipHash & hash) const
{
    getCodecDesc()->updateTreeHash(hash);
}

UInt32 CompressionCodecEncrypted::getMaxCompressedDataSize(UInt32 uncompressed_size) const
{
    // The GCM mode is a stream cipher. No paddings are
    // involved. There will be a tag at the end of ciphertext (16
    // octets). Also it has not more than 8 bytes for key_id in the beginning
    return uncompressed_size + tag_size + key_id_max_size;
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

    UInt64 current_key_id;
    String current_key, nonce;
    Configuration::instance().getCurrentKeyAndNonce(encryption_method, current_key_id, current_key, nonce);

    char* ciphertext = writeVarUInt(current_key_id, dest);
    size_t keyid_size = ciphertext - dest;

    size_t out_len = encrypt(plaintext, ciphertext, encryption_method, current_key, nonce);

    if (out_len != source_size + tag_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't encrypt data, length after encryption {} is wrong, expected {}", out_len, source_size + tag_size);

    return out_len + keyid_size;
}

void CompressionCodecEncrypted::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    UInt64 key_id;
    const char * ciphertext = readVarUInt(key_id, source, source_size);
    size_t keyid_size = ciphertext - source;
    size_t ciphertext_size = source_size - keyid_size;
    if (ciphertext_size != uncompressed_size + tag_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't decrypt data, uncompressed_size {} is wrong, expected {}", uncompressed_size, ciphertext_size - tag_size);

    String key, nonce;
    Configuration::instance().getKeyAndNonce(encryption_method, key_id, key, nonce);

    size_t out_len = decrypt({ciphertext, ciphertext_size}, dest, encryption_method, key, nonce);
    if (out_len != ciphertext_size - tag_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't decrypt data, out length after decryption {} is wrong, expected {}", out_len, ciphertext_size - tag_size);
}


void registerCodecEncrypted(CompressionCodecFactory & factory)
{
    registerEncryptionCodec(factory, AES_128_GCM_SIV);
    registerEncryptionCodec(factory, AES_256_GCM_SIV);
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
