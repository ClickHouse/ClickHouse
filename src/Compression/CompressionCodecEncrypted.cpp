#include <string_view>
#include <Common/config.h>
#include <Common/Exception.h>
#include <base/types.h>
#include <IO/VarInt.h>
#include <Compression/CompressionFactory.h>
#include <Compression/CompressionCodecEncrypted.h>
#include <Poco/Logger.h>
#include <Common/logger_useful.h>

// This depends on BoringSSL-specific API, notably <openssl/aead.h>.
#if USE_SSL
#include <openssl/digest.h>
#include <openssl/err.h>
#include <boost/algorithm/hex.hpp>
#include <openssl/aead.h>
#endif

// Common part for both parts (with SSL and without)
namespace DB
{

namespace ErrorCodes
{
    extern const int OPENSSL_ERROR;
}

namespace
{

/// Get string name for method. Return empty string for undefined Method
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

/// Get method code (used for codec, to understand which one we are using)
uint8_t getMethodCode(EncryptionMethod Method)
{
    if (Method == AES_128_GCM_SIV)
    {
        return static_cast<uint8_t>(CompressionMethodByte::AES_128_GCM_SIV);
    }
    else if (Method == AES_256_GCM_SIV)
    {
        return static_cast<uint8_t>(CompressionMethodByte::AES_256_GCM_SIV);
    }
    else
    {
        throw Exception("Wrong encryption Method. Got " + getMethodName(Method), ErrorCodes::BAD_ARGUMENTS);
    }
}

} // end of namespace

} // end of namespace DB

#if USE_SSL
namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
    extern const int LOGICAL_ERROR;
    extern const int BAD_ARGUMENTS;
    extern const int INCORRECT_DATA;
}

namespace
{
constexpr size_t tag_size          = 16;   /// AES-GCM-SIV always uses a tag of 16 bytes length
constexpr size_t key_id_max_size   = 8;    /// Max size of varint.
constexpr size_t nonce_max_size    = 13;   /// Nonce size and one byte to show if nonce in in text
constexpr size_t actual_nonce_size = 12;   /// Nonce actual size
const String empty_nonce = {"\0\0\0\0\0\0\0\0\0\0\0\0", actual_nonce_size};

/// Get encryption/decryption algorithms.
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

/// Find out key size for each algorithm
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
    std::array<char, 1024> buffer = {};
    ERR_error_string_n(ERR_get_error(), buffer.data(), buffer.size());
    return std::string(buffer.data());
}

/// Encrypt plaintext with particular algorithm and put result into ciphertext_and_tag.
/// This function get key and nonce and encrypt text with their help.
/// If something went wrong (can't init context or can't encrypt data) it throws exception.
/// It returns length of encrypted text.
size_t encrypt(std::string_view plaintext, char * ciphertext_and_tag, EncryptionMethod method, const String & key, const String & nonce)
{
    /// Init context for encryption, using key.
    EVP_AEAD_CTX encrypt_ctx;
    EVP_AEAD_CTX_zero(&encrypt_ctx);
    const int ok_init = EVP_AEAD_CTX_init(&encrypt_ctx, getMethod(method)(),
                                            reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                                            tag_size, nullptr);
    if (!ok_init)
        throw Exception(lastErrorString(), ErrorCodes::OPENSSL_ERROR);

    /// encrypt data using context and given nonce.
    size_t out_len;
    const int ok_open = EVP_AEAD_CTX_seal(&encrypt_ctx,
                                            reinterpret_cast<uint8_t *>(ciphertext_and_tag),
                                            &out_len, plaintext.size() + tag_size,
                                            reinterpret_cast<const uint8_t *>(nonce.data()), nonce.size(),
                                            reinterpret_cast<const uint8_t *>(plaintext.data()), plaintext.size(),
                                            nullptr, 0);
    if (!ok_open)
        throw Exception(lastErrorString(), ErrorCodes::OPENSSL_ERROR);

    return out_len;
}

/// Encrypt plaintext with particular algorithm and put result into ciphertext_and_tag.
/// This function get key and nonce and encrypt text with their help.
/// If something went wrong (can't init context or can't encrypt data) it throws exception.
/// It returns length of encrypted text.
size_t decrypt(std::string_view ciphertext, char * plaintext, EncryptionMethod method, const String & key, const String & nonce)
{
    /// Init context for decryption with given key.
    EVP_AEAD_CTX decrypt_ctx;
    EVP_AEAD_CTX_zero(&decrypt_ctx);

    const int ok_init = EVP_AEAD_CTX_init(&decrypt_ctx, getMethod(method)(),
                                          reinterpret_cast<const uint8_t*>(key.data()), key.size(),
                                          tag_size, nullptr);
    if (!ok_init)
        throw Exception(lastErrorString(), ErrorCodes::OPENSSL_ERROR);

    /// decrypt data using given nonce
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

/// Register codec in factory
void registerEncryptionCodec(CompressionCodecFactory & factory, EncryptionMethod Method)
{
    const auto method_code = getMethodCode(Method); /// Codec need to know its code
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

/// Firstly, write a byte, which shows if the nonce will be put in text (if it was defined in config)
/// Secondly, write nonce in text (this step depends from first step)
/// return new position to write
inline char* writeNonce(const String& nonce, char* dest)
{
    /// If nonce consists of nul bytes, it shouldn't be in dest. Zero byte is the only byte that should be written.
    /// Otherwise, 1 is written and data from nonce is copied
    if (nonce != empty_nonce)
    {
        *dest = 1;
        ++dest;
        size_t copied_symbols = nonce.copy(dest, nonce.size());
        if (copied_symbols != nonce.size())
            throw Exception(ErrorCodes::INCORRECT_DATA, "Can't copy nonce into destination. Count of copied symbols {}, need to copy {}", copied_symbols, nonce.size());
        dest += copied_symbols;
        return dest;
    }
    else
    {
        *dest = 0;
        return ++dest;
    }
}

/// Firstly, read a byte, which shows if the nonce will be put in text (if it was defined in config)
/// Secondly, read nonce in text (this step depends from first step)
/// return new position to read
inline const char* readNonce(String& nonce, const char* source)
{
    /// If first is zero byte: move source and set zero-bytes nonce
    if (!*source)
    {
        nonce = empty_nonce;
        return ++source;
    }
    /// Move to next byte. Nonce will begin from there
    ++source;

    /// Otherwise, use data from source in nonce
    nonce = {source, actual_nonce_size};
    source += actual_nonce_size;
    return source;
}

}

CompressionCodecEncrypted::Configuration & CompressionCodecEncrypted::Configuration::instance()
{
    static CompressionCodecEncrypted::Configuration ret;
    return ret;
}

void CompressionCodecEncrypted::Configuration::loadImpl(
    const Poco::Util::AbstractConfiguration & config, const String & config_prefix, EncryptionMethod method, std::unique_ptr<Params> & new_params)
{
    // if method is not smaller than MAX_ENCRYPTION_METHOD it is incorrect
    if (method >= MAX_ENCRYPTION_METHOD)
        throw Exception("Wrong argument for loading configurations.", ErrorCodes::BAD_ARGUMENTS);

    /// Scan all keys in config and add them into storage. If key is in hex, transform it.
    /// Remember key ID for each key, because it will be used in encryption/decryption
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

        /// For each key its id should be unique.
        if (new_params->keys_storage[method].contains(key_id))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Multiple keys have the same ID {}", key_id);

        /// Check size of key. Its length depends on encryption algorithm.
        if (key.size() != methodKeySize(method))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Got an encryption key with unexpected size {}, the size should be {}",
                key.size(), methodKeySize(method));

        new_params->keys_storage[method][key_id] = key;
    }

    /// Check that we have at least one key for this method (otherwise it is incorrect to use it).
    if (new_params->keys_storage[method].empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "No keys, an encryption needs keys to work");

    if (!config.has(config_prefix + ".current_key_id"))
    {
        /// In case of multiple keys, current_key_id is mandatory
        if (new_params->keys_storage[method].size() > 1)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "There are multiple keys in config. current_key_id is required");

        /// If there is only one key with non zero ID, curren_key_id should be defined.
        if (new_params->keys_storage[method].size() == 1 && !new_params->keys_storage[method].contains(0))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Config has one key with non zero id. сurrent_key_id is required");
    }

    /// Try to find which key will be used for encryption. If there is no current_key and only one key without id
    /// or with zero id, first key will be used for encryption (its index equals to zero).
    new_params->current_key_id[method] = config.getUInt64(config_prefix + ".current_key_id", 0);

    /// Check that we have current key. Otherwise config is incorrect.
    if (!new_params->keys_storage[method].contains(new_params->current_key_id[method]))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Not found a key with the current ID {}", new_params->current_key_id[method]);

    /// Read nonce (in hex or in string). Its length should be 12 bytes (actual_nonce_size).
    if (config.has(config_prefix + ".nonce_hex"))
        new_params->nonce[method] = unhexKey(config.getString(config_prefix + ".nonce_hex"));
    else
        new_params->nonce[method] = config.getString(config_prefix + ".nonce", "");

    if (new_params->nonce[method].size() != actual_nonce_size && !new_params->nonce[method].empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Got nonce with unexpected size {}, the size should be {}", new_params->nonce[method].size(), actual_nonce_size);
}

bool CompressionCodecEncrypted::Configuration::tryLoad(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    /// Try to create new parameters and fill them from config.
    /// If there will be some errors, print their message to notify user that
    /// something went wrong and new parameters are not available
    try
    {
        load(config, config_prefix);
    }
    catch (...)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
        return false;
    }
    return true;
}

void CompressionCodecEncrypted::Configuration::load(const Poco::Util::AbstractConfiguration & config, const String & config_prefix)
{
    /// Try to create new parameters and fill them from config.
    /// If there will be some errors, throw error
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

void CompressionCodecEncrypted::Configuration::getCurrentKeyAndNonce(EncryptionMethod method, UInt64 & current_key_id, String &current_key, String & nonce) const
{
    /// It parameters were not set, throw exception
    if (!params.get())
        throw Exception("Empty params in CompressionCodecEncrypted configuration", ErrorCodes::BAD_ARGUMENTS);

    /// Save parameters in variable, because they can always change.
    /// As this function not atomic, we should be certain that we get information from one particular version for correct work.
    const auto current_params = params.get();
    current_key_id = current_params->current_key_id[method];

    /// As parameters can be created empty, we need to check that this key is available.
    if (current_params->keys_storage[method].contains(current_key_id))
        current_key = current_params->keys_storage[method].at(current_key_id);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no current_key {} in config. Please, put it in config and reload.", current_key_id);

    /// If there is no nonce in config, we need to generate particular one,
    /// because all encryptions should have nonce and random nonce generation will lead to cases
    /// when nonce after config reload (nonce is not defined in config) will differ from previously generated one.
    /// This will lead to data loss.
    nonce = current_params->nonce[method];
    if (nonce.empty())
        nonce = empty_nonce;
}

String CompressionCodecEncrypted::Configuration::getKey(EncryptionMethod method, const UInt64 & key_id) const
{
    String key;
    /// See description of previous finction, logic is the same.
    if (!params.get())
        throw Exception("Empty params in CompressionCodecEncrypted configuration", ErrorCodes::BAD_ARGUMENTS);

    const auto current_params = params.get();

    /// check if there is current key in storage
    if (current_params->keys_storage[method].contains(key_id))
        key = current_params->keys_storage[method].at(key_id);
    else
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "There is no key {} in config", key_id);

    return key;
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
    // KeyID is followed by byte, that shows if nonce was set in config (and also will be put into data)
    // and 12 bytes nonce or this byte will be equal to zero and no nonce will follow it.
    return uncompressed_size + tag_size + key_id_max_size + nonce_max_size;
}

UInt32 CompressionCodecEncrypted::doCompressData(const char * source, UInt32 source_size, char * dest) const
{
    // Nonce, key and plaintext will be used to generate authentication tag
    // and message encryption key. AES-GCM-SIV authenticates the encoded additional data and plaintext.
    // For this purpose message_authentication_key is used.
    // Algorithm is completely deterministic, but does not leak any
    // information about the data block except for equivalence of
    // identical blocks (under the same key).

    const std::string_view plaintext = std::string_view(source, source_size);

    /// Get key and nonce for encryption
    UInt64 current_key_id;
    String current_key, nonce;
    Configuration::instance().getCurrentKeyAndNonce(encryption_method, current_key_id, current_key, nonce);

    /// Write current key id to support multiple keys.
    /// (key id in the beginning will help to decrypt data after changing current key)
    char* ciphertext_with_nonce = writeVarUInt(current_key_id, dest);
    size_t keyid_size = ciphertext_with_nonce - dest;

    /// write nonce in data. This will help to read data even after changing nonce in config
    /// If there were no nonce in data, one zero byte will be written
    char* ciphertext = writeNonce(nonce, ciphertext_with_nonce);
    UInt64 nonce_size = ciphertext - ciphertext_with_nonce;

    // The ciphertext and the authentication tag will be written directly in the dest buffer.
    size_t out_len = encrypt(plaintext, ciphertext, encryption_method, current_key, nonce);

    /// Length of encrypted text should be equal to text length plus tag_size (which was added by algorithm).
    if (out_len != source_size + tag_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't encrypt data, length after encryption {} is wrong, expected {}", out_len, source_size + tag_size);

    return out_len + keyid_size + nonce_size;
}

void CompressionCodecEncrypted::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size) const
{
    /// The key is needed for decrypting. That's why it is read at the beginning of process.
    UInt64 key_id;
    const char * ciphertext_with_nonce = readVarUInt(key_id, source, source_size);

    /// Size of text should be decreased by key_size, because key_size bytes were not participating in encryption process.
    size_t keyid_size = ciphertext_with_nonce - source;
    String nonce;
    String key = Configuration::instance().getKey(encryption_method, key_id);

    /// try to read nonce from file (if it was set while encrypting)
    const char * ciphertext = readNonce(nonce, ciphertext_with_nonce);

    /// Size of text should be decreased by nonce_size, because nonce_size bytes were not participating in encryption process.
    UInt64 nonce_size = ciphertext - ciphertext_with_nonce;

    /// Count text size (nonce and key_id was read from source)
    size_t ciphertext_size = source_size - keyid_size - nonce_size;
    if (ciphertext_size != uncompressed_size + tag_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't decrypt data, uncompressed_size {} is wrong, expected {}", uncompressed_size, ciphertext_size - tag_size);


    size_t out_len = decrypt({ciphertext, ciphertext_size}, dest, encryption_method, key, nonce);
    if (out_len != ciphertext_size - tag_size)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Can't decrypt data, out length after decryption {} is wrong, expected {}", out_len, ciphertext_size - tag_size);
}

}

#else /* USE_SSL */

namespace DB
{

namespace
{

/// Register codec in factory
void registerEncryptionCodec(CompressionCodecFactory & factory, EncryptionMethod Method)
{
    auto throw_no_ssl = [](const ASTPtr &) -> CompressionCodecPtr { throw Exception(ErrorCodes::OPENSSL_ERROR, "Server was built without SSL support. Encryption is disabled."); };
    const auto method_code = getMethodCode(Method); /// Codec need to know its code
    factory.registerCompressionCodec(getMethodName(Method), method_code, throw_no_ssl);
}

}

CompressionCodecEncrypted::Configuration & CompressionCodecEncrypted::Configuration::instance()
{
    static CompressionCodecEncrypted::Configuration ret;
    return ret;
}

/// if encryption is disabled.
bool CompressionCodecEncrypted::Configuration::tryLoad(const Poco::Util::AbstractConfiguration & config [[maybe_unused]], const String & config_prefix [[maybe_unused]])
{
    return false;
}

/// if encryption is disabled, print warning about this.
void CompressionCodecEncrypted::Configuration::load(const Poco::Util::AbstractConfiguration & config [[maybe_unused]], const String & config_prefix [[maybe_unused]])
{
    LOG_WARNING(&Poco::Logger::get("CompressionCodecEncrypted"), "Server was built without SSL support. Encryption is disabled.");
}

}

#endif /* USE_SSL */

namespace DB
{
/// Register codecs for all algorithms
void registerCodecEncrypted(CompressionCodecFactory & factory)
{
    registerEncryptionCodec(factory, AES_128_GCM_SIV);
    registerEncryptionCodec(factory, AES_256_GCM_SIV);
}
}
