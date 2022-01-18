#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif
#include <Compression/CompressionFactory.h>
#if USE_SSL && USE_INTERNAL_SSL_LIBRARY

#include <Compression/CompressionCodecEncrypted.h>
#include <Parsers/ASTLiteral.h>
#include <cassert>
#include <openssl/digest.h> // Y_IGNORE
#include <openssl/err.h>
#include <openssl/hkdf.h> // Y_IGNORE
#include <string_view>

namespace DB
{
    namespace ErrorCodes
    {
        extern const int ILLEGAL_CODEC_PARAMETER;
        extern const int ILLEGAL_SYNTAX_FOR_CODEC_TYPE;
        extern const int NO_ELEMENTS_IN_CONFIG;
        extern const int OPENSSL_ERROR;
    }

    void CompressionCodecEncrypted::setMasterKey(const std::string_view & master_key)
    {
        keys.emplace(master_key);
    }

    CompressionCodecEncrypted::KeyHolder::KeyHolder(const std::string_view & master_key)
    {
        // Derive a key from it.
        keygen_key = deriveKey(master_key);

        // EVP_AEAD_CTX is not stateful so we can create an
        // instance now.
        EVP_AEAD_CTX_zero(&ctx);
        const int ok = EVP_AEAD_CTX_init(&ctx, EVP_aead_aes_128_gcm(),
                                         reinterpret_cast<const uint8_t*>(keygen_key.data()), keygen_key.size(),
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

    CompressionCodecEncrypted::CompressionCodecEncrypted(const std::string_view & cipher)
    {
        setCodecDescription("Encrypted", {std::make_shared<ASTLiteral>(cipher)});
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
        return uncompressed_size + 16;
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
        return source_size + 16;
    }

    void CompressionCodecEncrypted::doDecompressData(const char * source, UInt32 source_size, char * dest, UInt32 uncompressed_size [[maybe_unused]]) const
    {
        // Extract the IV from the encrypted data block. Decrypt the
        // block with the extracted IV, and compare the tag. Throw an
        // exception if tags don't match.
        const std::string_view ciphertext_and_tag = std::string_view(source, source_size);
        assert(ciphertext_and_tag.size() == uncompressed_size + 16);

        decrypt(ciphertext_and_tag, dest);
    }

    std::string CompressionCodecEncrypted::lastErrorString()
    {
        std::array<char, 1024> buffer;
        ERR_error_string_n(ERR_get_error(), buffer.data(), buffer.size());
        return std::string(buffer.data());
    }

    std::string CompressionCodecEncrypted::deriveKey(const std::string_view & master_key)
    {
        std::string_view salt(""); // No salt: derive keys in a deterministic manner.
        std::string_view info("Codec Encrypted('AES-128-GCM-SIV') key generation key");
        std::array<char, 32> result;

        const int ok = HKDF(reinterpret_cast<uint8_t *>(result.data()), result.size(),
                            EVP_sha256(),
                            reinterpret_cast<const uint8_t *>(master_key.data()), master_key.size(),
                            reinterpret_cast<const uint8_t *>(salt.data()), salt.size(),
                            reinterpret_cast<const uint8_t *>(info.data()), info.size());
        if (!ok)
            throw Exception(lastErrorString(), ErrorCodes::OPENSSL_ERROR);

        return std::string(result.data(), 16);
    }

    void CompressionCodecEncrypted::encrypt(const std::string_view & plaintext, char * ciphertext_and_tag)
    {
        // Fixed nonce. Yes this is unrecommended, but we have to live
        // with it.
        std::string_view nonce("\0\0\0\0\0\0\0\0\0\0\0\0", 12);

        size_t out_len;
        const int ok = EVP_AEAD_CTX_seal(&getKeys().ctx,
                                         reinterpret_cast<uint8_t *>(ciphertext_and_tag),
                                         &out_len, plaintext.size() + 16,
                                         reinterpret_cast<const uint8_t *>(nonce.data()), nonce.size(),
                                         reinterpret_cast<const uint8_t *>(plaintext.data()), plaintext.size(),
                                         nullptr, 0);
        if (!ok)
            throw Exception(lastErrorString(), ErrorCodes::OPENSSL_ERROR);

        assert(out_len == plaintext.size() + 16);
    }

    void CompressionCodecEncrypted::decrypt(const std::string_view & ciphertext, char * plaintext)
    {
        std::string_view nonce("\0\0\0\0\0\0\0\0\0\0\0\0", 12);

        size_t out_len;
        const int ok = EVP_AEAD_CTX_open(&getKeys().ctx,
                                         reinterpret_cast<uint8_t *>(plaintext),
                                         &out_len, ciphertext.size(),
                                         reinterpret_cast<const uint8_t *>(nonce.data()), nonce.size(),
                                         reinterpret_cast<const uint8_t *>(ciphertext.data()), ciphertext.size(),
                                         nullptr, 0);
        if (!ok)
            throw Exception(lastErrorString(), ErrorCodes::OPENSSL_ERROR);

        assert(out_len == ciphertext.size() - 16);
    }

    void registerCodecEncrypted(CompressionCodecFactory & factory)
    {
        const auto method_code = uint8_t(CompressionMethodByte::Encrypted);
        factory.registerCompressionCodec("Encrypted", method_code, [&](const ASTPtr & arguments) -> CompressionCodecPtr
        {
            if (arguments)
            {
                if (arguments->children.size() != 1)
                    throw Exception("Codec Encrypted() must have 1 parameter, given " +
                                    std::to_string(arguments->children.size()),
                                    ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);

                const auto children = arguments->children;
                const auto * literal = children[0]->as<ASTLiteral>();
                if (!literal)
                    throw Exception("Wrong argument for codec Encrypted(). Expected a string literal",
                                    ErrorCodes::ILLEGAL_SYNTAX_FOR_CODEC_TYPE);

                const String cipher = literal->value.safeGet<String>();
                if (cipher == "AES-128-GCM-SIV")
                    return std::make_shared<CompressionCodecEncrypted>(cipher);
                else
                    throw Exception("Cipher '" + cipher + "' is not supported",
                                    ErrorCodes::ILLEGAL_CODEC_PARAMETER);
            }
            else
            {
                /* The factory is asking us to construct the codec
                 * only from the method code. How can that be
                 * possible? For now we only support a single cipher
                 * so it's not really a problem, but if we were to
                 * support more ciphers it would be catastrophic. */
                return std::make_shared<CompressionCodecEncrypted>("AES-128-GCM-SIV");
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
