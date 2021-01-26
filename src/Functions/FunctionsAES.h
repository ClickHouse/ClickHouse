#pragma once

#if !defined(ARCADIA_BUILD)
#    include <Common/config.h>
#endif

#if USE_SSL
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <fmt/format.h>

#include <openssl/evp.h>
#include <openssl/engine.h>

#include <string_view>
#include <functional>
#include <initializer_list>

#include <string.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}
}

namespace OpenSSLDetails
{
[[noreturn]] void onError(std::string error_message);
StringRef foldEncryptionKeyInMySQLCompatitableMode(size_t cipher_key_size, const StringRef & key, std::array<char, EVP_MAX_KEY_LENGTH> & folded_key);

const EVP_CIPHER * getCipherByName(const StringRef & name);

enum class CompatibilityMode
{
    MySQL,
    OpenSSL
};

enum class CipherMode
{
    MySQLCompatibility,   // with key folding
    OpenSSLCompatibility, // just as regular openssl's enc application does (AEAD modes, like GCM and CCM are not supported)
    RFC5116_AEAD_AES_GCM  // AEAD GCM with custom IV length and tag (HMAC) appended to the ciphertext, see https://tools.ietf.org/html/rfc5116#section-5.1
};


template <CipherMode mode>
struct KeyHolder
{
    inline StringRef setKey(size_t cipher_key_size, const StringRef & key) const
    {
        if (key.size != cipher_key_size)
            throw DB::Exception(fmt::format("Invalid key size: {} expected {}", key.size, cipher_key_size),
                    DB::ErrorCodes::BAD_ARGUMENTS);

        return key;
    }
};

template <>
struct KeyHolder<CipherMode::MySQLCompatibility>
{
    inline StringRef setKey(size_t cipher_key_size, const StringRef & key)
    {
        if (key.size < cipher_key_size)
            throw DB::Exception(fmt::format("Invalid key size: {} expected {}", key.size, cipher_key_size),
                    DB::ErrorCodes::BAD_ARGUMENTS);

        // MySQL does something fancy with the keys that are too long,
        // ruining compatibility with OpenSSL and not improving security.
        // But we have to do the same to be compatitable with MySQL.
        // see https://github.com/mysql/mysql-server/blob/8.0/router/src/harness/src/my_aes_openssl.cc#L71
        // (my_aes_create_key function)
        return foldEncryptionKeyInMySQLCompatitableMode(cipher_key_size, key, folded_key);
    }

    ~KeyHolder()
    {
        OPENSSL_cleanse(folded_key.data(), folded_key.size());
    }

private:
    std::array<char, EVP_MAX_KEY_LENGTH> folded_key;
};

template <CompatibilityMode compatibility_mode>
inline void validateCipherMode(const EVP_CIPHER * evp_cipher)
{
    if constexpr (compatibility_mode == CompatibilityMode::MySQL)
    {
        switch (EVP_CIPHER_mode(evp_cipher))
        {
            case EVP_CIPH_ECB_MODE: [[fallthrough]];
            case EVP_CIPH_CBC_MODE: [[fallthrough]];
            case EVP_CIPH_CFB_MODE: [[fallthrough]];
            case EVP_CIPH_OFB_MODE:
                return;
        }
    }
    else if constexpr (compatibility_mode == CompatibilityMode::OpenSSL)
    {
        switch (EVP_CIPHER_mode(evp_cipher))
        {
            case EVP_CIPH_ECB_MODE: [[fallthrough]];
            case EVP_CIPH_CBC_MODE: [[fallthrough]];
            case EVP_CIPH_CFB_MODE: [[fallthrough]];
            case EVP_CIPH_OFB_MODE: [[fallthrough]];
            case EVP_CIPH_CTR_MODE: [[fallthrough]];
            case EVP_CIPH_GCM_MODE:
                return;
        }
    }

    throw DB::Exception("Unsupported cipher mode " + std::string(EVP_CIPHER_name(evp_cipher)), DB::ErrorCodes::BAD_ARGUMENTS);
}

template <CipherMode mode>
inline void validateIV(const StringRef & iv_value, const size_t cipher_iv_size)
{
    // In MySQL mode we don't care if IV is longer than expected, only if shorter.
    if ((mode == CipherMode::MySQLCompatibility && iv_value.size != 0 && iv_value.size < cipher_iv_size)
            || (mode == CipherMode::OpenSSLCompatibility && iv_value.size != 0 && iv_value.size != cipher_iv_size))
        throw DB::Exception(fmt::format("Invalid IV size: {} expected {}", iv_value.size, cipher_iv_size),
                DB::ErrorCodes::BAD_ARGUMENTS);
}

}

namespace DB
{
template <typename Impl>
class FunctionEncrypt : public IFunction
{
public:
    static constexpr OpenSSLDetails::CompatibilityMode compatibility_mode = Impl::compatibility_mode;
    static constexpr auto name = Impl::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionEncrypt>(); }

private:
    using CipherMode = OpenSSLDetails::CipherMode;

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto optional_args = FunctionArgumentDescriptors{
            {"IV", isStringOrFixedString, nullptr, "Initialization vector binary string"},
        };

        if constexpr (compatibility_mode == OpenSSLDetails::CompatibilityMode::OpenSSL)
        {
            optional_args.emplace_back(FunctionArgumentDescriptor{
                "AAD", isStringOrFixedString, nullptr, "Additional authenticated data binary string for GCM mode"
            });
        }

        validateFunctionArgumentTypes(*this, arguments,
            FunctionArgumentDescriptors{
                {"mode", isStringOrFixedString, isColumnConst, "encryption mode string"},
                {"input", nullptr, nullptr, "plaintext"},
                {"key", isStringOrFixedString, nullptr, "encryption key binary string"},
            },
            optional_args
        );

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        using namespace OpenSSLDetails;

        const auto mode = arguments[0].column->getDataAt(0);

        if (mode.size == 0 || !std::string_view(mode).starts_with("aes-"))
            throw Exception("Invalid mode: " + mode.toString(), ErrorCodes::BAD_ARGUMENTS);

        const auto * evp_cipher = getCipherByName(mode);
        if (evp_cipher == nullptr)
            throw Exception("Invalid mode: " + mode.toString(), ErrorCodes::BAD_ARGUMENTS);

        const auto cipher_mode = EVP_CIPHER_mode(evp_cipher);

        const auto input_column = arguments[1].column;
        const auto key_column = arguments[2].column;

        OpenSSLDetails::validateCipherMode<compatibility_mode>(evp_cipher);

        ColumnPtr result_column;
        if (arguments.size() <= 3)
            result_column = doEncrypt(evp_cipher, input_rows_count, input_column, key_column, nullptr, nullptr);
        else
        {
            const auto iv_column = arguments[3].column;
            if (compatibility_mode != OpenSSLDetails::CompatibilityMode::MySQL && EVP_CIPHER_iv_length(evp_cipher) == 0)
                throw Exception(mode.toString() + " does not support IV", ErrorCodes::BAD_ARGUMENTS);

            if (arguments.size() <= 4)
            {
                result_column = doEncrypt(evp_cipher, input_rows_count, input_column, key_column, iv_column, nullptr);
            }
            else
            {
                if (cipher_mode != EVP_CIPH_GCM_MODE)
                    throw Exception("AAD can be only set for GCM-mode", ErrorCodes::BAD_ARGUMENTS);

                const auto aad_column = arguments[4].column;
                result_column = doEncrypt(evp_cipher, input_rows_count, input_column, key_column, iv_column, aad_column);
            }
        }

        return result_column;
    }

    template <typename InputColumnType, typename KeyColumnType, typename IvColumnType, typename AadColumnType>
    static ColumnPtr doEncrypt(const EVP_CIPHER * evp_cipher,
                    size_t input_rows_count,
                    const InputColumnType & input_column,
                    const KeyColumnType & key_column,
                    const IvColumnType & iv_column,
                    const AadColumnType & aad_column)
    {
        if constexpr (compatibility_mode == OpenSSLDetails::CompatibilityMode::MySQL)
        {
            return doEncryptImpl<CipherMode::MySQLCompatibility>(evp_cipher, input_rows_count, input_column, key_column, iv_column, aad_column);
        }
        else
        {
            if (EVP_CIPHER_mode(evp_cipher) == EVP_CIPH_GCM_MODE)
            {
                return doEncryptImpl<CipherMode::RFC5116_AEAD_AES_GCM>(evp_cipher, input_rows_count, input_column, key_column, iv_column, aad_column);
            }
            else
            {
                return doEncryptImpl<CipherMode::OpenSSLCompatibility>(evp_cipher, input_rows_count, input_column, key_column, iv_column, aad_column);
            }
        }

        return nullptr;
    }

    template <CipherMode mode, typename InputColumnType, typename KeyColumnType, typename IvColumnType, typename AadColumnType>
    static ColumnPtr doEncryptImpl(const EVP_CIPHER * evp_cipher,
                    size_t input_rows_count,
                    const InputColumnType & input_column,
                    const KeyColumnType & key_column,
                    [[maybe_unused]] const IvColumnType & iv_column,
                    [[maybe_unused]] const AadColumnType & aad_column)
    {
        using namespace OpenSSLDetails;

        auto evp_ctx_ptr = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>(EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free);
        auto * evp_ctx = evp_ctx_ptr.get();

        const auto block_size = static_cast<size_t>(EVP_CIPHER_block_size(evp_cipher));
        const auto key_size = static_cast<size_t>(EVP_CIPHER_key_length(evp_cipher));
        [[maybe_unused]] const auto iv_size = static_cast<size_t>(EVP_CIPHER_iv_length(evp_cipher));
        const auto tag_size = 16; // https://tools.ietf.org/html/rfc5116#section-5.1

        auto encrypted_result_column = ColumnString::create();
        auto & encrypted_result_column_data = encrypted_result_column->getChars();
        auto & encrypted_result_column_offsets = encrypted_result_column->getOffsets();

        {
            size_t resulting_size = 0;
            // for modes with block_size > 1, plaintext is padded up to a block_size,
            // which may result in allocating to much for block_size = 1.
            // That may lead later to reading unallocated data from underlying PaddedPODArray
            // due to assumption that it is safe to read up to 15 bytes past end.
            const auto pad_to_next_block = block_size == 1 ? 0 : 1;
            for (size_t r = 0; r < input_rows_count; ++r)
            {
                resulting_size += (input_column->getDataAt(r).size / block_size + pad_to_next_block) * block_size + 1;
                if constexpr (mode == CipherMode::RFC5116_AEAD_AES_GCM)
                    resulting_size += tag_size;
            }
#if defined(MEMORY_SANITIZER)
            encrypted_result_column_data.resize_fill(resulting_size, 0xFF);
#else
            encrypted_result_column_data.resize(resulting_size);
#endif
        }

        auto * encrypted = encrypted_result_column_data.data();

        KeyHolder<mode> key_holder;

        for (size_t r = 0; r < input_rows_count; ++r)
        {
            const auto key_value = key_holder.setKey(key_size, key_column->getDataAt(r));
            auto iv_value = StringRef{};
            if constexpr (!std::is_same_v<std::nullptr_t, std::decay_t<IvColumnType>>)
            {
                iv_value = iv_column->getDataAt(r);
            }

            const auto input_value = input_column->getDataAt(r);

            if constexpr (mode != CipherMode::MySQLCompatibility)
            {
                // in GCM mode IV can be of arbitrary size (>0), IV is optional for other modes.
                if (mode == CipherMode::RFC5116_AEAD_AES_GCM && iv_value.size == 0)
                {
                    throw Exception("Invalid IV size " + std::to_string(iv_value.size) + " != expected size " + std::to_string(iv_size),
                            DB::ErrorCodes::BAD_ARGUMENTS);
                }

                if (mode != CipherMode::RFC5116_AEAD_AES_GCM && key_value.size != key_size)
                {
                    throw Exception("Invalid key size " + std::to_string(key_value.size) + " != expected size " + std::to_string(key_size),
                            DB::ErrorCodes::BAD_ARGUMENTS);
                }
            }

            // Avoid extra work on empty ciphertext/plaintext for some ciphers
            if (!(input_value.size == 0 && block_size == 1 && mode != CipherMode::RFC5116_AEAD_AES_GCM))
            {
                // 1: Init CTX
                if constexpr (mode == CipherMode::RFC5116_AEAD_AES_GCM)
                {
                    // 1.a.1: Init CTX with custom IV length and optionally with AAD
                    if (EVP_EncryptInit_ex(evp_ctx, evp_cipher, nullptr, nullptr, nullptr) != 1)
                        onError("Failed to initialize encryption context with cipher");

                    if (EVP_CIPHER_CTX_ctrl(evp_ctx, EVP_CTRL_AEAD_SET_IVLEN, iv_value.size, nullptr) != 1)
                        onError("Failed to set custom IV length to " + std::to_string(iv_value.size));

                    if (EVP_EncryptInit_ex(evp_ctx, nullptr, nullptr,
                            reinterpret_cast<const unsigned char*>(key_value.data),
                            reinterpret_cast<const unsigned char*>(iv_value.data)) != 1)
                        onError("Failed to set key and IV");

                    // 1.a.2 Set AAD
                    if constexpr (!std::is_same_v<std::nullptr_t, std::decay_t<AadColumnType>>)
                    {
                        const auto aad_data = aad_column->getDataAt(r);
                        int tmp_len = 0;
                        if (aad_data.size != 0 && EVP_EncryptUpdate(evp_ctx, nullptr, &tmp_len,
                                reinterpret_cast<const unsigned char *>(aad_data.data), aad_data.size) != 1)
                            onError("Failed to set AAD data");
                    }
                }
                else
                {
                    // 1.b: Init CTX
                    validateIV<mode>(iv_value, iv_size);

                    if (EVP_EncryptInit_ex(evp_ctx, evp_cipher, nullptr,
                            reinterpret_cast<const unsigned char*>(key_value.data),
                            reinterpret_cast<const unsigned char*>(iv_value.data)) != 1)
                        onError("Failed to initialize cipher context");
                }

                int output_len = 0;
                // 2: Feed the data to the cipher
                if (EVP_EncryptUpdate(evp_ctx,
                        reinterpret_cast<unsigned char*>(encrypted), &output_len,
                        reinterpret_cast<const unsigned char*>(input_value.data), static_cast<int>(input_value.size)) != 1)
                    onError("Failed to encrypt");
                encrypted += output_len;

                // 3: retrieve encrypted data (ciphertext)
                if (EVP_EncryptFinal_ex(evp_ctx,
                        reinterpret_cast<unsigned char*>(encrypted), &output_len) != 1)
                    onError("Failed to fetch ciphertext");
                encrypted += output_len;

                // 4: optionally retrieve a tag and append it to the ciphertext (RFC5116):
                // https://tools.ietf.org/html/rfc5116#section-5.1
                if constexpr (mode == CipherMode::RFC5116_AEAD_AES_GCM)
                {
                    if (EVP_CIPHER_CTX_ctrl(evp_ctx, EVP_CTRL_AEAD_GET_TAG, tag_size, encrypted) != 1)
                        onError("Failed to retrieve GCM tag");
                    encrypted += tag_size;
                }
            }

            *encrypted = '\0';
            ++encrypted;

            encrypted_result_column_offsets.push_back(encrypted - encrypted_result_column_data.data());
        }

        // in case of block size of 1, we overestimate buffer required for encrypted data, fix it up.
        if (!encrypted_result_column_offsets.empty() && encrypted_result_column_data.size() > encrypted_result_column_offsets.back())
        {
            encrypted_result_column_data.resize(encrypted_result_column_offsets.back());
        }

        encrypted_result_column->validate();
        return encrypted_result_column;
    }
};


/// AES_decrypt(string, key, block_mode[, init_vector])
template <typename Impl>
class FunctionDecrypt : public IFunction
{
public:
    static constexpr OpenSSLDetails::CompatibilityMode compatibility_mode = Impl::compatibility_mode;
    static constexpr auto name = Impl::name;
    static FunctionPtr create(const Context &) { return std::make_shared<FunctionDecrypt>(); }

private:
    using CipherMode = OpenSSLDetails::CipherMode;

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto optional_args = FunctionArgumentDescriptors{
            {"IV", isStringOrFixedString, nullptr, "Initialization vector binary string"},
        };

        if constexpr (compatibility_mode == OpenSSLDetails::CompatibilityMode::OpenSSL)
        {
            optional_args.emplace_back(FunctionArgumentDescriptor{
                "AAD", isStringOrFixedString, nullptr, "Additional authenticated data binary string for GCM mode"
            });
        }

        validateFunctionArgumentTypes(*this, arguments,
            FunctionArgumentDescriptors{
                {"mode", isStringOrFixedString, isColumnConst, "decryption mode string"},
                {"input", nullptr, nullptr, "ciphertext"},
                {"key", isStringOrFixedString, nullptr, "decryption key binary string"},
            },
            optional_args
        );

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        using namespace OpenSSLDetails;

        const auto mode = arguments[0].column->getDataAt(0);
        if (mode.size == 0 || !std::string_view(mode).starts_with("aes-"))
            throw Exception("Invalid mode: " + mode.toString(), ErrorCodes::BAD_ARGUMENTS);

        const auto * evp_cipher = getCipherByName(mode);
        if (evp_cipher == nullptr)
            throw Exception("Invalid mode: " + mode.toString(), ErrorCodes::BAD_ARGUMENTS);

        OpenSSLDetails::validateCipherMode<compatibility_mode>(evp_cipher);

        const auto input_column = arguments[1].column;
        const auto key_column = arguments[2].column;

        ColumnPtr result_column;
        if (arguments.size() <= 3)
            result_column = doDecrypt(evp_cipher, input_rows_count, input_column, key_column, nullptr, nullptr);
        else
        {
            const auto iv_column = arguments[3].column;
            if (compatibility_mode != OpenSSLDetails::CompatibilityMode::MySQL && EVP_CIPHER_iv_length(evp_cipher) == 0)
                throw Exception(mode.toString() + " does not support IV", ErrorCodes::BAD_ARGUMENTS);

            if (arguments.size() <= 4)
            {
                result_column = doDecrypt(evp_cipher, input_rows_count, input_column, key_column, iv_column, nullptr);
            }
            else
            {
                if (EVP_CIPHER_mode(evp_cipher) != EVP_CIPH_GCM_MODE)
                    throw Exception("AAD can be only set for GCM-mode", ErrorCodes::BAD_ARGUMENTS);

                const auto aad_column = arguments[4].column;
                result_column = doDecrypt(evp_cipher, input_rows_count, input_column, key_column, iv_column, aad_column);
            }
        }

        return result_column;
    }

    template <typename InputColumnType, typename KeyColumnType, typename IvColumnType, typename AadColumnType>
    static ColumnPtr doDecrypt(const EVP_CIPHER * evp_cipher,
                    size_t input_rows_count,
                    const InputColumnType & input_column,
                    const KeyColumnType & key_column,
                    const IvColumnType & iv_column,
                    const AadColumnType & aad_column)
    {
        if constexpr (compatibility_mode == OpenSSLDetails::CompatibilityMode::MySQL)
        {
            return doDecryptImpl<CipherMode::MySQLCompatibility>(evp_cipher, input_rows_count, input_column, key_column, iv_column, aad_column);
        }
        else
        {
            const auto cipher_mode = EVP_CIPHER_mode(evp_cipher);
            if (cipher_mode == EVP_CIPH_GCM_MODE)
            {
                return doDecryptImpl<CipherMode::RFC5116_AEAD_AES_GCM>(evp_cipher, input_rows_count, input_column, key_column, iv_column, aad_column);
            }
            else
            {
                return doDecryptImpl<CipherMode::OpenSSLCompatibility>(evp_cipher, input_rows_count, input_column, key_column, iv_column, aad_column);
            }
        }

        return nullptr;
    }

    template <CipherMode mode, typename InputColumnType, typename KeyColumnType, typename IvColumnType, typename AadColumnType>
    static ColumnPtr doDecryptImpl(const EVP_CIPHER * evp_cipher,
                    size_t input_rows_count,
                    const InputColumnType & input_column,
                    const KeyColumnType & key_column,
                    [[maybe_unused]] const IvColumnType & iv_column,
                    [[maybe_unused]] const AadColumnType & aad_column)
    {
        using namespace OpenSSLDetails;

        auto evp_ctx_ptr = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>(EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free);
        auto * evp_ctx = evp_ctx_ptr.get();

        [[maybe_unused]] const auto block_size = static_cast<size_t>(EVP_CIPHER_block_size(evp_cipher));
        [[maybe_unused]] const auto iv_size = static_cast<size_t>(EVP_CIPHER_iv_length(evp_cipher));
        const auto key_size = static_cast<size_t>(EVP_CIPHER_key_length(evp_cipher));
        const auto tag_size = 16; // https://tools.ietf.org/html/rfc5116#section-5.1

        auto decrypted_result_column = ColumnString::create();
        auto & decrypted_result_column_data = decrypted_result_column->getChars();
        auto & decrypted_result_column_offsets = decrypted_result_column->getOffsets();

        {
            size_t resulting_size = 0;
            for (size_t r = 0; r < input_rows_count; ++r)
            {
                resulting_size += input_column->getDataAt(r).size + 1;
                if constexpr (mode == CipherMode::RFC5116_AEAD_AES_GCM)
                    resulting_size -= tag_size;
            }

#if defined(MEMORY_SANITIZER)
            // Pre-fill result column with values to prevent MSAN from dropping dead on
            // aes-X-ecb mode with "WARNING: MemorySanitizer: use-of-uninitialized-value".
            // This is most likely to be caused by the underlying assembler implementation:
            // see crypto/aes/aesni-x86_64.s, function aesni_ecb_encrypt
            // which msan seems to fail instrument correctly.
            decrypted_result_column_data.resize_fill(resulting_size, 0xFF);
#else
            decrypted_result_column_data.resize(resulting_size);
#endif
        }
        auto * decrypted = decrypted_result_column_data.data();

        KeyHolder<mode> key_holder;
        for (size_t r = 0; r < input_rows_count; ++r)
        {
            // 0: prepare key if required
            auto key_value = key_holder.setKey(key_size, key_column->getDataAt(r));
            auto iv_value = StringRef{};
            if constexpr (!std::is_same_v<std::nullptr_t, std::decay_t<IvColumnType>>)
            {
                iv_value = iv_column->getDataAt(r);
            }

            auto input_value = input_column->getDataAt(r);
            if constexpr (mode == CipherMode::RFC5116_AEAD_AES_GCM)
            {
                // empty plaintext results in empty ciphertext + tag, means there should be at least tag_size bytes.
                if (input_value.size < tag_size)
                    throw Exception(fmt::format("Encrypted data is too short: only {} bytes, "
                            "should contain at least {} bytes of a tag.",
                            input_value.size, block_size, tag_size), ErrorCodes::BAD_ARGUMENTS);
                input_value.size -= tag_size;
            }

            if constexpr (mode != CipherMode::MySQLCompatibility)
            {
                // in GCM mode IV can be of arbitrary size (>0), for other modes IV is optional.
                if (mode == CipherMode::RFC5116_AEAD_AES_GCM && iv_value.size == 0)
                {
                    throw Exception("Invalid IV size " + std::to_string(iv_value.size) + " != expected size " + std::to_string(iv_size),
                            DB::ErrorCodes::BAD_ARGUMENTS);
                }

                if (key_value.size != key_size)
                {
                    throw Exception("Invalid key size " + std::to_string(key_value.size) + " != expected size " + std::to_string(key_size),
                            DB::ErrorCodes::BAD_ARGUMENTS);
                }
            }

            // Avoid extra work on empty ciphertext/plaintext for some ciphers
            if (!(input_value.size == 0 && block_size == 1 && mode != CipherMode::RFC5116_AEAD_AES_GCM))
            {
                // 1: Init CTX
                if constexpr (mode == CipherMode::RFC5116_AEAD_AES_GCM)
                {
                    if (EVP_DecryptInit_ex(evp_ctx, evp_cipher, nullptr, nullptr, nullptr) != 1)
                        onError("Failed to initialize cipher context 1");

                    // 1.a.1 : Set custom IV length
                    if (EVP_CIPHER_CTX_ctrl(evp_ctx, EVP_CTRL_AEAD_SET_IVLEN, iv_value.size, nullptr) != 1)
                        onError("Failed to set custom IV length to " + std::to_string(iv_value.size));

                    // 1.a.1 : Init CTX with key and IV
                    if (EVP_DecryptInit_ex(evp_ctx, nullptr, nullptr,
                            reinterpret_cast<const unsigned char*>(key_value.data),
                            reinterpret_cast<const unsigned char*>(iv_value.data)) != 1)
                        onError("Failed to set key and IV");

                    // 1.a.2: Set AAD if present
                    if constexpr (!std::is_same_v<std::nullptr_t, std::decay_t<AadColumnType>>)
                    {
                        const auto aad_data = aad_column->getDataAt(r);
                        int tmp_len = 0;
                        if (aad_data.size != 0 && EVP_DecryptUpdate(evp_ctx, nullptr, &tmp_len,
                                reinterpret_cast<const unsigned char *>(aad_data.data), aad_data.size) != 1)
                            onError("Failed to sed AAD data");
                    }
                }
                else
                {
                    // 1.b: Init CTX
                    validateIV<mode>(iv_value, iv_size);

                    if (EVP_DecryptInit_ex(evp_ctx, evp_cipher, nullptr,
                            reinterpret_cast<const unsigned char*>(key_value.data),
                            reinterpret_cast<const unsigned char*>(iv_value.data)) != 1)
                        onError("Failed to initialize cipher context");
                }

                // 2: Feed the data to the cipher
                int output_len = 0;
                if (EVP_DecryptUpdate(evp_ctx,
                        reinterpret_cast<unsigned char*>(decrypted), &output_len,
                        reinterpret_cast<const unsigned char*>(input_value.data), static_cast<int>(input_value.size)) != 1)
                    onError("Failed to decrypt");
                decrypted += output_len;

                // 3: optionally get tag from the ciphertext (RFC5116) and feed it to the context
                if constexpr (mode == CipherMode::RFC5116_AEAD_AES_GCM)
                {
                    void * tag = const_cast<void *>(reinterpret_cast<const void *>(input_value.data + input_value.size));
                    if (EVP_CIPHER_CTX_ctrl(evp_ctx, EVP_CTRL_AEAD_SET_TAG, tag_size, tag) != 1)
                        onError("Failed to set tag");
                }

                // 4: retrieve encrypted data (ciphertext)
                if (EVP_DecryptFinal_ex(evp_ctx,
                        reinterpret_cast<unsigned char*>(decrypted), &output_len) != 1)
                    onError("Failed to decrypt");
                decrypted += output_len;
            }

            *decrypted = '\0';
            ++decrypted;

            decrypted_result_column_offsets.push_back(decrypted - decrypted_result_column_data.data());

        }

        // in case we overestimate buffer required for decrypted data, fix it up.
        if (!decrypted_result_column_offsets.empty() && decrypted_result_column_data.size() > decrypted_result_column_offsets.back())
        {
            decrypted_result_column_data.resize(decrypted_result_column_offsets.back());
        }

        decrypted_result_column->validate();
        return decrypted_result_column;
    }
};

}


#endif
