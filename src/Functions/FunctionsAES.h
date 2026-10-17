#pragma once

#include "config.h"

#include <base/MemorySanitizer.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeNullable.h>
#include <Common/OpenSSLHelpers.h>
#include <Common/safe_cast.h>

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
    extern const int OPENSSL_ERROR;
}


namespace OpenSSLDetails
{
[[noreturn]] void onError(std::string error_message);
std::string_view foldEncryptionKeyInMySQLCompatitableMode(size_t cipher_key_size, std::string_view key, std::array<char, EVP_MAX_KEY_LENGTH> & folded_key);

using EVP_CIPHER_ptr = std::unique_ptr<EVP_CIPHER, decltype(&EVP_CIPHER_free)>;

/// Fetches a provider-backed cipher via EVP_CIPHER_fetch, which (unlike a legacy cipher with
/// prov == NULL) avoids implicit EVP_CIPHER_fetch calls on every EVP_EncryptInit_ex /
/// EVP_DecryptInit_ex invocation in OpenSSL 3.x. Returns nullptr for an unknown cipher name.
EVP_CIPHER_ptr fetchCipher(std::string_view name);

/// For plain AES block cipher modes (aes-{128,192,256}-{ecb,cbc}) returns the name of the
/// ECB cipher with the same key size, used by the block-composed fast path. Returns an
/// empty view for everything else.
std::string_view ecbEquivalentCipherName(std::string_view mode);

/// Constant-time byte comparisons for PKCS#7 padding validation: 0xFF when the condition
/// holds, 0 otherwise, with no data-dependent branches.
inline UInt8 constantTimeGE(UInt8 a, UInt8 b)
{
    return static_cast<UInt8>(~((static_cast<unsigned int>(a) - b) >> 8));
}

inline UInt8 constantTimeEQ(UInt8 a, UInt8 b)
{
    return static_cast<UInt8>((static_cast<unsigned int>(a ^ b) - 1) >> 8);
}

/// Remembers the key an EVP_CIPHER_CTX has been initialized with, so rows repeating the
/// same key can re-initialize only the IV (passing a null cipher and key), skipping the
/// cipher setup and key schedule expansion that dominate the per-row cost in OpenSSL 3.x.
struct CachedKeyState
{
    bool matches(std::string_view key_value) const
    {
        return initialized && key_value.size() == key_size
            && memcmp(key_value.data(), key.data(), key_size) == 0;
    }

    void remember(std::string_view key_value)
    {
        chassert(key_value.size() <= key.size());
        memcpy(key.data(), key_value.data(), key_value.size());
        key_size = key_value.size();
        initialized = true;
    }

    /// Whether the context has been initialized with the cipher at least once. After
    /// that, a key change only needs the new key (with a null cipher), which replaces
    /// the key schedule but keeps the provider context and its parameters; re-passing
    /// the cipher would repeat the full context setup.
    bool hasContext() const { return initialized; }

    void reset() { initialized = false; }

private:
    bool initialized = false;
    size_t key_size = 0;
    std::array<char, EVP_MAX_KEY_LENGTH> key{};
};

enum class CompatibilityMode : uint8_t
{
    MySQL,
    OpenSSL
};

enum class CipherMode : uint8_t
{
    MySQLCompatibility,   // with key folding
    OpenSSLCompatibility, // just as regular openssl's enc application does (AEAD modes, like GCM and CCM are not supported)
    RFC5116_AEAD_AES_GCM  // AEAD GCM with custom IV length and tag (HMAC) appended to the ciphertext, see https://tools.ietf.org/html/rfc5116#section-5.1
};


template <CipherMode mode>
struct KeyHolder
{
    std::string_view setKey(size_t cipher_key_size, std::string_view key) const
    {
        if (key.size() != cipher_key_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid key size: {} expected {}", key.size(), cipher_key_size);

        return key;
    }
};

template <>
struct KeyHolder<CipherMode::MySQLCompatibility>
{
    std::string_view setKey(size_t cipher_key_size, std::string_view key)
    {
        if (key.size() < cipher_key_size)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid key size: {} expected {}", key.size(), cipher_key_size);

        // MySQL does something fancy with the keys that are too long,
        // ruining compatibility with OpenSSL and not improving security.
        // But we have to do the same to be compatitable with MySQL.
        // see https://github.com/mysql/mysql-server/blob/8.0/router/src/harness/src/my_aes_openssl.cc#L71
        // (my_aes_create_key function)
        return foldEncryptionKeyInMySQLCompatitableMode(cipher_key_size, key, folded_key);
    }

    /// There is a function to clear key securely.
    /// It makes absolutely zero sense to call it here because
    /// key comes from column and already copied multiple times through various memory buffers.

private:
    std::array<char, EVP_MAX_KEY_LENGTH> folded_key;
};

template <CompatibilityMode compatibility_mode>
inline void validateCipherMode(const EVP_CIPHER * evp_cipher)
{
    if constexpr (compatibility_mode == CompatibilityMode::MySQL)
    {
        switch (EVP_CIPHER_mode(evp_cipher)) /// NOLINT(bugprone-switch-missing-default-case)
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
        switch (EVP_CIPHER_mode(evp_cipher)) /// NOLINT(bugprone-switch-missing-default-case)
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

    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unsupported cipher mode");
}

template <CipherMode mode>
inline void validateIV(std::string_view iv_value, const size_t cipher_iv_size)
{
    // In MySQL mode we don't care if IV is longer than expected, only if shorter.
    if ((mode == CipherMode::MySQLCompatibility && !iv_value.empty() && iv_value.size() < cipher_iv_size)
            || (mode == CipherMode::OpenSSLCompatibility && !iv_value.empty() && iv_value.size() != cipher_iv_size))
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid IV size: {} expected {}", iv_value.size(), cipher_iv_size);
}

}

template <typename Impl>
class FunctionEncrypt final : public IFunction
{
public:
    static constexpr OpenSSLDetails::CompatibilityMode compatibility_mode = Impl::compatibility_mode;
    static constexpr auto name = Impl::name;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionEncrypt>(); }

private:
    using CipherMode = OpenSSLDetails::CipherMode;

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto optional_args = FunctionArgumentDescriptors
            {
            {"IV", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "Initialization vector binary string"},
        };

        if constexpr (compatibility_mode == OpenSSLDetails::CompatibilityMode::OpenSSL)
        {
            optional_args.emplace_back(FunctionArgumentDescriptor{
                "AAD", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "Additional authenticated data binary string for GCM mode"
            });
        }

        validateFunctionArguments(*this, arguments,
            FunctionArgumentDescriptors
            {
                {"mode", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), isColumnConst, "encryption mode string"},
                {"input", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), {}, "plaintext"},
                {"key", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), {}, "encryption key binary string"},
            },
            optional_args
        );

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        using namespace OpenSSLDetails;

        const std::string_view mode = arguments[0].column->getDataAt(0);

        if (mode.empty() || !mode.starts_with("aes-"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid mode: {}", mode);

        /// Fetch a provider-backed cipher once per block to avoid implicit EVP_CIPHER_fetch
        /// on every EVP_EncryptInit_ex call in the per-row loop.
        /// See https://github.com/ClickHouse/ClickHouse/issues/65116
        auto fetched_cipher = fetchCipher(mode);
        if (fetched_cipher == nullptr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid mode: {}", mode);
        const auto * evp_cipher = fetched_cipher.get();

        const auto cipher_mode = EVP_CIPHER_mode(evp_cipher);

        const auto input_column = arguments[1].column;
        const auto key_column = arguments[2].column;

        OpenSSLDetails::validateCipherMode<compatibility_mode>(evp_cipher);

        /// Plain AES ECB/CBC modes take a fast path composed on top of a streaming ECB
        /// cipher context (see doEncryptBlockCipher).
        EVP_CIPHER_ptr ecb_cipher(nullptr, EVP_CIPHER_free);
        if (const auto ecb_name = ecbEquivalentCipherName(mode); !ecb_name.empty())
        {
            ecb_cipher = fetchCipher(ecb_name);
            if (ecb_cipher == nullptr)
                throw Exception(ErrorCodes::OPENSSL_ERROR, "Cannot fetch cipher {}", ecb_name);
        }

        ColumnPtr result_column;
        if (arguments.size() <= 3)
            result_column = doEncrypt(fetched_cipher.get(), ecb_cipher.get(), input_rows_count, input_column, key_column, nullptr, nullptr);
        else
        {
            const auto iv_column = arguments[3].column;
            if (compatibility_mode != OpenSSLDetails::CompatibilityMode::MySQL && EVP_CIPHER_iv_length(evp_cipher) == 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} does not support IV", mode);

            if (arguments.size() <= 4)
            {
                result_column = doEncrypt(fetched_cipher.get(), ecb_cipher.get(), input_rows_count, input_column, key_column, iv_column, nullptr);
            }
            else
            {
                if (cipher_mode != EVP_CIPH_GCM_MODE)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "AAD can be only set for GCM-mode");

                const auto aad_column = arguments[4].column;
                result_column = doEncrypt(fetched_cipher.get(), ecb_cipher.get(), input_rows_count, input_column, key_column, iv_column, aad_column);
            }
        }

        return result_column;
    }

    static ColumnPtr doEncrypt(
        const EVP_CIPHER * evp_cipher,
        const EVP_CIPHER * ecb_cipher,
        size_t input_rows_count,
        const ColumnPtr & input_column,
        const ColumnPtr & key_column,
        const ColumnPtr & iv_column,
        const ColumnPtr & aad_column)
    {
        if constexpr (compatibility_mode == OpenSSLDetails::CompatibilityMode::MySQL)
        {
            if (ecb_cipher)
                return doEncryptBlockCipher<CipherMode::MySQLCompatibility>(evp_cipher, ecb_cipher, input_rows_count, input_column, key_column, iv_column);

            return doEncryptImpl<CipherMode::MySQLCompatibility>(evp_cipher, input_rows_count, input_column, key_column, iv_column, aad_column);
        }
        else
        {
            if (EVP_CIPHER_mode(evp_cipher) == EVP_CIPH_GCM_MODE)
            {
                return doEncryptImpl<CipherMode::RFC5116_AEAD_AES_GCM>(evp_cipher, input_rows_count, input_column, key_column, iv_column, aad_column);
            }

            if (ecb_cipher)
                return doEncryptBlockCipher<CipherMode::OpenSSLCompatibility>(evp_cipher, ecb_cipher, input_rows_count, input_column, key_column, iv_column);

            return doEncryptImpl<CipherMode::OpenSSLCompatibility>(
                evp_cipher, input_rows_count, input_column, key_column, iv_column, aad_column);
        }

        return nullptr;
    }

    /// Fast path for plain AES ECB and CBC modes (see ecbEquivalentCipherName).
    ///
    /// OpenSSL 3.x spends most of the per-row time in EVP_EncryptInit_ex: the provider
    /// dispatch and AES key schedule expansion cost an order of magnitude more than
    /// actually encrypting a short value. Instead of re-initializing a cipher context
    /// per row, this path keeps a single streaming ECB context with padding disabled,
    /// initialized once per distinct key, and composes the block mode manually: PKCS#7
    /// padding and, for CBC, the chaining XOR. For longer CBC rows the per-block
    /// EVP_EncryptUpdate calls lose to OpenSSL's stitched AES-NI CBC implementation,
    /// so those rows go through a CBC context that is re-initialized with only the IV
    /// (keeping the expanded key schedule) between rows.
    /// See https://github.com/ClickHouse/ClickHouse/issues/65116
    template <CipherMode mode>
    static ColumnPtr doEncryptBlockCipher(
        const EVP_CIPHER * evp_cipher,
        const EVP_CIPHER * ecb_cipher,
        size_t input_rows_count,
        const ColumnPtr & input_column,
        const ColumnPtr & key_column,
        const ColumnPtr & iv_column)
    {
        using namespace OpenSSLDetails;

        static_assert(mode != CipherMode::RFC5116_AEAD_AES_GCM);

        /// The AES block size, for both ECB and CBC.
        constexpr size_t block_size = 16;
        /// Below this input size composing CBC from per-block ECB updates beats the
        /// stitched CBC implementation re-initialized per row (the measured crossover
        /// is between 64 and 96 bytes).
        constexpr size_t stitched_cbc_threshold = 64;

        const bool is_cbc = EVP_CIPHER_mode(evp_cipher) == EVP_CIPH_CBC_MODE;
        const auto key_size = static_cast<size_t>(EVP_CIPHER_key_length(evp_cipher));
        const auto iv_size = static_cast<size_t>(EVP_CIPHER_iv_length(evp_cipher));
        chassert(static_cast<size_t>(EVP_CIPHER_block_size(evp_cipher)) == block_size);

        auto ecb_ctx_ptr = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>(EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free);
        auto cbc_ctx_ptr = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>(is_cbc ? EVP_CIPHER_CTX_new() : nullptr, &EVP_CIPHER_CTX_free);
        if (!ecb_ctx_ptr || (is_cbc && !cbc_ctx_ptr))
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_CIPHER_CTX_new failed");
        auto * ecb_ctx = ecb_ctx_ptr.get();
        auto * cbc_ctx = cbc_ctx_ptr.get();

        auto encrypted_result_column = ColumnString::create();
        auto & encrypted_result_column_data = encrypted_result_column->getChars();
        auto & encrypted_result_column_offsets = encrypted_result_column->getOffsets();

        {
            size_t resulting_size = 0;
            for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
                resulting_size += (input_column->getDataAt(row_idx).size() / block_size + 1) * block_size;
            encrypted_result_column_data.resize(resulting_size);
        }

        auto * encrypted = encrypted_result_column_data.data();

        KeyHolder<mode> key_holder{};
        CachedKeyState ecb_key_state;
        CachedKeyState cbc_key_state;
        static constexpr unsigned char zero_iv[EVP_MAX_IV_LENGTH]{};

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            const auto key_value = key_holder.setKey(key_size, key_column->getDataAt(row_idx));
            auto iv_value = std::string_view{};
            if (iv_column)
                iv_value = iv_column->getDataAt(row_idx);

            validateIV<mode>(iv_value, iv_size);
            /// A fresh cipher context starts from a zero IV when no IV is given, so an
            /// absent IV is passed as an explicit zero IV here.
            const auto * iv_ptr = iv_value.empty() ? zero_iv : reinterpret_cast<const unsigned char *>(iv_value.data());

            const std::string_view input_value = input_column->getDataAt(row_idx);
            const auto * input = reinterpret_cast<const unsigned char *>(input_value.data());
            const size_t input_size = input_value.size();
            [[maybe_unused]] const auto * row_begin = encrypted;

            int output_len = 0;
            if (is_cbc && input_size >= stitched_cbc_threshold)
            {
                if (cbc_key_state.matches(key_value))
                {
                    /// Reset only the IV, keeping the expanded key schedule.
                    if (EVP_EncryptInit_ex(cbc_ctx, nullptr, nullptr, nullptr, iv_ptr) != 1)
                        onError("EVP_EncryptInit_ex");
                }
                else
                {
                    if (EVP_EncryptInit_ex(cbc_ctx, cbc_key_state.hasContext() ? nullptr : evp_cipher, nullptr,
                            reinterpret_cast<const unsigned char *>(key_value.data()), iv_ptr) != 1)
                        onError("EVP_EncryptInit_ex");
                    cbc_key_state.remember(key_value);
                }

                if (EVP_EncryptUpdate(cbc_ctx, reinterpret_cast<unsigned char *>(encrypted), &output_len,
                        input, static_cast<int>(input_size)) != 1)
                    onError("EVP_EncryptUpdate");
                encrypted += output_len;
                if (EVP_EncryptFinal_ex(cbc_ctx, reinterpret_cast<unsigned char *>(encrypted), &output_len) != 1)
                    onError("EVP_EncryptFinal_ex");
                encrypted += output_len;
            }
            else
            {
                if (!ecb_key_state.matches(key_value))
                {
                    const bool first_init = !ecb_key_state.hasContext();
                    if (EVP_EncryptInit_ex(ecb_ctx, first_init ? ecb_cipher : nullptr, nullptr,
                            reinterpret_cast<const unsigned char *>(key_value.data()), nullptr) != 1)
                        onError("EVP_EncryptInit_ex");
                    /// The context produces raw blocks; PKCS#7 padding is applied manually below.
                    /// Key-only re-initialization keeps the setting.
                    if (first_init && EVP_CIPHER_CTX_set_padding(ecb_ctx, 0) != 1)
                        onError("EVP_CIPHER_CTX_set_padding");
                    ecb_key_state.remember(key_value);
                }

                const size_t full_blocks = input_size / block_size;
                unsigned char chain[block_size];

                if (is_cbc)
                {
                    memcpy(chain, iv_ptr, block_size);

                    for (size_t block = 0; block < full_blocks; ++block)
                    {
                        unsigned char buf[block_size];
                        const auto * src = input + block * block_size;
                        for (size_t i = 0; i < block_size; ++i)
                            buf[i] = src[i] ^ chain[i];
                        if (EVP_EncryptUpdate(ecb_ctx, reinterpret_cast<unsigned char *>(encrypted), &output_len, buf, block_size) != 1)
                            onError("EVP_EncryptUpdate");
                        memcpy(chain, encrypted, block_size);
                        encrypted += block_size;
                    }
                }
                else if (full_blocks > 0)
                {
                    if (EVP_EncryptUpdate(ecb_ctx, reinterpret_cast<unsigned char *>(encrypted), &output_len,
                            input, static_cast<int>(full_blocks * block_size)) != 1)
                        onError("EVP_EncryptUpdate");
                    encrypted += output_len;
                }

                /// The last block: remaining input bytes plus PKCS#7 padding.
                const size_t remaining = input_size - full_blocks * block_size;
                unsigned char last[block_size];
                if (remaining > 0)
                    memcpy(last, input + full_blocks * block_size, remaining);
                memset(last + remaining, static_cast<int>(block_size - remaining), block_size - remaining);
                if (is_cbc)
                {
                    for (size_t i = 0; i < block_size; ++i)
                        last[i] ^= chain[i];
                }
                if (EVP_EncryptUpdate(ecb_ctx, reinterpret_cast<unsigned char *>(encrypted), &output_len, last, block_size) != 1)
                    onError("EVP_EncryptUpdate");
                encrypted += block_size;
            }

            __msan_unpoison(row_begin, encrypted - row_begin); /// OpenSSL uses assembly which evades msan's analysis
            encrypted_result_column_offsets.push_back(encrypted - encrypted_result_column_data.data());
        }

        encrypted_result_column->validate();
        return encrypted_result_column;
    }

    template <CipherMode mode>
    static ColumnPtr doEncryptImpl(
        const EVP_CIPHER * evp_cipher,
        size_t input_rows_count,
        const ColumnPtr & input_column,
        const ColumnPtr & key_column,
        [[maybe_unused]] const ColumnPtr & iv_column,
        [[maybe_unused]] const ColumnPtr & aad_column)
    {
        using namespace OpenSSLDetails;

        auto evp_ctx_ptr = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>(EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free);
        if (!evp_ctx_ptr)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_CIPHER_CTX_new failed");
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
            for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
            {
                resulting_size += (input_column->getDataAt(row_idx).size() / block_size + pad_to_next_block) * block_size;
                if constexpr (mode == CipherMode::RFC5116_AEAD_AES_GCM)
                    resulting_size += tag_size;
            }
            encrypted_result_column_data.resize(resulting_size);
        }

        auto * encrypted = encrypted_result_column_data.data();

        KeyHolder<mode> key_holder{};
        CachedKeyState ctx_key_state;

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            const auto key_value = key_holder.setKey(key_size, key_column->getDataAt(row_idx));
            auto iv_value = std::string_view{};
            if (iv_column)
            {
                iv_value = iv_column->getDataAt(row_idx);

                /// If the length is zero (empty string is passed) it should be treat as no IV.
                if (iv_value.empty())
                    iv_value = std::string_view{};
            }

            const std::string_view input_value = input_column->getDataAt(row_idx);

            if constexpr (mode != CipherMode::MySQLCompatibility)
            {
                // in GCM mode IV can be of arbitrary size (>0), IV is optional for other modes.
                if (mode == CipherMode::RFC5116_AEAD_AES_GCM && iv_value.empty())
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid IV size {} != expected size {}", iv_value.size(), iv_size);
                }

                if (mode != CipherMode::RFC5116_AEAD_AES_GCM && key_value.size() != key_size)
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid key size {} != expected size {}", key_value.size(), key_size);
                }
            }

            // Avoid extra work on empty ciphertext/plaintext for some ciphers
            if (!(input_value.empty() && block_size == 1 && mode != CipherMode::RFC5116_AEAD_AES_GCM))
            {
                // 1: Init CTX
                if constexpr (mode == CipherMode::RFC5116_AEAD_AES_GCM)
                {
                    // 1.a.1: Init CTX with custom IV length and optionally with AAD
                    if (EVP_EncryptInit_ex(evp_ctx, evp_cipher, nullptr, nullptr, nullptr) != 1)
                        onError("EVP_EncryptInit_ex");

                    if (EVP_CIPHER_CTX_ctrl(evp_ctx, EVP_CTRL_AEAD_SET_IVLEN, safe_cast<int>(iv_value.size()), nullptr) != 1)
                        onError("EVP_CIPHER_CTX_ctrl");

                    if (EVP_EncryptInit_ex(evp_ctx, nullptr, nullptr,
                            reinterpret_cast<const unsigned char*>(key_value.data()),
                            reinterpret_cast<const unsigned char*>(iv_value.data())) != 1)
                        onError("EVP_EncryptInit_ex");

                    // 1.a.2 Set AAD
                    if (aad_column)
                    {
                        const auto aad_data = aad_column->getDataAt(row_idx);
                        int tmp_len = 0;
                        if (!aad_data.empty() && EVP_EncryptUpdate(evp_ctx, nullptr, &tmp_len,
                                reinterpret_cast<const unsigned char *>(aad_data.data()), safe_cast<int>(aad_data.size())) != 1)
                            onError("EVP_EncryptUpdate");
                    }
                }
                else
                {
                    // 1.b: Init CTX
                    validateIV<mode>(iv_value, iv_size);

                    /// A fresh full initialization starts from a zero IV when no IV is
                    /// given, while an IV-only re-initialization with a null IV pointer
                    /// would keep the previous row's advanced IV state, so an absent IV
                    /// is passed as an explicit zero IV.
                    static constexpr unsigned char zero_iv[EVP_MAX_IV_LENGTH]{};
                    const auto * iv_ptr = iv_value.empty() ? zero_iv : reinterpret_cast<const unsigned char *>(iv_value.data());

                    if (ctx_key_state.matches(key_value))
                    {
                        /// The context already holds the expanded key schedule for this
                        /// key: reset only the IV, skipping the cipher setup and key
                        /// schedule expansion that dominate the per-row cost in OpenSSL 3.x.
                        if (EVP_EncryptInit_ex(evp_ctx, nullptr, nullptr, nullptr, iv_ptr) != 1)
                            onError("EVP_EncryptInit_ex");
                    }
                    else
                    {
                        if (EVP_EncryptInit_ex(evp_ctx, ctx_key_state.hasContext() ? nullptr : evp_cipher, nullptr,
                                reinterpret_cast<const unsigned char*>(key_value.data()), iv_ptr) != 1)
                            onError("EVP_EncryptInit_ex");
                        ctx_key_state.remember(key_value);
                    }
                }

                int output_len = 0;
                // 2: Feed the data to the cipher
                if (EVP_EncryptUpdate(evp_ctx,
                        reinterpret_cast<unsigned char*>(encrypted), &output_len,
                        reinterpret_cast<const unsigned char*>(input_value.data()), static_cast<int>(input_value.size())) != 1)
                    onError("EVP_EncryptUpdate");
                __msan_unpoison(encrypted, output_len); /// OpenSSL uses assembly which evades msan's analysis
                encrypted += output_len;

                // 3: retrieve encrypted data (ciphertext)
                if (EVP_EncryptFinal_ex(evp_ctx, reinterpret_cast<unsigned char*>(encrypted), &output_len) != 1)
                    onError("EVP_EncryptFinal_ex");
                __msan_unpoison(encrypted, output_len); /// OpenSSL uses assembly which evades msan's analysis
                encrypted += output_len;

                // 4: optionally retrieve a tag and append it to the ciphertext (RFC5116):
                // https://tools.ietf.org/html/rfc5116#section-5.1
                if constexpr (mode == CipherMode::RFC5116_AEAD_AES_GCM)
                {
                    if (EVP_CIPHER_CTX_ctrl(evp_ctx, EVP_CTRL_AEAD_GET_TAG, tag_size, encrypted) != 1)
                        onError("EVP_CIPHER_CTX_ctrl");
                    encrypted += tag_size;
                }
            }

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


/// decrypt(string, key, block_mode[, init_vector])
template <typename Impl>
class FunctionDecrypt final : public IFunction
{
public:
    static constexpr OpenSSLDetails::CompatibilityMode compatibility_mode = Impl::compatibility_mode;
    static constexpr auto name = Impl::name;
    static constexpr bool use_null_when_decrypt_fail = Impl::use_null_when_decrypt_fail;
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionDecrypt>(); }

private:
    using CipherMode = OpenSSLDetails::CipherMode;

    String getName() const override { return name; }
    bool isVariadic() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {0}; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        auto optional_args = FunctionArgumentDescriptors{
            {"IV", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "Initialization vector binary string"},
        };

        if constexpr (compatibility_mode == OpenSSLDetails::CompatibilityMode::OpenSSL)
        {
            optional_args.emplace_back(FunctionArgumentDescriptor{
                "AAD", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "Additional authenticated data binary string for GCM mode"
            });
        }

        validateFunctionArguments(*this, arguments,
            FunctionArgumentDescriptors{
                {"mode", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), isColumnConst, "decryption mode string"},
                {"input", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), {}, "ciphertext"},
                {"key", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), {}, "decryption key binary string"},
            },
            optional_args
        );

        if constexpr (use_null_when_decrypt_fail)
            return std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>());

        return std::make_shared<DataTypeString>();
    }

    DataTypePtr getReturnTypeForDefaultImplementationForDynamic() const override
    {
        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        using namespace OpenSSLDetails;

        const auto mode = arguments[0].column->getDataAt(0);
        if (mode.empty() || !mode.starts_with("aes-"))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid mode: {}", mode);

        /// Fetch a provider-backed cipher once per block to avoid implicit EVP_CIPHER_fetch
        /// on every EVP_DecryptInit_ex call in the per-row loop.
        /// See https://github.com/ClickHouse/ClickHouse/issues/65116
        auto fetched_cipher = fetchCipher(mode);
        if (fetched_cipher == nullptr)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid mode: {}", mode);
        const auto * evp_cipher = fetched_cipher.get();

        OpenSSLDetails::validateCipherMode<compatibility_mode>(evp_cipher);

        /// Plain AES ECB/CBC modes take a fast path composed on top of a streaming ECB
        /// cipher context (see doDecryptBlockCipher).
        EVP_CIPHER_ptr ecb_cipher(nullptr, EVP_CIPHER_free);
        if (const auto ecb_name = ecbEquivalentCipherName(mode); !ecb_name.empty())
        {
            ecb_cipher = fetchCipher(ecb_name);
            if (ecb_cipher == nullptr)
                throw Exception(ErrorCodes::OPENSSL_ERROR, "Cannot fetch cipher {}", ecb_name);
        }

        const auto input_column = arguments[1].column;
        const auto key_column = arguments[2].column;

        ColumnPtr result_column;
        if (arguments.size() <= 3)
        {
            result_column = doDecrypt<use_null_when_decrypt_fail>(fetched_cipher.get(), ecb_cipher.get(), input_rows_count, input_column, key_column, nullptr, nullptr);
        }
        else
        {
            const auto iv_column = arguments[3].column;
            if (compatibility_mode != OpenSSLDetails::CompatibilityMode::MySQL && EVP_CIPHER_iv_length(evp_cipher) == 0)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "{} does not support IV", mode);

            if (arguments.size() <= 4)
            {
                result_column = doDecrypt<use_null_when_decrypt_fail>(fetched_cipher.get(), ecb_cipher.get(), input_rows_count, input_column, key_column, iv_column, nullptr);
            }
            else
            {
                if (EVP_CIPHER_mode(evp_cipher) != EVP_CIPH_GCM_MODE)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "AAD can be only set for GCM-mode");

                const auto aad_column = arguments[4].column;
                result_column = doDecrypt<use_null_when_decrypt_fail>(fetched_cipher.get(), ecb_cipher.get(), input_rows_count, input_column, key_column, iv_column, aad_column);
            }
        }

        return result_column;
    }
    template<bool use_null_when_decrypt_fail>
    static ColumnPtr doDecrypt(
        const EVP_CIPHER * evp_cipher,
        const EVP_CIPHER * ecb_cipher,
        size_t input_rows_count,
        const ColumnPtr & input_column,
        const ColumnPtr & key_column,
        const ColumnPtr & iv_column,
        const ColumnPtr & aad_column)
    {
        if constexpr (compatibility_mode == OpenSSLDetails::CompatibilityMode::MySQL)
        {
            if (ecb_cipher)
                return doDecryptBlockCipher<CipherMode::MySQLCompatibility, use_null_when_decrypt_fail>(evp_cipher, ecb_cipher, input_rows_count, input_column, key_column, iv_column);

            return doDecryptImpl<CipherMode::MySQLCompatibility, use_null_when_decrypt_fail>(evp_cipher, input_rows_count, input_column, key_column, iv_column, aad_column);
        }
        else
        {
            const auto cipher_mode = EVP_CIPHER_mode(evp_cipher);
            if (cipher_mode == EVP_CIPH_GCM_MODE)
            {
                return doDecryptImpl<CipherMode::RFC5116_AEAD_AES_GCM, use_null_when_decrypt_fail>(evp_cipher, input_rows_count, input_column, key_column, iv_column, aad_column);
            }

            if (ecb_cipher)
                return doDecryptBlockCipher<CipherMode::OpenSSLCompatibility, use_null_when_decrypt_fail>(evp_cipher, ecb_cipher, input_rows_count, input_column, key_column, iv_column);

            return doDecryptImpl<CipherMode::OpenSSLCompatibility, use_null_when_decrypt_fail>(
                evp_cipher, input_rows_count, input_column, key_column, iv_column, aad_column);
        }

        return nullptr;
    }

    /// Counterpart of FunctionEncrypt::doEncryptBlockCipher for plain AES ECB and CBC.
    ///
    /// Unlike encryption, block-mode decryption has no sequential dependency between
    /// blocks, so each row is decrypted with a single EVP_DecryptUpdate call on a
    /// streaming ECB context (with padding disabled), initialized once per distinct
    /// key. The CBC chaining XOR is applied afterwards, and PKCS#7 padding is
    /// validated manually, checking all padding bytes like EVP_DecryptFinal_ex does.
    /// See https://github.com/ClickHouse/ClickHouse/issues/65116
    template <CipherMode mode, bool use_null_when_decrypt_fail>
    static ColumnPtr doDecryptBlockCipher(
        const EVP_CIPHER * evp_cipher,
        const EVP_CIPHER * ecb_cipher,
        size_t input_rows_count,
        const ColumnPtr & input_column,
        const ColumnPtr & key_column,
        const ColumnPtr & iv_column)
    {
        using namespace OpenSSLDetails;

        static_assert(mode != CipherMode::RFC5116_AEAD_AES_GCM);

        /// The AES block size, for both ECB and CBC.
        constexpr size_t block_size = 16;

        const bool is_cbc = EVP_CIPHER_mode(evp_cipher) == EVP_CIPH_CBC_MODE;
        const auto key_size = static_cast<size_t>(EVP_CIPHER_key_length(evp_cipher));
        const auto iv_size = static_cast<size_t>(EVP_CIPHER_iv_length(evp_cipher));
        chassert(static_cast<size_t>(EVP_CIPHER_block_size(evp_cipher)) == block_size);

        auto ecb_ctx_ptr = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>(EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free);
        if (!ecb_ctx_ptr)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_CIPHER_CTX_new failed");
        auto * ecb_ctx = ecb_ctx_ptr.get();

        auto decrypted_result_column = ColumnString::create();
        auto null_map = ColumnUInt8::create();
        auto & decrypted_result_column_data = decrypted_result_column->getChars();
        auto & decrypted_result_column_offsets = decrypted_result_column->getOffsets();

        {
            size_t resulting_size = 0;
            for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
                resulting_size += input_column->getDataAt(row_idx).size();
            decrypted_result_column_data.resize(resulting_size);
        }

        auto * decrypted = decrypted_result_column_data.data();

        KeyHolder<mode> key_holder{};
        CachedKeyState ecb_key_state;
        static constexpr unsigned char zero_iv[EVP_MAX_IV_LENGTH]{};

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            const auto key_value = key_holder.setKey(key_size, key_column->getDataAt(row_idx));
            auto iv_value = std::string_view{};
            if (iv_column)
                iv_value = iv_column->getDataAt(row_idx);

            const std::string_view input_value = input_column->getDataAt(row_idx);
            const auto * input = reinterpret_cast<const unsigned char *>(input_value.data());
            const size_t input_size = input_value.size();

            bool decrypt_fail = false;
            /// Always decrypt empty to empty, like the generic implementation.
            if (!input_value.empty())
            {
                validateIV<mode>(iv_value, iv_size);
                const auto * iv_ptr = iv_value.empty() ? zero_iv : reinterpret_cast<const unsigned char *>(iv_value.data());

                if (input_size % block_size != 0)
                {
                    if constexpr (!use_null_when_decrypt_fail)
                        throw Exception(ErrorCodes::OPENSSL_ERROR,
                            "Cannot decrypt: data size {} is not a multiple of the cipher block size {}", input_size, block_size);
                    decrypt_fail = true;
                }
                else
                {
                    if (!ecb_key_state.matches(key_value))
                    {
                        const bool first_init = !ecb_key_state.hasContext();
                        if (EVP_DecryptInit_ex(ecb_ctx, first_init ? ecb_cipher : nullptr, nullptr,
                                reinterpret_cast<const unsigned char *>(key_value.data()), nullptr) != 1)
                            onError("EVP_DecryptInit_ex");
                        /// The context consumes raw blocks; PKCS#7 padding is validated manually below.
                        /// Key-only re-initialization keeps the setting.
                        if (first_init && EVP_CIPHER_CTX_set_padding(ecb_ctx, 0) != 1)
                            onError("EVP_CIPHER_CTX_set_padding");
                        ecb_key_state.remember(key_value);
                    }

                    int output_len = 0;
                    if (EVP_DecryptUpdate(ecb_ctx,
                            reinterpret_cast<unsigned char *>(decrypted), &output_len,
                            input, static_cast<int>(input_size)) != 1)
                        onError("EVP_DecryptUpdate");
                    chassert(static_cast<size_t>(output_len) == input_size);
                    __msan_unpoison(decrypted, output_len); /// OpenSSL uses assembly which evades msan's analysis

                    if (is_cbc)
                    {
                        /// Undo the chaining: the first block is XORed with the IV,
                        /// every following block with the previous ciphertext block.
                        for (size_t i = 0; i < block_size; ++i)
                            decrypted[i] ^= iv_ptr[i];
                        for (size_t i = block_size; i < input_size; ++i)
                            decrypted[i] ^= input[i - block_size];
                    }

                    /// Constant-time PKCS#7 validation over the last block, like
                    /// EVP_DecryptFinal_ex: every byte of the block participates and
                    /// nothing branches on the decrypted data, so invalid padding is
                    /// not distinguishable by timing.
                    const auto * last_block = decrypted + input_size - block_size;
                    const UInt8 pad = last_block[block_size - 1];
                    UInt8 padding_good = constantTimeGE(block_size, pad) & constantTimeGE(pad, 1);
                    for (size_t i = 0; i < block_size; ++i)
                    {
                        /// Byte i of the last block is a padding byte iff pad >= block_size - i.
                        const UInt8 is_pad_byte = constantTimeGE(pad, static_cast<UInt8>(block_size - i));
                        padding_good &= static_cast<UInt8>(~is_pad_byte) | constantTimeEQ(last_block[i], pad);
                    }
                    const bool padding_valid = padding_good != 0;

                    if (!padding_valid)
                    {
                        if constexpr (!use_null_when_decrypt_fail)
                            throw Exception(ErrorCodes::OPENSSL_ERROR, "Cannot decrypt: invalid PKCS#7 padding");
                        decrypt_fail = true;
                    }
                    else
                        decrypted += input_size - pad;
                }
            }

            decrypted_result_column_offsets.push_back(decrypted - decrypted_result_column_data.data());
            if constexpr (use_null_when_decrypt_fail)
                null_map->insertValue(decrypt_fail ? 1 : 0);
        }

        /// The padding stripped from the last row leaves the buffer overallocated.
        if (!decrypted_result_column_offsets.empty() && decrypted_result_column_data.size() > decrypted_result_column_offsets.back())
            decrypted_result_column_data.resize(decrypted_result_column_offsets.back());

        decrypted_result_column->validate();
        if constexpr (use_null_when_decrypt_fail)
            return ColumnNullable::create(std::move(decrypted_result_column), std::move(null_map));
        else
            return decrypted_result_column;
    }

    template <CipherMode mode, bool use_null_when_decrypt_fail>
    static ColumnPtr doDecryptImpl(const EVP_CIPHER * evp_cipher,
        size_t input_rows_count,
        const ColumnPtr & input_column,
        const ColumnPtr & key_column,
        [[maybe_unused]] const ColumnPtr & iv_column,
        [[maybe_unused]] const ColumnPtr & aad_column)
    {
        using namespace OpenSSLDetails;

        auto evp_ctx_ptr = std::unique_ptr<EVP_CIPHER_CTX, decltype(&::EVP_CIPHER_CTX_free)>(EVP_CIPHER_CTX_new(), &EVP_CIPHER_CTX_free);
        if (!evp_ctx_ptr)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_CIPHER_CTX_new failed");
        auto * evp_ctx = evp_ctx_ptr.get();

        [[maybe_unused]] const auto block_size = static_cast<size_t>(EVP_CIPHER_block_size(evp_cipher));
        [[maybe_unused]] const auto iv_size = static_cast<size_t>(EVP_CIPHER_iv_length(evp_cipher));

        const size_t key_size = static_cast<size_t>(EVP_CIPHER_key_length(evp_cipher));
        static constexpr size_t tag_size = 16; // https://tools.ietf.org/html/rfc5116#section-5.1

        auto decrypted_result_column = ColumnString::create();
        auto null_map = ColumnUInt8::create();
        auto & decrypted_result_column_data = decrypted_result_column->getChars();
        auto & decrypted_result_column_offsets = decrypted_result_column->getOffsets();

        {
            size_t resulting_size = 0;
            for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
            {
                size_t string_size = input_column->getDataAt(row_idx).size();
                resulting_size += string_size;

                if constexpr (mode == CipherMode::RFC5116_AEAD_AES_GCM)
                {
                    if (string_size > 0)
                    {
                        if (string_size < tag_size)
                            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Encrypted data is smaller than the size of additional data for AEAD mode, cannot decrypt.");

                        resulting_size -= tag_size;
                    }
                }
            }

            decrypted_result_column_data.resize(resulting_size);
        }

        auto * decrypted = decrypted_result_column_data.data();

        KeyHolder<mode> key_holder{};
        [[maybe_unused]] CachedKeyState ctx_key_state;

        for (size_t row_idx = 0; row_idx < input_rows_count; ++row_idx)
        {
            // 0: prepare key if required
            auto key_value = key_holder.setKey(key_size, key_column->getDataAt(row_idx));
            auto iv_value = std::string_view{};
            if (iv_column)
            {
                iv_value = iv_column->getDataAt(row_idx);

                /// If the length is zero (empty string is passed) it should be treat as no IV.
                if (iv_value.empty())
                    iv_value = std::string_view{};
            }

            auto input_value = input_column->getDataAt(row_idx);

            if constexpr (mode == CipherMode::RFC5116_AEAD_AES_GCM)
            {
                if (!input_value.empty())
                {
                    // empty plaintext results in empty ciphertext + tag, means there should be at least tag_size bytes.
                    if (input_value.size() < tag_size)
                        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Encrypted data is too short: only {} bytes, "
                                "should contain at least {} bytes of a tag.",
                                input_value.size(), tag_size);
                    input_value.remove_suffix(tag_size);
                }
            }

            if constexpr (mode != CipherMode::MySQLCompatibility)
            {
                // in GCM mode IV can be of arbitrary size (>0), for other modes IV is optional.
                if (mode == CipherMode::RFC5116_AEAD_AES_GCM && iv_value.empty())
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid IV size {} != expected size {}", iv_value.size(), iv_size);
                }

                if (key_value.size() != key_size)
                {
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid key size {} != expected size {}", key_value.size(), key_size);
                }
            }

            bool decrypt_fail = false;
            /// Avoid extra work on empty ciphertext/plaintext. Always decrypt empty to empty.
            /// This makes sense for default implementation for NULLs.
            if (!input_value.empty())
            {
                // 1: Init CTX
                if constexpr (mode == CipherMode::RFC5116_AEAD_AES_GCM)
                {
                    if (EVP_DecryptInit_ex(evp_ctx, evp_cipher, nullptr, nullptr, nullptr) != 1)
                        onError("EVP_DecryptInit_ex");

                    // 1.a.1 : Set custom IV length
                    if (EVP_CIPHER_CTX_ctrl(evp_ctx, EVP_CTRL_AEAD_SET_IVLEN, safe_cast<int>(iv_value.size()), nullptr) != 1)
                        onError("EVP_CIPHER_CTX_ctrl");

                    // 1.a.1 : Init CTX with key and IV
                    if (EVP_DecryptInit_ex(evp_ctx, nullptr, nullptr,
                            reinterpret_cast<const unsigned char*>(key_value.data()),
                            reinterpret_cast<const unsigned char*>(iv_value.data())) != 1)
                        onError("EVP_DecryptInit_ex");

                    // 1.a.2: Set AAD if present
                    if (aad_column)
                    {
                        std::string_view aad_data = aad_column->getDataAt(row_idx);
                        int tmp_len = 0;
                        if (!aad_data.empty() && EVP_DecryptUpdate(evp_ctx, nullptr, &tmp_len,
                                reinterpret_cast<const unsigned char *>(aad_data.data()), safe_cast<int>(aad_data.size())) != 1)
                        onError("EVP_DecryptUpdate");
                    }
                }
                else
                {
                    // 1.b: Init CTX
                    validateIV<mode>(iv_value, iv_size);

                    /// A fresh full initialization starts from a zero IV when no IV is
                    /// given, while an IV-only re-initialization with a null IV pointer
                    /// would keep the previous row's advanced IV state, so an absent IV
                    /// is passed as an explicit zero IV.
                    static constexpr unsigned char zero_iv[EVP_MAX_IV_LENGTH]{};
                    const auto * iv_ptr = iv_value.empty() ? zero_iv : reinterpret_cast<const unsigned char *>(iv_value.data());

                    if (ctx_key_state.matches(key_value))
                    {
                        /// The context already holds the expanded key schedule for this
                        /// key: reset only the IV, skipping the cipher setup and key
                        /// schedule expansion that dominate the per-row cost in OpenSSL 3.x.
                        if (EVP_DecryptInit_ex(evp_ctx, nullptr, nullptr, nullptr, iv_ptr) != 1)
                            onError("EVP_DecryptInit_ex");
                    }
                    else
                    {
                        if (EVP_DecryptInit_ex(evp_ctx, ctx_key_state.hasContext() ? nullptr : evp_cipher, nullptr,
                                reinterpret_cast<const unsigned char*>(key_value.data()), iv_ptr) != 1)
                            onError("EVP_DecryptInit_ex");
                        ctx_key_state.remember(key_value);
                    }
                }

                // 2: Feed the data to the cipher
                int output_len = 0;
                if (EVP_DecryptUpdate(evp_ctx,
                        reinterpret_cast<unsigned char*>(decrypted), &output_len,
                        reinterpret_cast<const unsigned char*>(input_value.data()), static_cast<int>(input_value.size())) != 1)
                {
                    if constexpr (!use_null_when_decrypt_fail)
                        onError("EVP_DecryptUpdate");
                    decrypt_fail = true;
                }
                else
                {
                    __msan_unpoison(decrypted, output_len); /// OpenSSL uses assembly which evades msan's analysis
                    decrypted += output_len;
                    // 3: optionally get tag from the ciphertext (RFC5116) and feed it to the context
                    if constexpr (mode == CipherMode::RFC5116_AEAD_AES_GCM)
                    {
                        void * tag = const_cast<void *>(reinterpret_cast<const void *>(input_value.data() + input_value.size()));
                        if (EVP_CIPHER_CTX_ctrl(evp_ctx, EVP_CTRL_AEAD_SET_TAG, tag_size, tag) != 1)
                            onError("EVP_CIPHER_CTX_ctrl");
                    }

                    // 4: retrieve encrypted data (ciphertext)
                    if (!decrypt_fail && EVP_DecryptFinal_ex(evp_ctx, reinterpret_cast<unsigned char*>(decrypted), &output_len) != 1)
                    {
                        if constexpr (!use_null_when_decrypt_fail)
                            onError("EVP_DecryptFinal_ex");
                        decrypt_fail = true;
                    }
                    else
                    {
                        __msan_unpoison(decrypted, output_len); /// OpenSSL uses assembly which evades msan's analysis
                        decrypted += output_len;
                    }
                }
            }

            /// A failed EVP_DecryptUpdate / EVP_DecryptFinal_ex can leave buffered input
            /// in the context, so force a full re-initialization on the next row.
            if (decrypt_fail)
                ctx_key_state.reset();

            decrypted_result_column_offsets.push_back(decrypted - decrypted_result_column_data.data());
            if constexpr (use_null_when_decrypt_fail)
            {
                if (decrypt_fail)
                    null_map->insertValue(1);
                else
                    null_map->insertValue(0);
            }

        }

        // in case we overestimate buffer required for decrypted data, fix it up.
        if (!decrypted_result_column_offsets.empty() && decrypted_result_column_data.size() > decrypted_result_column_offsets.back())
        {
            decrypted_result_column_data.resize(decrypted_result_column_offsets.back());
        }

        decrypted_result_column->validate();
        if constexpr (use_null_when_decrypt_fail)
            return ColumnNullable::create(std::move(decrypted_result_column), std::move(null_map));
        else
            return decrypted_result_column;
    }
};

}


#endif
