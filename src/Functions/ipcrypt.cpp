#include "config.h"

#if USE_IPCRYPT

#include <Columns/ColumnConst.h>
#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFixedString.h>
#include <DataTypes/DataTypeIPv4andIPv6.h>
#include <DataTypes/DataTypeString.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <Common/formatIPv6.h>

#include <ipcrypt2.h>

#include <cstring>

namespace DB
{

namespace Setting
{
    extern const SettingsBool allow_experimental_ipcrypt_functions;
}

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

enum class IPCryptMode
{
    Encrypt,
    Decrypt,
    PfxEncrypt,
    PfxDecrypt,
};

template <IPCryptMode mode>
class FunctionIPCrypt : public IFunction
{
public:
    static constexpr auto name
        = mode == IPCryptMode::Encrypt      ? "ipcryptEncrypt"
        : mode == IPCryptMode::Decrypt      ? "ipcryptDecrypt"
        : mode == IPCryptMode::PfxEncrypt   ? "ipcryptPfxEncrypt"
                                            : "ipcryptPfxDecrypt";

    static constexpr bool is_pfx = (mode == IPCryptMode::PfxEncrypt || mode == IPCryptMode::PfxDecrypt);
    static constexpr size_t required_key_bytes = is_pfx ? IPCRYPT_PFX_KEYBYTES : IPCRYPT_KEYBYTES;

    static FunctionPtr create(ContextPtr context)
    {
        if (!context->getSettingsRef()[Setting::allow_experimental_ipcrypt_functions])
            throw Exception(
                ErrorCodes::SUPPORT_IS_DISABLED,
                "IP address encryption function '{}' is experimental. "
                "Set `allow_experimental_ipcrypt_functions` setting to enable it",
                name);
        return std::make_shared<FunctionIPCrypt>();
    }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto & ip_type = arguments[0];
        if (!isIPv4(ip_type) && !isIPv6(ip_type) && !isString(ip_type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument 1 of function {} must be IPv4, IPv6, or String, got {}",
                getName(), ip_type->getName());

        const auto & key_type = arguments[1];
        if (!isString(key_type) && !isFixedString(key_type))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument 2 of function {} must be String or FixedString (encryption key), got {}",
                getName(), key_type->getName());

        /// PFX modes preserve IP address class; deterministic modes return IPv6 for typed inputs, String for String input.
        if constexpr (is_pfx)
        {
            if (isIPv4(ip_type))
                return std::make_shared<DataTypeIPv4>();
            if (isIPv6(ip_type))
                return std::make_shared<DataTypeIPv6>();
            return std::make_shared<DataTypeString>();
        }
        else
        {
            if (isString(ip_type))
                return std::make_shared<DataTypeString>();
            return std::make_shared<DataTypeIPv6>();
        }
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        /// Extract constant key
        const auto & key_col = arguments[1];
        String key_str;
        if (const auto * key_const = checkAndGetColumnConst<ColumnString>(key_col.column.get()))
            key_str = key_const->getValue<String>();
        else if (const auto * key_fixed_const = checkAndGetColumnConst<ColumnFixedString>(key_col.column.get()))
            key_str = key_fixed_const->getValue<String>();
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Key argument of function {} must be a constant string", getName());

        /// Parse and validate key
        uint8_t key_bytes[required_key_bytes];
        parseKey(key_str, key_bytes);

        /// Initialize crypto context and process rows
        if constexpr (is_pfx)
        {
            IPCryptPFX ctx;
            int rc = ipcrypt_pfx_init(&ctx, key_bytes);
            if (rc != 0)
            {
                ipcrypt_pfx_deinit(&ctx);
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Invalid PFX key for function {}: the two 16-byte key halves must not be identical",
                    getName());
            }
            auto result = processRows(arguments, ctx, input_rows_count);
            ipcrypt_pfx_deinit(&ctx);
            return result;
        }
        else
        {
            IPCrypt ctx;
            ipcrypt_init(&ctx, key_bytes);
            auto result = processRows(arguments, ctx, input_rows_count);
            ipcrypt_deinit(&ctx);
            return result;
        }
    }

private:
    void parseKey(const String & key_str, uint8_t * key_bytes) const
    {
        constexpr size_t hex_len = required_key_bytes * 2;

        if (key_str.size() == required_key_bytes)
        {
            memcpy(key_bytes, key_str.data(), required_key_bytes);
        }
        else if (key_str.size() == hex_len)
        {
            if (ipcrypt_key_from_hex(key_bytes, required_key_bytes, key_str.data(), hex_len) != 0)
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS,
                    "Invalid hex key for function {}. Expected {} hex characters.",
                    getName(), hex_len);
        }
        else
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Invalid key length for function {}. Expected {} raw bytes or {} hex characters, got {} bytes.",
                getName(), required_key_bytes, hex_len, key_str.size());
        }
    }

    template <typename Ctx>
    ColumnPtr processRows(const ColumnsWithTypeAndName & arguments, const Ctx & ctx, size_t input_rows_count) const
    {
        const auto & ip_col = arguments[0];

        if (const auto * col_ipv4 = checkAndGetColumn<ColumnIPv4>(ip_col.column.get()))
            return processIPv4(col_ipv4->getData(), ctx, input_rows_count);
        if (const auto * col_ipv6 = checkAndGetColumn<ColumnIPv6>(ip_col.column.get()))
            return processIPv6(col_ipv6->getData(), ctx, input_rows_count);
        if (const auto * col_str = checkAndGetColumn<ColumnString>(ip_col.column.get()))
            return processString(*col_str, ctx, input_rows_count);

        throw Exception(
            ErrorCodes::ILLEGAL_COLUMN,
            "Illegal column {} of argument 1 of function {}",
            ip_col.column->getName(), getName());
    }

    static void ipv4ToMappedIP16(UInt32 ipv4_host, uint8_t buf[16])
    {
        memset(buf, 0, 10);
        buf[10] = 0xFF;
        buf[11] = 0xFF;
        buf[12] = static_cast<uint8_t>(ipv4_host >> 24);
        buf[13] = static_cast<uint8_t>(ipv4_host >> 16);
        buf[14] = static_cast<uint8_t>(ipv4_host >> 8);
        buf[15] = static_cast<uint8_t>(ipv4_host);
    }

    static bool isIPv4Mapped(const uint8_t buf[16])
    {
        static const uint8_t ipv4_mapped_prefix[12] = {0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xFF, 0xFF};
        return memcmp(buf, ipv4_mapped_prefix, 12) == 0;
    }

    static UInt32 ip16ToIPv4(const uint8_t buf[16])
    {
        return (static_cast<UInt32>(buf[12]) << 24)
             | (static_cast<UInt32>(buf[13]) << 16)
             | (static_cast<UInt32>(buf[14]) << 8)
             |  static_cast<UInt32>(buf[15]);
    }

    /// Parse a string to an ip16 buffer. Returns true if IPv4-mapped, false if native IPv6.
    static bool parseStringToIP16(const char * src, size_t str_len, uint8_t buf[16])
    {
        const char * pos = src;
        const char * end = src + str_len;
        if (!parseIPv6orIPv4(pos, [&]() { return pos >= end; }, reinterpret_cast<unsigned char *>(buf)))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Cannot parse IP address from string '{}'",
                std::string_view(src, str_len));

        return isIPv4Mapped(buf);
    }

    template <typename Ctx>
    static ColumnPtr processIPv6(const PaddedPODArray<IPv6> & vec_in, const Ctx & ctx, size_t count)
    {
        auto col_res = ColumnIPv6::create(count);
        auto & vec_res = col_res->getData();
        for (size_t i = 0; i < count; ++i)
        {
            uint8_t buf[16];
            memcpy(buf, &vec_in[i], 16);
            applyTransform(ctx, buf);
            memcpy(&vec_res[i], buf, 16);
        }
        return col_res;
    }

    template <typename Ctx>
    ColumnPtr processIPv4(const PaddedPODArray<IPv4> & vec_in, const Ctx & ctx, size_t count) const
    {
        if constexpr (is_pfx)
        {
            auto col_res = ColumnIPv4::create(count);
            auto & vec_res = col_res->getData();
            for (size_t i = 0; i < count; ++i)
            {
                uint8_t buf[16];
                ipv4ToMappedIP16(vec_in[i].toUnderType(), buf);
                applyTransform(ctx, buf);

                if (!isIPv4Mapped(buf))
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "PFX transform on IPv4-mapped address produced non-IPv4-mapped result in function {}",
                        getName());

                vec_res[i] = IPv4(ip16ToIPv4(buf));
            }
            return col_res;
        }
        else
        {
            auto col_res = ColumnIPv6::create(count);
            auto & vec_res = col_res->getData();
            for (size_t i = 0; i < count; ++i)
            {
                uint8_t buf[16];
                ipv4ToMappedIP16(vec_in[i].toUnderType(), buf);
                applyTransform(ctx, buf);
                memcpy(&vec_res[i], buf, 16);
            }
            return col_res;
        }
    }

    template <typename Ctx>
    ColumnPtr processString(const ColumnString & col_str, const Ctx & ctx, size_t count) const
    {
        auto col_res = ColumnString::create();

        const auto & chars = col_str.getChars();
        const auto & offsets = col_str.getOffsets();
        size_t prev_offset = 0;

        for (size_t i = 0; i < count; ++i)
        {
            const char * src = reinterpret_cast<const char *>(&chars[prev_offset]);
            size_t raw_len = offsets[i] - prev_offset;
            size_t str_len = (raw_len > 0 && chars[offsets[i] - 1] == 0) ? raw_len - 1 : raw_len;

            uint8_t buf[16]{};
            [[maybe_unused]] bool was_ipv4 = parseStringToIP16(src, str_len, buf);
            applyTransform(ctx, buf);

            char tmp[IPV6_MAX_TEXT_LENGTH + 1];
            char * p = tmp;

            if constexpr (is_pfx)
            {
                if (was_ipv4)
                {
                    if (!isIPv4Mapped(buf))
                        throw Exception(
                            ErrorCodes::LOGICAL_ERROR,
                            "PFX transform on IPv4-mapped address produced non-IPv4-mapped result in function {}",
                            getName());

                    UInt32 ipv4_host = ip16ToIPv4(buf);
                    formatIPv4(reinterpret_cast<const unsigned char *>(&ipv4_host), p);
                }
                else
                {
                    formatIPv6(buf, p);
                }
            }
            else
            {
                formatIPv6(buf, p);
            }

            col_res->insertData(tmp, p - tmp);
            prev_offset = offsets[i];
        }
        return col_res;
    }

    static void applyTransform(const IPCrypt & ctx, uint8_t * buf)
    {
        if constexpr (mode == IPCryptMode::Encrypt)
            ipcrypt_encrypt_ip16(&ctx, buf);
        else if constexpr (mode == IPCryptMode::Decrypt)
            ipcrypt_decrypt_ip16(&ctx, buf);
    }

    static void applyTransform(const IPCryptPFX & ctx, uint8_t * buf)
    {
        if constexpr (mode == IPCryptMode::PfxEncrypt)
            ipcrypt_pfx_encrypt_ip16(&ctx, buf);
        else if constexpr (mode == IPCryptMode::PfxDecrypt)
            ipcrypt_pfx_decrypt_ip16(&ctx, buf);
    }
};

REGISTER_FUNCTION(IPCrypt)
{
    FunctionDocumentation::IntroducedIn introduced_in = {26, 2};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::IPAddress;

    /// ipcryptEncrypt
    {
        FunctionDocumentation::Description description = R"(
Encrypts an IP address using a 16-byte key with deterministic format-preserving encryption.
IPv4/IPv6 input returns IPv6, String input returns String.
)";
        FunctionDocumentation::Arguments arguments = {
            {"ip", "IP address to encrypt.", {"IPv4", "IPv6", "String"}},
            {"key", "16-byte encryption key (raw bytes or 32 hex characters). Must be constant.", {"String", "FixedString"}},
        };
        FunctionDocumentation::ReturnedValue returned_value
            = {"Encrypted IP address. IPv4/IPv6 input returns IPv6, String input returns String.", {"IPv6", "String"}};
        FunctionDocumentation::Examples examples = {
            {"Encrypt an IPv4 address",
             "SELECT ipcryptEncrypt(toIPv4('192.168.1.1'), unhex('00112233445566778899aabbccddeeff'))",
             ""},
        };
        factory.registerFunction<FunctionIPCrypt<IPCryptMode::Encrypt>>(
            {description, "ipcryptEncrypt(ip, key)", arguments, {}, returned_value, examples, introduced_in, category});
    }

    /// ipcryptDecrypt
    {
        FunctionDocumentation::Description description = R"(
Decrypts an IP address previously encrypted with ipcryptEncrypt using the same key.
IPv4/IPv6 input returns IPv6, String input returns String.
)";
        FunctionDocumentation::Arguments arguments = {
            {"encrypted_ip", "Encrypted IP address to decrypt.", {"IPv4", "IPv6", "String"}},
            {"key", "16-byte encryption key (raw bytes or 32 hex characters). Must be constant.", {"String", "FixedString"}},
        };
        FunctionDocumentation::ReturnedValue returned_value
            = {"Decrypted IP address. IPv4/IPv6 input returns IPv6, String input returns String.", {"IPv6", "String"}};
        FunctionDocumentation::Examples examples = {
            {"Round-trip encrypt then decrypt",
             "SELECT ipcryptDecrypt(ipcryptEncrypt(toIPv4('192.168.1.1'), unhex('00112233445566778899aabbccddeeff')), unhex('00112233445566778899aabbccddeeff'))",
             ""},
        };
        factory.registerFunction<FunctionIPCrypt<IPCryptMode::Decrypt>>(
            {description, "ipcryptDecrypt(encrypted_ip, key)", arguments, {}, returned_value, examples, introduced_in, category});
    }

    /// ipcryptPfxEncrypt
    {
        FunctionDocumentation::Description description = R"(
Encrypts an IP address using prefix-preserving encryption with a 32-byte key.
Addresses sharing a network prefix will have encrypted addresses that also share a prefix.
The return type preserves the input class: IPv4 stays IPv4, IPv6 stays IPv6.
)";
        FunctionDocumentation::Arguments arguments = {
            {"ip", "IP address to encrypt.", {"IPv4", "IPv6", "String"}},
            {"key", "32-byte encryption key (raw bytes or 64 hex characters). Must be constant.", {"String", "FixedString"}},
        };
        FunctionDocumentation::ReturnedValue returned_value
            = {"Encrypted IP address. IPv4 input returns IPv4, IPv6 returns IPv6, String returns String.", {"IPv4", "IPv6", "String"}};
        FunctionDocumentation::Examples examples = {
            {"Prefix-preserving encryption of IPv4",
             "SELECT ipcryptPfxEncrypt(toIPv4('192.168.1.1'), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100'))",
             ""},
        };
        factory.registerFunction<FunctionIPCrypt<IPCryptMode::PfxEncrypt>>(
            {description, "ipcryptPfxEncrypt(ip, key)", arguments, {}, returned_value, examples, introduced_in, category});
    }

    /// ipcryptPfxDecrypt
    {
        FunctionDocumentation::Description description = R"(
Decrypts an IP address previously encrypted with ipcryptPfxEncrypt using the same key.
The return type preserves the input class: IPv4 stays IPv4, IPv6 stays IPv6.
)";
        FunctionDocumentation::Arguments arguments = {
            {"encrypted_ip", "Encrypted IP address to decrypt.", {"IPv4", "IPv6", "String"}},
            {"key", "32-byte encryption key (raw bytes or 64 hex characters). Must be constant.", {"String", "FixedString"}},
        };
        FunctionDocumentation::ReturnedValue returned_value
            = {"Decrypted IP address. IPv4 input returns IPv4, IPv6 returns IPv6, String returns String.", {"IPv4", "IPv6", "String"}};
        FunctionDocumentation::Examples examples = {
            {"Round-trip prefix-preserving encrypt then decrypt",
             "SELECT ipcryptPfxDecrypt(ipcryptPfxEncrypt(toIPv4('192.168.1.1'), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100')), unhex('00112233445566778899aabbccddeeffffeeddccbbaa99887766554433221100'))",
             ""},
        };
        factory.registerFunction<FunctionIPCrypt<IPCryptMode::PfxDecrypt>>(
            {description, "ipcryptPfxDecrypt(encrypted_ip, key)", arguments, {}, returned_value, examples, introduced_in, category});
    }
}

}

#endif
