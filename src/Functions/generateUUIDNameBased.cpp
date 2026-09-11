#include <Columns/ColumnFixedString.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/UUID.h>
#include <DataTypes/DataTypeUUID.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <base/unaligned.h>

#include "config.h"

#if USE_SSL

#    include <openssl/evp.h>
#    include <Common/OpenSSLHelpers.h>
#    include <Common/Crypto/OpenSSLInitializer.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int OPENSSL_ERROR;
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

struct UUIDv3Impl
{
    static constexpr auto name = "generateUUIDv3";
    static constexpr const EVP_MD * (*provider)() = &EVP_md5;
    static constexpr uint64_t version_mask = 0x0000000000003000ull;
};

struct UUIDv5Impl
{
    static constexpr auto name = "generateUUIDv5";
    static constexpr const EVP_MD * (*provider)() = &EVP_sha1;
    static constexpr uint64_t version_mask = 0x0000000000005000ull;
};

template <typename Impl>
class FunctionGenerateUUIDNameBased : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(ContextPtr)
    {
        return std::make_shared<FunctionGenerateUUIDNameBased>();
    }

    FunctionGenerateUUIDNameBased()
    {
        if (OpenSSLInitializer::instance().isFIPSEnabled())
            throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "Function {} is not available in FIPS mode", name);
    }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        FunctionArgumentDescriptors args{
            {"namespace", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isUUID), nullptr, "UUID"},
            {"name", static_cast<FunctionArgumentDescriptor::TypeValidator>(&isStringOrFixedString), nullptr, "String or FixedString"}
        };
        validateFunctionArguments(*this, arguments, args);

        return std::make_shared<DataTypeUUID>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * col_namespace = checkAndGetColumn<ColumnUUID>(arguments[0].column.get());
        if (!col_namespace)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}",
                arguments[0].column->getName(), getName());

        const auto & namespaces = col_namespace->getData();
        const IColumn & col_name = *arguments[1].column;

        auto col_res = ColumnVector<UUID>::create();
        typename ColumnVector<UUID>::Container & vec_to = col_res->getData();
        vec_to.resize(input_rows_count);

        using EVP_MD_CTX_ptr = std::unique_ptr<EVP_MD_CTX, decltype(&EVP_MD_CTX_free)>;

        /// A context is initialized with the digest once, then only copied for each row.
        /// This is faster than re-initializing every time (see the comment in HalfMD5Impl in FunctionsHashing.h).
        EVP_MD_CTX_ptr ctx_template(EVP_MD_CTX_new(), EVP_MD_CTX_free);
        if (!ctx_template)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_MD_CTX_new failed: {}", getOpenSSLErrors());
        if (EVP_DigestInit_ex(ctx_template.get(), Impl::provider(), nullptr) != 1)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_DigestInit_ex failed: {}", getOpenSSLErrors());

        EVP_MD_CTX_ptr ctx(EVP_MD_CTX_new(), EVP_MD_CTX_free);
        if (!ctx)
            throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_MD_CTX_new failed: {}", getOpenSSLErrors());

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            /// RFC 4122 requires the namespace UUID to be hashed in network byte order.
            char namespace_bytes[16];
            unalignedStoreBigEndian<uint64_t>(namespace_bytes, UUIDHelpers::getHighBytes(namespaces[i]));
            unalignedStoreBigEndian<uint64_t>(namespace_bytes + 8, UUIDHelpers::getLowBytes(namespaces[i]));

            std::string_view name_view = col_name.getDataAt(i).toView();

            if (EVP_MD_CTX_copy_ex(ctx.get(), ctx_template.get()) != 1)
                throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_MD_CTX_copy_ex failed: {}", getOpenSSLErrors());
            if (EVP_DigestUpdate(ctx.get(), namespace_bytes, sizeof(namespace_bytes)) != 1)
                throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_DigestUpdate failed: {}", getOpenSSLErrors());
            if (EVP_DigestUpdate(ctx.get(), name_view.data(), name_view.size()) != 1)
                throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_DigestUpdate failed: {}", getOpenSSLErrors());

            unsigned char digest[EVP_MAX_MD_SIZE];
            if (EVP_DigestFinal_ex(ctx.get(), digest, nullptr) != 1)
                throw Exception(ErrorCodes::OPENSSL_ERROR, "EVP_DigestFinal_ex failed: {}", getOpenSSLErrors());

            /// https://tools.ietf.org/html/rfc4122#section-4.3
            /// The UUID is the first 16 bytes of the digest (SHA-1 produces 20) with the version and variant bits overridden.
            UUID & uuid = vec_to[i];
            UUIDHelpers::getHighBytes(uuid) = (unalignedLoadBigEndian<uint64_t>(digest) & 0xffffffffffff0fffull) | Impl::version_mask;
            UUIDHelpers::getLowBytes(uuid) = (unalignedLoadBigEndian<uint64_t>(digest + 8) & 0x3fffffffffffffffull) | 0x8000000000000000ull;
        }

        return col_res;
    }
};

}

REGISTER_FUNCTION(GenerateUUIDNameBased)
{
    /// generateUUIDv3 documentation
    FunctionDocumentation::Description description_v3 = R"(
Generates a [version 3](https://tools.ietf.org/html/rfc4122#section-4.3) name-based [UUID](/reference/data-types/uuid) from a namespace UUID and a name.

The UUID is the MD5 hash of the concatenation of the namespace UUID (in network byte order) and the name, with the version and variant bits set accordingly.
The function is deterministic: the same namespace and name always produce the same UUID.
This is compatible with Python's `uuid.uuid3` and PostgreSQL's `uuid_generate_v3`.

The well-known namespaces from RFC 4122 Appendix C can be used, for example the DNS namespace `6ba7b810-9dad-11d1-80b4-00c04fd430c8`, or any application-defined UUID.

Prefer [`generateUUIDv5`](#generateUUIDv5), which is identical except that it uses the SHA-1 hash instead of MD5, unless compatibility with existing version 3 UUIDs is required.
    )";
    FunctionDocumentation::Syntax syntax_v3 = "generateUUIDv3(namespace, name)";
    FunctionDocumentation::Arguments arguments_v3 = {
        {"namespace", "The namespace UUID.", {"UUID"}},
        {"name", "The name within the namespace.", {"String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_v3 = {"Returns a UUIDv3.", {"UUID"}};
    FunctionDocumentation::Examples examples_v3 = {
    {
        "Usage example",
        R"(
SELECT generateUUIDv3(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'python.org') AS uuid;
        )",
        R"(
┌─uuid─────────────────────────────────┐
│ 6fa459ea-ee8a-3ca4-894e-db77e160355e │
└──────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_v3 = {26, 9};
    FunctionDocumentation::Category category_v3 = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation_v3 = {description_v3, syntax_v3, arguments_v3, {}, returned_value_v3, examples_v3, introduced_in_v3, category_v3};

    factory.registerFunction<FunctionGenerateUUIDNameBased<UUIDv3Impl>>(documentation_v3);

    /// generateUUIDv5 documentation
    FunctionDocumentation::Description description_v5 = R"(
Generates a [version 5](https://tools.ietf.org/html/rfc4122#section-4.3) name-based [UUID](/reference/data-types/uuid) from a namespace UUID and a name.

The UUID is built from the first 16 bytes of the SHA-1 hash of the concatenation of the namespace UUID (in network byte order) and the name, with the version and variant bits set accordingly.
The function is deterministic: the same namespace and name always produce the same UUID.
This is compatible with Python's `uuid.uuid5` and PostgreSQL's `uuid_generate_v5`.

The well-known namespaces from RFC 4122 Appendix C can be used, for example the DNS namespace `6ba7b810-9dad-11d1-80b4-00c04fd430c8`, or any application-defined UUID.
    )";
    FunctionDocumentation::Syntax syntax_v5 = "generateUUIDv5(namespace, name)";
    FunctionDocumentation::Arguments arguments_v5 = {
        {"namespace", "The namespace UUID.", {"UUID"}},
        {"name", "The name within the namespace.", {"String", "FixedString"}}
    };
    FunctionDocumentation::ReturnedValue returned_value_v5 = {"Returns a UUIDv5.", {"UUID"}};
    FunctionDocumentation::Examples examples_v5 = {
    {
        "Usage example",
        R"(
SELECT generateUUIDv5(toUUID('6ba7b810-9dad-11d1-80b4-00c04fd430c8'), 'python.org') AS uuid;
        )",
        R"(
┌─uuid─────────────────────────────────┐
│ 886313e1-3b8a-5372-9b90-0c9aee199e5d │
└──────────────────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in_v5 = {26, 9};
    FunctionDocumentation::Category category_v5 = FunctionDocumentation::Category::UUID;
    FunctionDocumentation documentation_v5 = {description_v5, syntax_v5, arguments_v5, {}, returned_value_v5, examples_v5, introduced_in_v5, category_v5};

    factory.registerFunction<FunctionGenerateUUIDNameBased<UUIDv5Impl>>(documentation_v5);
}

}

#endif
