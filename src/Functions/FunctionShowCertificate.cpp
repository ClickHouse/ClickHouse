#include "config.h"

#include <memory>
#include <optional>
#include <string>

#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

#if USE_SSL
    #include <Poco/Net/SSLManager.h>
    #include <Common/Crypto/X509Certificate.h>
    #include <Server/CertificateReloader.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

#if USE_SSL
/// The certificate that the server currently serves to clients.
/// It is not always the certificate of the default SSL context: when certificates are provisioned
/// dynamically (the `<acme>` configuration), that context has no certificate at all, and only
/// `CertificateReloader` knows the certificate in use.
std::optional<X509Certificate> getServerCertificate()
{
    auto served_certificate = CertificateReloader::instance().getCertificate(Poco::Net::SSLManager::CFG_SERVER_PREFIX);
    if (served_certificate)
        return served_certificate;

    X509 * context_certificate = SSL_CTX_get0_certificate(Poco::Net::SSLManager::instance().defaultServerContext()->sslContext());
    if (!context_certificate)
        return {};

    /// `SSL_CTX_get0_certificate` does not transfer the ownership, and `X509` is reference counted.
    X509_up_ref(context_certificate);
    return X509Certificate(context_certificate);
}
#endif

// showCertificate()
class FunctionShowCertificate final : public IFunction
{
public:
    static constexpr auto name = "showCertificate";

    static FunctionPtr create(ContextPtr ctx [[maybe_unused]])
    {
#if USE_SSL
        return std::make_shared<FunctionShowCertificate>(ctx->getQueryContext()->getClientInfo().certificate);
#else
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SSL support is disabled");
#endif
    }

    std::string certificate;

#if USE_SSL
    explicit FunctionShowCertificate(const std::string & certificate_ = "") : certificate(certificate_) {}
#endif

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 0; }

    /// The connection's client certificate, or the executing node's own server certificate.
    bool isDeterministic() const override { return false; }
    bool isServerConstant() const override { return true; }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override
    {
        return std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName &, const DataTypePtr &, size_t input_rows_count) const override
    {
        MutableColumnPtr keys = DataTypeString().createColumn();
        MutableColumnPtr values = DataTypeString().createColumn();
        MutableColumnPtr offsets = DataTypeNumber<IColumn::Offset>().createColumn();

        if (input_rows_count)
        {
#if USE_SSL
            std::optional<X509Certificate> x509_cert;
            if (!certificate.empty())
                x509_cert.emplace(certificate);
            else
                x509_cert = getServerCertificate();

            if (x509_cert)
            {
                keys->insert("version");
                values->insert(std::to_string(x509_cert->version()));

                keys->insert("serial_number");
                values->insert(x509_cert->serialNumber());

                keys->insert("signature_algo");
                values->insert(x509_cert->signatureAlgorithm());

                keys->insert("issuer");
                values->insert(x509_cert->issuerName());

                keys->insert("not_before");
                values->insert(x509_cert->validFrom());

                keys->insert("not_after");
                values->insert(x509_cert->expiresOn());

                keys->insert("subject");
                values->insert(x509_cert->subjectName());

                keys->insert("pkey_algo");
                values->insert(x509_cert->publicKeyAlgorithm());
            }
            offsets->insert(keys->size());
#endif
        }

        size_t sz = keys->size();

        if (sz && input_rows_count > 1)
        {
            keys->reserve(sz * input_rows_count);
            values->reserve(sz * input_rows_count);
            offsets->reserve(input_rows_count);
        }

        for (size_t i = 1; i < input_rows_count; ++i)
        {
            for (size_t j = 0; j < sz; ++j)
            {
                keys->insertFrom(*keys, j);
                values->insertFrom(*values, j);
            }
            offsets->insert(keys->size());
        }

        auto nested_column = ColumnArray::create(
            ColumnTuple::create(Columns{std::move(keys), std::move(values)}), std::move(offsets));

        return ColumnMap::create(nested_column);
    }
};

}

REGISTER_FUNCTION(ShowCertificate)
{
    FunctionDocumentation::Description description = R"(
Shows information about the current server's Secure Sockets Layer (SSL) certificate if it has been configured.
An empty map is returned if the server has no certificate, for example, when the certificate is provisioned with ACME and has not been issued yet.
See [Configuring TLS](/concepts/features/security/tls/configuring-tls) for more information on how to configure ClickHouse to use OpenSSL certificates to validate connections.
    )";
    FunctionDocumentation::Syntax syntax = "showCertificate()";
    FunctionDocumentation::Arguments arguments = {};
    FunctionDocumentation::ReturnedValue returned_value = {"Returns map of key-value pairs relating to the configured SSL certificate.", {"Map(String, String)"}};
    FunctionDocumentation::Examples examples = {{"Usage example",
        R"(
SELECT showCertificate() FORMAT LineAsString;
        )",
        R"(
{'version':'1','serial_number':'2D9071D64530052D48308473922C7ADAFA85D6C5','signature_algo':'sha256WithRSAEncryption','issuer':'/CN=marsnet.local CA','not_before':'May  7 17:01:21 2024 GMT','not_after':'May  7 17:01:21 2025 GMT','subject':'/CN=chnode1','pkey_algo':'rsaEncryption'}
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {22, 6};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::Other;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};

    factory.registerFunction<FunctionShowCertificate>(documentation);
}

}
