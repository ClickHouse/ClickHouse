#include "config.h"

#include <memory>
#include <string>

#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnTuple.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>
#include <base/scope_guard.h>

#if USE_SSL
    #include <Poco/Net/SSLManager.h>
    #include <Common/Crypto/X509Certificate.h>
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

namespace
{

// showCertificate()
class FunctionShowCertificate : public IFunction
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
            std::unique_ptr<X509Certificate> x509_cert;
            if (!certificate.empty())
                x509_cert = std::make_unique<X509Certificate>(certificate);

            if (!x509_cert)
                x509_cert = std::make_unique<X509Certificate>(SSL_CTX_get0_certificate(Poco::Net::SSLManager::instance().defaultServerContext()->sslContext()));

            if (x509_cert)
            {
                keys->insert("version");
                values->insert(x509_cert->version());

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
    factory.registerFunction<FunctionShowCertificate>();
}

}
