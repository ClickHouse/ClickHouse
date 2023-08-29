#pragma once

#include <Common/config.h>

#include <Columns/ColumnMap.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/Context.h>

#if USE_SSL
    #include <openssl/x509v3.h>
    #include "Poco/Net/SSLManager.h"
    #include "Poco/Crypto/X509Certificate.h"
#endif

namespace DB
{

namespace ErrorCodes
{
    extern const int SUPPORT_IS_DISABLED;
}

// showCertificate()
class FunctionShowCertificate : public IFunction
{
public:
    static constexpr auto name = "showCertificate";

    static FunctionPtr create(ContextPtr)
    {
#if !defined(USE_SSL) || USE_SSL == 0
        throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SSL support is disabled");
#endif
        return std::make_shared<FunctionShowCertificate>();
    }

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
            if (const X509 * cert = SSL_CTX_get0_certificate(Poco::Net::SSLManager::instance().defaultServerContext()->sslContext()))
            {
                BIO * b = BIO_new(BIO_s_mem());
                SCOPE_EXIT(
                {
                    BIO_free(b);
                });

                keys->insert("version");
                values->insert(std::to_string(X509_get_version(cert) + 1));

                {
                    char buf[1024] = {0};
                    const ASN1_INTEGER * sn = X509_get0_serialNumber(cert);
                    BIGNUM * bnsn = ASN1_INTEGER_to_BN(sn, nullptr);
                    SCOPE_EXIT(
                    {
                        BN_free(bnsn);
                    });
                    if (BN_print(b, bnsn) > 0 && BIO_read(b, buf, sizeof(buf)) > 0)
                    {
                        keys->insert("serial_number");
                        values->insert(buf);
                    }

                }

                {
                    const ASN1_BIT_STRING *sig = nullptr;
                    const X509_ALGOR *al = nullptr;
                    char buf[1024] = {0};
                    X509_get0_signature(&sig, &al, cert);
                    if (al)
                    {
                        OBJ_obj2txt(buf, sizeof(buf), al->algorithm, 0);
                        keys->insert("signature_algo");
                        values->insert(buf);
                    }
                }

                char * issuer = X509_NAME_oneline(X509_get_issuer_name(cert), nullptr, 0);
                if (issuer)
                {
                    SCOPE_EXIT(
                    {
                        OPENSSL_free(issuer);
                    });
                    keys->insert("issuer");
                    values->insert(issuer);
                }

                {
                    char buf[1024] = {0};
                    if (ASN1_TIME_print(b, X509_get_notBefore(cert)) && BIO_read(b, buf, sizeof(buf)) > 0)
                    {
                        keys->insert("not_before");
                        values->insert(buf);
                    }
                }

                {
                    char buf[1024] = {0};
                    if (ASN1_TIME_print(b, X509_get_notAfter(cert)) && BIO_read(b, buf, sizeof(buf)) > 0)
                    {
                        keys->insert("not_after");
                        values->insert(buf);
                    }
                }

                char * subject = X509_NAME_oneline(X509_get_subject_name(cert), nullptr, 0);
                if (subject)
                {
                    SCOPE_EXIT(
                    {
                        OPENSSL_free(subject);
                    });
                    keys->insert("subject");
                    values->insert(subject);
                }

                if (X509_PUBKEY * pkey = X509_get_X509_PUBKEY(cert))
                {
                    char buf[1024] = {0};
                    ASN1_OBJECT *ppkalg = nullptr;
                    const unsigned char *pk = nullptr;
                    int ppklen = 0;
                    X509_ALGOR *pa = nullptr;
                    if (X509_PUBKEY_get0_param(&ppkalg, &pk, &ppklen, &pa, pkey) &&
                        i2a_ASN1_OBJECT(b, ppkalg) > 0 && BIO_read(b, buf, sizeof(buf)) > 0)
                    {
                        keys->insert("pkey_algo");
                        values->insert(buf);
                    }
                }
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
