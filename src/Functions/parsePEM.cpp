#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/ObjectUtils.h>
#include <Columns/ColumnTuple.h>
#include <Poco/Crypto/X509Certificate.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace
{

class FunctionParsePEM : public IFunction
{
public:
    static constexpr auto name = "parsePEM";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionParsePEM>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        const auto & type = arguments[0];
        if (!checkDataTypes<DataTypeString>(type.get()))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Argument for function '{}' must be String. Got '{}'",
                getName(), type->getName());

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &type, size_t /*input_rows_count*/) const override
    {
        auto column = arguments.at(0).column;

        std::istringstream str(static_cast<String>(column->getDataAt(0)));
        Poco::Crypto::X509Certificate cert(str);

        BIO * b = BIO_new(BIO_s_mem());
        std::stringstream ss;
        const X509 * x = cert.certificate();
        ss << "{\"Version\":\"" << (X509_get_version(x) + 1) << "\"";

        {
            const ASN1_INTEGER * sn =  x->cert_info->serialNumber;
            BIGNUM * bnsn = ASN1_INTEGER_to_BN(sn, nullptr);
            char buf[1024] = {0};
            if (BN_print(b, bnsn) > 0)
            {
                BIO_read(b, buf, sizeof(buf));
                ss << ",\"Serial Number\":\"" << buf << "\"";
            }
            BN_free(bnsn);
        }

        {
            const ASN1_BIT_STRING *sig = nullptr;
            const X509_ALGOR *al = nullptr;
            char buf[1024] = {0};
            X509_get0_signature(&sig, &al, x);
            if (sig && al && OBJ_obj2txt(buf, sizeof(buf), al->algorithm, 0) >= 0)
            {
                ss << ",\"Signature Algorithm\":\"" << buf << "\"";
                if (sig->data && sig->length > 0)
                {
                    ss << ",\"Signature\":\"";
                    ss << std::hex << std::setfill('0') << std::uppercase;
                    for (int i = 0; i < sig->length; ++i)
                        ss << std::setw(2) << static_cast<unsigned>(sig->data[i]);
                    ss << std::dec << std::setfill(' ') << std::nouppercase << "\"";
                }
            }
        }


        char * issuer = X509_NAME_oneline(x->cert_info->issuer, nullptr, 0);
        if (issuer)
        {
            ss << ",\"Issuer\":\"" << issuer << "\"";
            OPENSSL_free(issuer);
        }

        if (ASN1_TIME_print(b, X509_get_notBefore(x)))
        {
            char buf[1024] = {0};
            BIO_read(b, buf, sizeof(buf));
            ss << ",\"Not Before\":\"" << buf << "\"";
        }

        if (ASN1_TIME_print(b, X509_get_notAfter(x)))
        {
            char buf[1024] = {0};
            BIO_read(b, buf, sizeof(buf));
            ss << ",\"Not After\":\"" << buf << "\"";
        }

        char * subject = X509_NAME_oneline(x->cert_info->subject, nullptr, 0);
        if (subject)
        {
            ss << ",\"Subject\":\"" << subject << "\"";
            OPENSSL_free(subject);
        }

        if (X509_PUBKEY * pkey = X509_get_X509_PUBKEY(x))
        {
            ASN1_OBJECT *ppkalg = nullptr;
            const unsigned char *pk = nullptr;
            int ppklen = 0;
            X509_ALGOR *pa = nullptr;
            if (X509_PUBKEY_get0_param(&ppkalg, &pk, &ppklen, &pa, pkey))
            {
                if (i2a_ASN1_OBJECT(b, ppkalg) > 0)
                {
                    char buf[1024] = {0};
                    BIO_read(b, buf, sizeof(buf));
                    ss << ",\"Public Key Algorithm\":\"" << buf << "\"";
                }
            }
        }

        ss << "}";

        BIO_free(b);

        return type->createColumnConst(1, ss.str());
    }
};

}

void registerFunctionParsePEM(FunctionFactory & factory)
{
    factory.registerFunction<FunctionParsePEM>();
}

}
