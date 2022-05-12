#include <DataTypes/DataTypeString.h>
#include <Storages/System/StorageSystemCertificates.h>
#include <regex>
#include <filesystem>
#include "Poco/Util/Application.h"
#include "Poco/File.h"
//#ifdef USE_SSL
    #include <openssl/x509v3.h>
    #include "Poco/Crypto/X509Certificate.h"
//#endif

namespace DB
{

NamesAndTypesList StorageSystemCertificates::getNamesAndTypes()
{
    return
    {
        {"version", std::make_shared<DataTypeString>()},
        {"serial_number", std::make_shared<DataTypeString>()},
        {"signature_algo", std::make_shared<DataTypeString>()},
//        {"signature", std::make_shared<DataTypeString>()},
        {"issuer", std::make_shared<DataTypeString>()},
        {"not_before", std::make_shared<DataTypeString>()},
        {"not_after", std::make_shared<DataTypeString>()},
        {"subject", std::make_shared<DataTypeString>()},
        {"pkey_algo", std::make_shared<DataTypeString>()}
    };
}

#ifdef USE_SSL

static std::unordered_set<std::string> parse_dir(const std::string & dir)
{
    std::unordered_set<std::string> ret;

    if (dir.empty())
        return ret;

    size_t len;
    const char * ss;
    const char * s = dir.c_str();
    const char * p = s;
    do
    {
        if ((*p == ':') || (*p == '\0'))
        {
            ss = s;
            s = p + 1;
            len = p - ss;
            if (len == 0)
                continue;
            ret.emplace(ss, len);
        }
    } while (*p++ != '\0');

    return ret;
}

/*
static std::string hex_dump(const unsigned char *buf, size_t len)
{
    if (!buf)
        return "";
    WriteBufferFromOwnString ss;
    ss << std::hex << std::setfill('0') << std::uppercase;
    for (size_t i = 0; i < len; ++i)
        ss << std::setw(2) << static_cast<unsigned>(buf[i]);

    return ss.str();
}
*/

static void populateTable(const X509 * cert, MutableColumns & res_columns)
{
    BIO * b = BIO_new(BIO_s_mem());
    size_t col = 0;

    res_columns[col++]->insert(std::to_string(X509_get_version(cert) + 1));

    {
        const ASN1_INTEGER * sn = cert->cert_info->serialNumber;
        BIGNUM * bnsn = ASN1_INTEGER_to_BN(sn, nullptr);
        char buf[1024] = {0};
        if (BN_print(b, bnsn) > 0)
            res_columns[col++]->insert(buf);
        BN_free(bnsn);
    }


    {
        const ASN1_BIT_STRING *sig = nullptr;
        const X509_ALGOR *al = nullptr;
        char buf[1024] = {0};
        X509_get0_signature(&sig, &al, cert);
        OBJ_obj2txt(buf, sizeof(buf), al->algorithm, 0);
        res_columns[col++]->insert(buf);
        // res_columns[col++]->insert(hex_dump(sig->data, sig->length));
    }


    char * issuer = X509_NAME_oneline(cert->cert_info->issuer, nullptr, 0);
    if (issuer)
    {
        res_columns[col++]->insert(issuer);
        OPENSSL_free(issuer);
    }

    // strange artefact - we need to make this read or else next read will contane garbage
    if (ASN1_TIME_print(b, X509_get_notBefore(cert)))
    {
        char buf[1024] = {0};
        BIO_read(b, buf, sizeof(buf));
    }

    if (ASN1_TIME_print(b, X509_get_notBefore(cert)))
    {
        char buf[1024] = {0};
        BIO_read(b, buf, sizeof(buf));
        res_columns[col++]->insert(buf);
    }

    if (ASN1_TIME_print(b, X509_get_notAfter(cert)))
    {
        char buf[1024] = {0};
        BIO_read(b, buf, sizeof(buf));
        res_columns[col++]->insert(buf);
    }

    char * subject = X509_NAME_oneline(cert->cert_info->subject, nullptr, 0);
    if (subject)
    {
        res_columns[col++]->insert(subject);
        OPENSSL_free(subject);
    }

    if (X509_PUBKEY * pkey = X509_get_X509_PUBKEY(cert))
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
                res_columns[col++]->insert(buf);
            }
        }
    }

    BIO_free(b);
}

static void enumCertificates(const std::string & dir, MutableColumns & res_columns)
{
    static const std::regex cert_name("^[a-fA-F0-9]{8}\\.\\d$");

    const std::filesystem::path p(dir);

    for (auto const& dir_entry : std::filesystem::directory_iterator(p))
    {
        if (!dir_entry.is_regular_file() || !std::regex_match(dir_entry.path().filename().string(), cert_name))
            continue;

        Poco::Crypto::X509Certificate cert(dir_entry.path());
        populateTable(cert.certificate(), res_columns);
    }
}

#endif

void StorageSystemCertificates::fillData(MutableColumns & res_columns, ContextPtr/* context*/, const SelectQueryInfo &) const
{
#ifdef USE_SSL
    Poco::Util::AbstractConfiguration &conf = Poco::Util::Application::instance().config();

    std::string ca_location = conf.getString("openSSL.server.caConfig", "");
    bool load_default_cas = conf.getBool("openSSL.server.loadDefaultCAFile", true);

    if (!ca_location.empty())
    {
        Poco::File afile(ca_location);
        if (afile.exists())
        {
            if (afile.isDirectory())
            {
                auto dir_set = parse_dir(ca_location);
                for (const auto & entry : dir_set)
                    enumCertificates(entry, res_columns);
            }
            else
            {
                auto certs = Poco::Crypto::X509Certificate::readPEM(afile.path());
                for (const auto & cert : certs)
                    populateTable(cert.certificate(), res_columns);
            }
        }
    }

    if (load_default_cas)
    {
        const char * dir = getenv(X509_get_default_cert_dir_env());
        if (!dir)
            dir = X509_get_default_cert_dir();
        if (dir)
        {
            auto dir_set = parse_dir(dir);
            for (const auto & entry : dir_set)
                enumCertificates(entry, res_columns);
        }

        const char * file = getenv(X509_get_default_cert_file_env());
        if (!file)
            file = X509_get_default_cert_file();
        if (file)
        {
            Poco::File afile(file);
            if (afile.exists())
            {
                auto certs = Poco::Crypto::X509Certificate::readPEM(file);
                for (const auto & cert : certs)
                    populateTable(cert.certificate(), res_columns);
            }
        }
    }
#endif
}

}
