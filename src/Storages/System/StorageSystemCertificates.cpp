#include <Common/config.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Storages/System/StorageSystemCertificates.h>
#include <re2/re2.h>
#include <boost/algorithm/string.hpp>
#include <filesystem>
#include "Poco/File.h"
#if USE_SSL
    #include <openssl/x509v3.h>
    #include "Poco/Net/SSLManager.h"
    #include "Poco/Crypto/X509Certificate.h"
#endif

namespace DB
{

NamesAndTypesList StorageSystemCertificates::getNamesAndTypes()
{
    return
    {
        {"version",         std::make_shared<DataTypeNumber<Int32>>()},
        {"serial_number",   std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"signature_algo",  std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"issuer",          std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"not_before",      std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"not_after",       std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"subject",         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"pkey_algo",       std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>())},
        {"path",            std::make_shared<DataTypeString>()},
        {"default",         std::make_shared<DataTypeNumber<UInt8>>()}
    };
}

#if USE_SSL

static std::unordered_set<std::string> parse_dir(const std::string & dir)
{
    std::unordered_set<std::string> ret;
    boost::split(ret, dir, boost::is_any_of(":"), boost::token_compress_on);
    return ret;
}

static void populateTable(const X509 * cert, MutableColumns & res_columns, const std::string & path, bool def)
{
    BIO * b = BIO_new(BIO_s_mem());
    SCOPE_EXIT(
    {
        BIO_free(b);
    });
    size_t col = 0;

    res_columns[col++]->insert(X509_get_version(cert) + 1);

    {
        char buf[1024] = {0};
        const ASN1_INTEGER * sn = cert->cert_info->serialNumber;
        BIGNUM * bnsn = ASN1_INTEGER_to_BN(sn, nullptr);
        SCOPE_EXIT(
        {
            BN_free(bnsn);
        });
        if (BN_print(b, bnsn) > 0 && BIO_read(b, buf, sizeof(buf)) > 0)
            res_columns[col]->insert(buf);
        else
            res_columns[col]->insertDefault();
    }
    ++col;

    {
        const ASN1_BIT_STRING *sig = nullptr;
        const X509_ALGOR *al = nullptr;
        char buf[1024] = {0};
        X509_get0_signature(&sig, &al, cert);
        if (al)
        {
            OBJ_obj2txt(buf, sizeof(buf), al->algorithm, 0);
            res_columns[col]->insert(buf);
        }
        else
            res_columns[col]->insertDefault();
    }
    ++col;

    char * issuer = X509_NAME_oneline(cert->cert_info->issuer, nullptr, 0);
    if (issuer)
    {
        SCOPE_EXIT(
        {
            OPENSSL_free(issuer);
        });
        res_columns[col]->insert(issuer);
    }
    else
        res_columns[col]->insertDefault();
    ++col;

    {
        char buf[1024] = {0};
        if (ASN1_TIME_print(b, X509_get_notBefore(cert)) && BIO_read(b, buf, sizeof(buf)) > 0)
            res_columns[col]->insert(buf);
        else
            res_columns[col]->insertDefault();
    }
    ++col;

    {
        char buf[1024] = {0};
        if (ASN1_TIME_print(b, X509_get_notAfter(cert)) && BIO_read(b, buf, sizeof(buf)) > 0)
            res_columns[col]->insert(buf);
        else
            res_columns[col]->insertDefault();
    }
    ++col;

    char * subject = X509_NAME_oneline(cert->cert_info->subject, nullptr, 0);
    if (subject)
    {
        SCOPE_EXIT(
        {
            OPENSSL_free(subject);
        });
        res_columns[col]->insert(subject);
    }
    else
        res_columns[col]->insertDefault();
    ++col;

    if (X509_PUBKEY * pkey = X509_get_X509_PUBKEY(cert))
    {
        char buf[1024] = {0};
        ASN1_OBJECT *ppkalg = nullptr;
        const unsigned char *pk = nullptr;
        int ppklen = 0;
        X509_ALGOR *pa = nullptr;
        if (X509_PUBKEY_get0_param(&ppkalg, &pk, &ppklen, &pa, pkey) &&
            i2a_ASN1_OBJECT(b, ppkalg) > 0 && BIO_read(b, buf, sizeof(buf)) > 0)
                res_columns[col]->insert(buf);
        else
            res_columns[col]->insertDefault();
    }
    else
        res_columns[col]->insertDefault();
    ++col;

    res_columns[col++]->insert(path);
    res_columns[col++]->insert(def);
}

static void enumCertificates(const std::string & dir, bool def, MutableColumns & res_columns)
{
    static const RE2 cert_name("^[a-fA-F0-9]{8}\\.\\d$");
    assert(cert_name.ok());

    const std::filesystem::path p(dir);

    for (auto const& dir_entry : std::filesystem::directory_iterator(p))
    {
        if (!dir_entry.is_regular_file() || !RE2::FullMatch(dir_entry.path().filename().string(), cert_name))
            continue;

        Poco::Crypto::X509Certificate cert(dir_entry.path());
        populateTable(cert.certificate(), res_columns, dir_entry.path(), def);
    }
}

#endif

void StorageSystemCertificates::fillData([[maybe_unused]] MutableColumns & res_columns, ContextPtr/* context*/, const SelectQueryInfo &) const
{
#if USE_SSL
    const auto & ca_paths = Poco::Net::SSLManager::instance().defaultServerContext()->getCAPaths();

    if (!ca_paths.caLocation.empty())
    {
        Poco::File afile(ca_paths.caLocation);
        if (afile.exists())
        {
            if (afile.isDirectory())
            {
                auto dir_set = parse_dir(ca_paths.caLocation);
                for (const auto & entry : dir_set)
                    enumCertificates(entry, false, res_columns);
            }
            else
            {
                auto certs = Poco::Crypto::X509Certificate::readPEM(afile.path());
                for (const auto & cert : certs)
                    populateTable(cert.certificate(), res_columns, afile.path(), false);
            }
        }
    }

    if (!ca_paths.caDefaultDir.empty())
    {
        auto dir_set = parse_dir(ca_paths.caDefaultDir);
        for (const auto & entry : dir_set)
            enumCertificates(entry, true, res_columns);
    }

    if (!ca_paths.caDefaultFile.empty())
    {
        Poco::File afile(ca_paths.caDefaultFile);
        if (afile.exists())
        {
            auto certs = Poco::Crypto::X509Certificate::readPEM(ca_paths.caDefaultFile);
            for (const auto & cert : certs)
                populateTable(cert.certificate(), res_columns, ca_paths.caDefaultFile, true);
        }
    }
#endif
}

}
