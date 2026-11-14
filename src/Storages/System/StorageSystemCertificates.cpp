#include <Storages/System/StorageSystemCertificates.h>

#include "config.h"

#include <Columns/IColumn.h>
#include <Common/re2.h>
#include <Common/logger_useful.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <base/scope_guard.h>

#if USE_SSL
    #include <Poco/Net/SSLManager.h>
    #include <Poco/Net/SSLException.h>
    #include <Common/Crypto/X509Certificate.h>
#endif

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/constants.hpp>
#include <boost/algorithm/string/split.hpp>

#include <Poco/DateTimeFormatter.h>
#include <Poco/File.h>
#include <Poco/Util/AbstractConfiguration.h>
#include <Interpreters/Context.h>

#include <filesystem>


namespace DB
{

ColumnsDescription StorageSystemCertificates::getColumnsDescription()
{
    return ColumnsDescription
    {
        {"version",         std::make_shared<DataTypeNumber<Int32>>(), "Version of the certificate. Values are 0 for v1, 1 for v2, 2 for v3."},
        {"serial_number",   std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Serial Number of the certificate assigned by the issuer."},
        {"signature_algo",  std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Signature Algorithm - an algorithm used by the issuer to sign this certificate."},
        {"issuer",          std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Issuer - an unique identifier for the Certificate Authority issuing this certificate."},
        {"not_before",      std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "The beginning of the time window when this certificate is valid."},
        {"not_after",       std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "The end of the time window when this certificate is valid."},
        {"subject",         std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Subject - identifies the owner of the public key."},
        {"pkey_algo",       std::make_shared<DataTypeNullable>(std::make_shared<DataTypeString>()), "Public Key Algorithm defines the algorithm the public key can be used with."},
        {"path",            std::make_shared<DataTypeString>(), "Path to the file or directory containing this certificate."},
        {"default",         std::make_shared<DataTypeNumber<UInt8>>(), "Certificate is in the default certificate location."},
        {"protocol",        std::make_shared<DataTypeString>(), "Protocol name if certificate is from per-protocol TLS config, otherwise empty."}
    };
}

#if USE_SSL

static std::unordered_set<std::string> parse_dir(const std::string & dir)
{
    std::unordered_set<std::string> ret;
    boost::split(ret, dir, boost::is_any_of(":"), boost::token_compress_on);
    return ret;
}

static void populateTable(const X509Certificate & certificate, MutableColumns & res_columns, const std::string & path, bool def, const std::string & protocol)
{
    size_t col = 0;

    res_columns[col++]->insert(certificate.version());
    res_columns[col++]->insert(certificate.serialNumber());
    res_columns[col++]->insert(certificate.signatureAlgorithm());
    res_columns[col++]->insert(certificate.issuerName());
    res_columns[col++]->insert(certificate.validFrom());
    res_columns[col++]->insert(certificate.expiresOn());
    res_columns[col++]->insert(certificate.subjectName());
    res_columns[col++]->insert(certificate.publicKeyAlgorithm());

    res_columns[col++]->insert(path);
    res_columns[col++]->insert(def);
    res_columns[col++]->insert(protocol);
}

static void enumCertificates(const std::string & dir, bool def, MutableColumns & res_columns, const std::string & protocol)
{
    static const RE2 cert_name("^[a-fA-F0-9]{8}\\.\\d$");
    assert(cert_name.ok());

    const std::filesystem::path p(dir);

    for (auto const& dir_entry : std::filesystem::directory_iterator(p))
    {
        if (!dir_entry.is_regular_file() || !RE2::FullMatch(dir_entry.path().filename().string(), cert_name))
            continue;

        X509Certificate cert(dir_entry.path());
        populateTable(cert, res_columns, dir_entry.path(), def, protocol);
    }
}

#endif

void StorageSystemCertificates::fillData([[maybe_unused]] MutableColumns & res_columns, [[maybe_unused]] ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
#if USE_SSL
    auto process_ca_paths = [&](const Poco::Net::Context::CAPaths & ca_paths, const std::string & protocol_name)
    {
        if (!ca_paths.caLocation.empty())
        {
            Poco::File afile(ca_paths.caLocation);
            if (afile.exists())
            {
                if (afile.isDirectory())
                {
                    auto dir_set = parse_dir(ca_paths.caLocation);
                    for (const auto & entry : dir_set)
                        enumCertificates(entry, false, res_columns, protocol_name);
                }
                else
                {
                    auto certs = X509Certificate::fromFile(afile.path());
                    for (const auto & cert : certs)
                        populateTable(cert, res_columns, afile.path(), false, protocol_name);
                }
            }
        }

        if (!ca_paths.caDefaultDir.empty())
        {
            auto dir_set = parse_dir(ca_paths.caDefaultDir);
            for (const auto & entry : dir_set)
                enumCertificates(entry, true, res_columns, protocol_name);
        }

        if (!ca_paths.caDefaultFile.empty())
        {
            Poco::File afile(ca_paths.caDefaultFile);
            if (afile.exists())
            {
                auto certs = X509Certificate::fromFile(ca_paths.caDefaultFile);
                for (const auto & cert : certs)
                    populateTable(cert, res_columns, ca_paths.caDefaultFile, true, protocol_name);
            }
        }
    };

    const auto & config = Context::getGlobalContextInstance()->getConfigRef();

    try
    {
        const auto & ca_paths = Poco::Net::SSLManager::instance().defaultServerContext()->getCAPaths();
        process_ca_paths(ca_paths, "");
    }
    catch (const Poco::Net::SSLException &)
    {
        tryLogCurrentException(__PRETTY_FUNCTION__);
    }

    Poco::Util::AbstractConfiguration::Keys protocols;
    config.keys("protocols", protocols);
    for (const auto & protocol_name : protocols)
    {
        const std::string prefix = "protocols." + protocol_name + ".";
        const std::string ca_location = config.getString(prefix + Poco::Net::SSLManager::CFG_CA_LOCATION, "");
        if (ca_location.empty())
            continue;

        if (auto ctx = Poco::Net::SSLManager::instance().getCustomServerContext(prefix))
        {
            const auto & ca_paths = ctx->getCAPaths();
            process_ca_paths(ca_paths, protocol_name);
        }
    }
#endif
}

}
