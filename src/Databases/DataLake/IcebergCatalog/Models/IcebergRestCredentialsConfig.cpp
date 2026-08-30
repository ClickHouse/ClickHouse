#include <Databases/DataLake/IcebergCatalog/Models/IcebergRestCredentialsConfig.h>

#if USE_AVRO

namespace DataLake::IcebergRestModels
{

namespace
{
constexpr auto gcs_token_str = "gcs.oauth2.token";
constexpr auto access_key_id_str = "s3.access-key-id";
constexpr auto secret_access_key_str = "s3.secret-access-key";
constexpr auto session_token_str = "s3.session-token";
constexpr auto storage_endpoint_str = "s3.endpoint";
}

VendedStorageConfig parseVendedStorageConfig(const Poco::JSON::Object::Ptr & config)
{
    VendedStorageConfig result;
    if (!config)
        return result;

    if (config->has(gcs_token_str))
        result.gcs_oauth2_token = config->get(gcs_token_str).extract<std::string>();

    if (config->has(access_key_id_str))
        result.s3_access_key_id = config->get(access_key_id_str).extract<std::string>();
    if (config->has(secret_access_key_str))
        result.s3_secret_access_key = config->get(secret_access_key_str).extract<std::string>();
    if (config->has(session_token_str))
        result.s3_session_token = config->get(session_token_str).extract<std::string>();
    if (config->has(storage_endpoint_str))
        result.s3_endpoint = config->get(storage_endpoint_str).extract<std::string>();

    std::vector<std::string> names;
    config->getNames(names);
    for (const auto & name : names)
    {
        if (name.starts_with("adls.sas-token."))
        {
            result.adls_sas_token = config->get(name).extract<std::string>();
            break;
        }
    }

    return result;
}

}

#endif
