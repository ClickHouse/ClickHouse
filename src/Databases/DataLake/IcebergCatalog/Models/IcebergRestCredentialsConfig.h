#pragma once
#include "config.h"

#if USE_AVRO

#include <optional>
#include <string>
#include <vector>

namespace Poco
{
namespace JSON
{
    class Object;
}
}

namespace DataLake::IcebergRestModels
{

struct VendedStorageConfig
{
    std::optional<std::string> gcs_oauth2_token;
    std::optional<std::string> s3_access_key_id;
    std::optional<std::string> s3_secret_access_key;
    std::optional<std::string> s3_session_token;
    std::optional<std::string> s3_endpoint;
    std::optional<std::string> adls_sas_token;
};

VendedStorageConfig parseVendedStorageConfig(const Poco::JSON::Object::Ptr & config);

}

#endif
