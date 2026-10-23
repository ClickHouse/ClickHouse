#pragma once

#include <base/types.h>

#include <Poco/JSON/Object.h>

namespace DB
{

/// IAM role that the server assumes to access the warehouse storage. No long-lived key is stored.
struct IcebergRESTCatalogWarehouseCredential
{
    static constexpr auto TYPE_AWS_ROLE = "aws_role";

    String role_arn;
    String external_id;
    String role_session_name;

    /// Shape of the `credential` object in the management API: `{"type": "aws_role", "role_arn": ..., ...}`.
    static IcebergRESTCatalogWarehouseCredential fromJSON(const Poco::JSON::Object & json);
    Poco::JSON::Object::Ptr toJSON() const;
};

/// Definition of a warehouse of the Iceberg REST catalog (RFC: issue #114697), stored on its Keeper root node.
struct IcebergRESTCatalogWarehouseDefinition
{
    /// Default storage prefix for tables, without trailing slashes.
    String base_location;
    IcebergRESTCatalogWarehouseCredential credential;

    /// Shape of the create request of the management API, minus `name`.
    static IcebergRESTCatalogWarehouseDefinition fromJSON(const Poco::JSON::Object & json);

    /// Data of the warehouse root node: the JSON above plus a `format` field.
    String toKeeperData() const;
    static IcebergRESTCatalogWarehouseDefinition fromKeeperData(const String & data, const String & path);
};

}
