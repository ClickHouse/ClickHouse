#pragma once

#include <base/types.h>
#include <Core/UUID.h>

#include <Poco/JSON/Object.h>

namespace DB
{

/// IAM role that the server assumes to access the warehouse storage.
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
    /// Identity of this incarnation of the warehouse. A warehouse dropped and created again under the same name gets a new one.
    UUID uuid = UUIDHelpers::Nil;
    /// Default storage prefix for tables, without trailing slashes.
    String base_location;
    IcebergRESTCatalogWarehouseCredential credential;

    /// Parses `{"base_location": "s3://...", "credential": {...}}`. Other keys, such as `name`, are ignored.
    static IcebergRESTCatalogWarehouseDefinition fromJSON(const Poco::JSON::Object & json);

    /// Data of the warehouse root node: the JSON above plus the `format_version` and `uuid` fields.
    String toKeeperData() const;
    static IcebergRESTCatalogWarehouseDefinition fromKeeperData(const String & data, const String & path);
};

}
