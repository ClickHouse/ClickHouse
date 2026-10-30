#pragma once

#include <base/types.h>

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
    /// With `hide_secrets`, `external_id` and `role_session_name` are shown as `[HIDDEN]`, like `SHOW CREATE TABLE` does.
    Poco::JSON::Object::Ptr toJSON(bool hide_secrets) const;
};

/// Definition of a warehouse of the Iceberg REST catalog (RFC: issue #114697), stored on its Keeper root node.
struct IcebergRESTCatalogWarehouseDefinition
{
    /// Default storage prefix for tables, without trailing slashes.
    String base_location;
    IcebergRESTCatalogWarehouseCredential credential;

    /// Parses `{"base_location": "s3://...", "credential": {...}}`. Other keys, such as `name`, are ignored.
    static IcebergRESTCatalogWarehouseDefinition fromJSON(const Poco::JSON::Object & json);
    /// Response of the management API: `{"name": ..., "base_location": ..., "credential": {...}}` with secrets hidden.
    Poco::JSON::Object::Ptr toJSON(const String & name) const;

    /// Data of the warehouse root node: the JSON above plus a `format_version` field.
    String toKeeperData() const;
    static IcebergRESTCatalogWarehouseDefinition fromKeeperData(const String & data, const String & path);
};

/// A warehouse name is a URL path segment, a Keeper node name and a SQL identifier. Allowed: `[A-Za-z0-9_-]`, 1 to 64 bytes.
void validateIcebergRESTCatalogWarehouseName(const String & name);

}
