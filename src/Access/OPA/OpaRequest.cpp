#include <Access/OPA/OpaRequest.h>

#include <Common/config_version.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <sstream>


namespace DB
{

namespace
{

/// Renders a resource as the single-key wrapper it occupies under `resource`, for example
/// `{"table": {"catalogName": ..., "schemaName": ..., "tableName": ..., "columns": [...]}}`.
/// The key names follow the shape a Trino policy already reads, so that an adapter only has to
/// translate the operation name and can leave the resource untouched.
Poco::JSON::Object::Ptr serializeResource(const OpaResource & resource)
{
    Poco::JSON::Object::Ptr body = new Poco::JSON::Object();
    Poco::JSON::Object::Ptr wrapper = new Poco::JSON::Object();

    switch (resource.kind)
    {
        case OpaResource::Kind::Catalog:
        {
            body->set("name", resource.name.catalog);
            wrapper->set("catalog", body);
            break;
        }
        case OpaResource::Kind::Schema:
        {
            body->set("catalogName", resource.name.catalog);
            body->set("schemaName", resource.name.schema);
            wrapper->set("schema", body);
            break;
        }
        case OpaResource::Kind::Table:
        {
            body->set("catalogName", resource.name.catalog);
            body->set("schemaName", resource.name.schema);
            body->set("tableName", resource.name.table);

            /// An operation that is not column scoped omits the key entirely rather than sending an
            /// empty array, so that a policy can tell "no columns are involved" from "these columns
            /// are involved".
            if (!resource.columns.empty())
            {
                Poco::JSON::Array::Ptr columns = new Poco::JSON::Array();
                for (const auto & column : resource.columns)
                    columns->add(column);
                body->set("columns", columns);
            }

            wrapper->set("table", body);
            break;
        }
    }

    return wrapper;
}

}

OpaResource OpaResource::forCatalog(String catalog)
{
    OpaResource result;
    result.kind = Kind::Catalog;
    result.name.catalog = std::move(catalog);
    return result;
}

OpaResource OpaResource::forSchema(OpaTableName name)
{
    OpaResource result;
    result.kind = Kind::Schema;
    result.name = std::move(name);
    return result;
}

OpaResource OpaResource::forTable(OpaTableName name, Names columns)
{
    OpaResource result;
    result.kind = Kind::Table;
    result.name = std::move(name);
    result.columns = std::move(columns);
    return result;
}

String OpaRequest::serialize(const OpaRequestContext & request_context) const
{
    Poco::JSON::Object::Ptr identity = new Poco::JSON::Object();
    identity->set("user", request_context.user);

    Poco::JSON::Array::Ptr groups = new Poco::JSON::Array();
    for (const auto & group : request_context.groups)
        groups->add(group);
    identity->set("groups", groups);

    Poco::JSON::Object::Ptr software_stack = new Poco::JSON::Object();
    software_stack->set("clickhouseVersion", String{VERSION_STRING});

    Poco::JSON::Object::Ptr context_object = new Poco::JSON::Object();
    context_object->set("identity", identity);
    context_object->set("queryId", request_context.query_id);
    context_object->set("softwareStack", software_stack);

    Poco::JSON::Object::Ptr action = new Poco::JSON::Object();
    action->set("operation", operation);

    if (resource)
        action->set("resource", serializeResource(*resource));

    if (target_resource)
        action->set("targetResource", serializeResource(*target_resource));

    if (!filter_resources.empty())
    {
        Poco::JSON::Array::Ptr resources = new Poco::JSON::Array();
        for (const auto & filter_resource : filter_resources)
            resources->add(serializeResource(filter_resource));
        action->set("filterResources", resources);
    }

    Poco::JSON::Object::Ptr input = new Poco::JSON::Object();
    input->set("context", context_object);
    input->set("action", action);

    Poco::JSON::Object::Ptr body = new Poco::JSON::Object();
    body->set("input", input);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    oss.exceptions(std::ios::failbit);
    body->stringify(oss);
    return oss.str();
}

}
