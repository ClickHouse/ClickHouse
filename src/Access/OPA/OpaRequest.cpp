#include <Access/OPA/OpaRequest.h>

#include <Common/config_version.h>

#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>

#include <sstream>


namespace DB
{

namespace
{

/// An empty field is omitted rather than sent as a blank, so that a policy can distinguish a check
/// covering a whole database from one about a table, and a check about particular columns from one
/// that is not column scoped.
Poco::JSON::Object::Ptr serializeResource(const OpaResource & resource)
{
    Poco::JSON::Object::Ptr body = new Poco::JSON::Object();
    body->set("database", resource.database);

    if (!resource.table.empty())
        body->set("table", resource.table);

    if (!resource.columns.empty())
    {
        Poco::JSON::Array::Ptr columns = new Poco::JSON::Array();
        for (const auto & column : resource.columns)
            columns->add(column);
        body->set("columns", columns);
    }

    return body;
}

}

OpaResource OpaResource::forDatabase(String database)
{
    OpaResource result;
    result.database = std::move(database);
    return result;
}

OpaResource OpaResource::forTable(String database, String table, Names columns)
{
    OpaResource result;
    result.database = std::move(database);
    result.table = std::move(table);
    result.columns = std::move(columns);
    return result;
}

String OpaRequest::serialize(const OpaRequestContext & request_context) const
{
    Poco::JSON::Array::Ptr roles = new Poco::JSON::Array();
    for (const auto & role : request_context.roles)
        roles->add(role);

    Poco::JSON::Object::Ptr context_object = new Poco::JSON::Object();
    context_object->set("user", request_context.user);
    context_object->set("roles", roles);
    context_object->set("query_id", request_context.query_id);
    context_object->set("clickhouse_version", String{VERSION_STRING});

    Poco::JSON::Array::Ptr operations_array = new Poco::JSON::Array();
    for (const auto & operation : operations)
        operations_array->add(operation);

    Poco::JSON::Object::Ptr action = new Poco::JSON::Object();
    action->set("operations", operations_array);

    if (resource)
        action->set("resource", serializeResource(*resource));

    if (!filter_resources.empty())
    {
        Poco::JSON::Array::Ptr resources = new Poco::JSON::Array();
        for (const auto & filter_resource : filter_resources)
            resources->add(serializeResource(filter_resource));
        action->set("filter_resources", resources);
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
