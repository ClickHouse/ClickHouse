#include <Databases/DataLake/IcebergCatalog/Models/IcebergRestTable.h>

#if USE_AVRO

#include <Common/Exception.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Object.h>
#include <Poco/JSON/Parser.h>
#include <Poco/JSON/Stringifier.h>
#include <Poco/URI.h>
#include <sstream>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DataLake::IcebergRestModels
{

TableIdentifiersPage parseTableIdentifiersPage(
    const std::string & json,
    const std::string & base_namespace,
    size_t limit)
{
    TableIdentifiersPage result;

    if (json.empty())
        return result;

    Poco::JSON::Parser parser;
    Poco::Dynamic::Var parsed = parser.parse(json);
    const auto & object = parsed.extract<Poco::JSON::Object::Ptr>();

    auto identifiers_object = object->get("identifiers").extract<Poco::JSON::Array::Ptr>();
    if (!identifiers_object)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot parse table identifiers result");

    for (size_t i = 0; i < identifiers_object->size(); ++i)
    {
        const auto current_table_json = identifiers_object->get(static_cast<int>(i)).extract<Poco::JSON::Object::Ptr>();
        const auto table_name_raw = current_table_json->get("name").extract<std::string>();
        std::string table_name;
        Poco::URI::encode(table_name_raw, "/", table_name);

        result.tables.push_back(base_namespace + "." + table_name);
        if (limit && result.tables.size() >= limit)
            break;
    }

    if (object->has("next-page-token") && !object->isNull("next-page-token"))
        result.next_page_token = object->get("next-page-token").extract<std::string>();

    return result;
}

LoadTableResponse parseLoadTableResponse(const std::string & json)
{
    Poco::JSON::Parser parser;
    Poco::Dynamic::Var parsed = parser.parse(json);
    const auto & object = parsed.extract<Poco::JSON::Object::Ptr>();

    LoadTableResponse result;
    result.metadata = object->get("metadata").extract<Poco::JSON::Object::Ptr>();
    if (!result.metadata)
        throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot parse load table result: missing metadata");

    if (object->has("config") && !object->get("config").isEmpty())
        result.config = object->get("config").extract<Poco::JSON::Object::Ptr>();

    if (object->has("metadata-location") && !object->get("metadata-location").isEmpty())
        result.metadata_location = object->get("metadata-location").extract<std::string>();

    if (result.metadata->has("table-uuid"))
        result.table_uuid = result.metadata->get("table-uuid").extract<std::string>();

    return result;
}

Poco::JSON::Object::Ptr buildCreateTableRequest(
    const std::string & table_name,
    Poco::JSON::Object::Ptr metadata_content,
    bool include_location)
{
    Poco::JSON::Object::Ptr request_body = new Poco::JSON::Object;
    request_body->set("name", table_name);
    if (include_location)
        request_body->set("location", metadata_content->getValue<std::string>("location"));

    {
        Poco::JSON::Object::Ptr initial_schema = metadata_content->getArray("schemas")->getObject(0);
        Poco::JSON::Array::Ptr identifier_fields = new Poco::JSON::Array;
        initial_schema->set("identifier-field-ids", identifier_fields);
        request_body->set("schema", initial_schema);
    }
    request_body->set("partition-spec", metadata_content->getArray("partition-specs")->get(0));

    {
        Poco::JSON::Object::Ptr write_order = new Poco::JSON::Object;
        write_order->set("order-id", 0);
        Poco::JSON::Array::Ptr fields = new Poco::JSON::Array;
        write_order->set("fields", fields);
        request_body->set("write-order", write_order);
    }
    request_body->set("stage-create", false);

    Poco::JSON::Object::Ptr properties = new Poco::JSON::Object;
    if (metadata_content->has("format-version"))
        properties->set("format-version", std::to_string(metadata_content->getValue<int>("format-version")));
    request_body->set("properties", properties);

    return request_body;
}

Poco::JSON::Object::Ptr buildUpdateMetadataRequest(
    const std::string & namespace_name,
    const std::string & table_name,
    Poco::JSON::Object::Ptr new_snapshot)
{
    Poco::JSON::Object::Ptr request_body = new Poco::JSON::Object;
    {
        Poco::JSON::Object::Ptr identifier = new Poco::JSON::Object;
        identifier->set("name", table_name);
        Poco::JSON::Array::Ptr namespaces = new Poco::JSON::Array;
        namespaces->add(namespace_name);
        identifier->set("namespace", namespaces);
        request_body->set("identifier", identifier);
    }

    {
        Poco::JSON::Object::Ptr requirement = new Poco::JSON::Object;
        requirement->set("type", "assert-ref-snapshot-id");
        requirement->set("ref", "main");

        if (new_snapshot->has("parent-snapshot-id"))
        {
            auto parent_snapshot_id = new_snapshot->getValue<Int64>("parent-snapshot-id");
            if (parent_snapshot_id != -1)
                requirement->set("snapshot-id", parent_snapshot_id);
        }

        Poco::JSON::Array::Ptr requirements = new Poco::JSON::Array;
        requirements->add(requirement);
        request_body->set("requirements", requirements);
    }

    {
        Poco::JSON::Array::Ptr updates = new Poco::JSON::Array;

        {
            Poco::JSON::Object::Ptr add_snapshot = new Poco::JSON::Object;
            add_snapshot->set("action", "add-snapshot");
            add_snapshot->set("snapshot", new_snapshot);
            updates->add(add_snapshot);
        }

        {
            Poco::JSON::Object::Ptr set_snapshot = new Poco::JSON::Object;
            set_snapshot->set("action", "set-snapshot-ref");
            set_snapshot->set("ref-name", "main");
            set_snapshot->set("type", "branch");
            set_snapshot->set("snapshot-id", new_snapshot->getValue<Int64>("snapshot-id"));
            updates->add(set_snapshot);
        }

        request_body->set("updates", updates);
    }

    return request_body;
}

Poco::JSON::Object::Ptr buildUpdateSchemaRequest(
    const std::string & namespace_name,
    const std::string & table_name,
    Poco::JSON::Object::Ptr new_schema,
    Int32 previous_schema_id)
{
    Poco::JSON::Object::Ptr request_body = new Poco::JSON::Object;
    {
        Poco::JSON::Object::Ptr identifier = new Poco::JSON::Object;
        identifier->set("name", table_name);
        Poco::JSON::Array::Ptr namespaces = new Poco::JSON::Array;
        namespaces->add(namespace_name);
        identifier->set("namespace", namespaces);
        request_body->set("identifier", identifier);
    }

    {
        Poco::JSON::Object::Ptr requirement = new Poco::JSON::Object;
        requirement->set("type", "assert-current-schema-id");
        requirement->set("current-schema-id", previous_schema_id);

        Poco::JSON::Array::Ptr requirements = new Poco::JSON::Array;
        requirements->add(requirement);
        request_body->set("requirements", requirements);
    }

    {
        Poco::JSON::Array::Ptr updates = new Poco::JSON::Array;

        {
            Poco::JSON::Object::Ptr add_schema = new Poco::JSON::Object;
            add_schema->set("action", "add-schema");
            add_schema->set("schema", new_schema);
            updates->add(add_schema);
        }

        {
            Poco::JSON::Object::Ptr set_current_schema = new Poco::JSON::Object;
            set_current_schema->set("action", "set-current-schema");
            set_current_schema->set("schema-id", -1);
            updates->add(set_current_schema);
        }

        request_body->set("updates", updates);
    }

    return request_body;
}

std::string serializeTableIdentifiersPage(const TableIdentifiersPage & page, const std::string & base_namespace)
{
    Poco::JSON::Object::Ptr root = new Poco::JSON::Object;
    Poco::JSON::Array::Ptr identifiers = new Poco::JSON::Array;

    for (const auto & full_name : page.tables)
    {
        std::string table_name = full_name;
        if (!base_namespace.empty() && full_name.starts_with(base_namespace + "."))
            table_name = full_name.substr(base_namespace.size() + 1);

        Poco::JSON::Object::Ptr identifier = new Poco::JSON::Object;
        identifier->set("name", table_name);
        identifiers->add(identifier);
    }

    root->set("identifiers", identifiers);
    if (!page.next_page_token.empty())
        root->set("next-page-token", page.next_page_token);

    std::ostringstream oss; // STYLE_CHECK_ALLOW_STD_STRING_STREAM
    Poco::JSON::Stringifier::stringify(root, oss);
    return oss.str();
}

}

#endif
