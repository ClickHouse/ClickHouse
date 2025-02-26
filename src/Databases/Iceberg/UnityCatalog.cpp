#include <Databases/Iceberg/UnityCatalog.h>

#if USE_AVRO

#include <DataTypes/DataTypeNullable.h>
#include <Poco/URI.h>
#include <Poco/JSON/Array.h>
#include <Poco/JSON/Parser.h>
#include <Common/checkStackSize.h>
#include <IO/ReadHelpers.h>
#include <IO/Operators.h>
#include <Core/NamesAndTypes.h>
#include <Storages/ObjectStorage/DataLakes/DeltaLakeMetadata.h>

namespace DB::ErrorCodes
{
    extern const int ICEBERG_CATALOG_ERROR;
}

namespace DeltaLake
{

extern const auto SCHEMAS_ENDPOINT = "schemas";
extern const auto TABLES_ENDPOINT = "tables";

struct UnityCatalogFullSchemaName
{
    std::string catalog_name;
    std::string schema_name;
};

UnityCatalogFullSchemaName parseFullSchemaName(const std::string & full_name)
{
    auto first_dot = full_name.find('.');
    auto catalog_name = full_name.substr(0, first_dot);
    auto schema = full_name.substr(first_dot + 1);
    return UnityCatalogFullSchemaName{.catalog_name = catalog_name, .schema_name = schema};
}


DB::ReadWriteBufferFromHTTPPtr UnityCatalog::createReadBuffer(
    const std::string & endpoint,
    const Poco::URI::QueryParameters & params,
    const DB::HTTPHeaderEntries & headers) const
{
    const auto & context = getContext();

    Poco::URI url(base_url / endpoint);
    if (!params.empty())
        url.setQueryParameters(params);

    auto create_buffer = [&]()
    {
        return DB::BuilderRWBufferFromHTTP(url)
            .withConnectionGroup(DB::HTTPConnectionGroupType::HTTP)
            .withSettings(getContext()->getReadSettings())
            .withTimeouts(DB::ConnectionTimeouts::getHTTPTimeouts(context->getSettingsRef(), context->getServerSettings()))
            .withHostFilter(&getContext()->getRemoteHostFilter())
            .withHeaders(headers)
            .withDelayInit(false)
            .withSkipNotFound(false)
            .create(credentials);
    };

    LOG_TEST(log, "Requesting: {}", url.toString());

    return create_buffer();
}


bool UnityCatalog::empty() const
{

    auto all_schemas = getSchemas("");
    for (const auto & schema : all_schemas)
    {
        if (!getTablesForSchema(schema, 1).empty())
            return false;
    }

    return true;
}


DB::Names UnityCatalog::getTables() const
{
    DB::Names result;

    auto all_schemas = getSchemas("");
    for (const auto & schema : all_schemas)
    {
        auto schema_tables = getTablesForSchema(schema);
        result.insert(result.end(), schema_tables.begin(), schema_tables.end());
    }

    return result;
}


void UnityCatalog::getTableMetadata(
    const std::string & namespace_name,
    const std::string & table_name,
    TableMetadata & result) const
{
    if (!tryGetTableMetadata(namespace_name, table_name, result))
        throw DB::Exception(DB::ErrorCodes::ICEBERG_CATALOG_ERROR, "No response from iceberg catalog");
}


bool UnityCatalog::tryGetTableMetadata(
    const std::string & schema_name,
    const std::string & table_name,
    TableMetadata & result) const
{
    auto full_table_name = warehouse + "." + schema_name + "." + table_name;
    auto buf = createReadBuffer(std::filesystem::path{TABLES_ENDPOINT} / full_table_name);
    if (buf->eof())
        return false;

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);
    try
    {
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var json = parser.parse(json_str);
        const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();
        if (object->has("name") && object->get("name").extract<String>() == table_name)
        {
            std::string location;
            if (result.requiresLocation())
            {
                location = object->get("storage_location").extract<String>();
                result.setLocation(location);
                LOG_TEST(log, "Location for table {}: {}", table_name, location);
            }

            if (result.requiresSchema())
            {
                LOG_DEBUG(log, "Requires schema, will parse from {}", json_str);
                DB::NamesAndTypesList schema;
                auto columns_json = object->getArray("columns");

                LOG_DEBUG(log, "Columns size {}", columns_json->size());
                for (size_t i = 0; i < columns_json->size(); ++i)
                {
                    const auto column_json = columns_json->get(static_cast<int>(i)).extract<Poco::JSON::Object::Ptr>();
                    std::string name = column_json->getValue<String>("name");
                    auto is_nullable = column_json->getValue<bool>("nullable");
                    LOG_WARNING(log, "Column json {}", fmt::join(column_json->getNames(), ","));
                    auto type_json_str = column_json->get("type_json").extract<String>();
                    DB::DataTypePtr data_type;
                    if (type_json_str.starts_with("\"") && type_json_str.ends_with("\"") && !type_json_str.contains('{'))
                    {
                        type_json_str.pop_back();
                        String type_name = type_json_str.substr(1);
                        auto data_type_from_str = DB::DeltaLakeMetadata::getSimpleTypeByName(type_name);
                        data_type = is_nullable ? makeNullable(data_type_from_str) : data_type_from_str;
                    }
                    else
                    {
                        auto parsed_json_type = parser.parse(type_json_str);
                        data_type = DB::DeltaLakeMetadata::getFieldType(parsed_json_type.extract<Poco::JSON::Object::Ptr>(), "type", is_nullable);
                    }
                    schema.push_back({name, data_type});
                }

                result.setSchema(schema);
            }
            else
            {
                LOG_DEBUG(log, "Doesn't require schema");
            }

            return true;
        }
        return false;
    }
    catch (DB::Exception & e)
    {
        e.addMessage("while parsing JSON: " + json_str);
        throw;
    }
    catch (const Poco::Exception & poco_ex)
    {
        DB::Exception our_ex(DB::Exception::CreateFromPocoTag{}, poco_ex);
        our_ex.addMessage("Cannot parse json {}", json_str);
        throw our_ex;
    }
}

bool UnityCatalog::existsTable(const std::string & schema_name, const std::string & table_name) const
{
    auto buf = createReadBuffer(std::filesystem::path{TABLES_ENDPOINT} / (warehouse + "." + schema_name + "." + table_name));
    if (buf->eof())
        return false;

    String json_str;
    readJSONObjectPossiblyInvalid(json_str, *buf);
    try
    {
        Poco::JSON::Parser parser;
        Poco::Dynamic::Var json = parser.parse(json_str);
        const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();
        if (object->has("name") && object->get("name").extract<String>() == table_name)
            return true;
        return false;
    }
    catch (DB::Exception & e)
    {
        e.addMessage("while parsing JSON: " + json_str);
        throw;
    }
}

DB::Names UnityCatalog::getTablesForSchema(const std::string & schema, size_t limit) const
{
    Poco::URI::QueryParameters params;
    params.push_back({"catalog_name", warehouse});
    params.push_back({"schema_name", schema});
    params.push_back({"max_results", DB::toString(limit)});

    DB::Names tables;
    do
    {
        auto buf = createReadBuffer(TABLES_ENDPOINT, params);
        if (buf->eof())
            return {};

        String json_str;
        readJSONObjectPossiblyInvalid(json_str, *buf);

        try
        {
            Poco::JSON::Parser parser;
            Poco::Dynamic::Var json = parser.parse(json_str);
            const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

            auto tables_object = object->get("tables").extract<Poco::JSON::Array::Ptr>();
            if (!tables_object)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot parse result");

            for (size_t i = 0; i < tables_object->size(); ++i)
            {
                const auto current_table_json = tables_object->get(static_cast<int>(i)).extract<Poco::JSON::Object::Ptr>();
                const auto table_name = current_table_json->get("name").extract<String>();

                tables.push_back(schema + "." + table_name);
                if (limit && tables.size() >= limit)
                    break;
            }

            if (limit && tables.size() >= limit)
                break;

            if (object->has("next_page_token") && !object->get("next_page_token").isEmpty())
            {
                auto continuation_token = object->get("next_page_token").extract<String>();

                if (continuation_token.empty())
                    break;

                if (params.size() == 4)
                    params.pop_back();

                params.push_back({"page_token", continuation_token});
            }
            else
            {
                break;
            }
        }
        catch (DB::Exception & e)
        {
            e.addMessage("while parsing JSON: " + json_str);
            throw;
        }
    }
    while (true);

    return tables;
}

Iceberg::ICatalog::Namespaces UnityCatalog::getSchemas(const std::string & base_prefix, size_t limit) const
{
    Poco::URI::QueryParameters params;
    params.push_back({"catalog_name", warehouse});

    Iceberg::ICatalog::Namespaces schemas;
    do
    {
        auto buf = createReadBuffer(SCHEMAS_ENDPOINT, params);
        if (buf->eof())
            return {};

        String json_str;
        readJSONObjectPossiblyInvalid(json_str, *buf);

        try
        {
            Poco::JSON::Parser parser;
            Poco::Dynamic::Var json = parser.parse(json_str);
            const Poco::JSON::Object::Ptr & object = json.extract<Poco::JSON::Object::Ptr>();

            auto schemas_object = object->get("schemas").extract<Poco::JSON::Array::Ptr>();
            if (!schemas_object)
                throw DB::Exception(DB::ErrorCodes::LOGICAL_ERROR, "Cannot parse result");

            for (size_t i = 0; i < schemas_object->size(); ++i)
            {
                auto schema_info = schemas_object->get(static_cast<int>(i)).extract<Poco::JSON::Object::Ptr>();
                chassert(schema_info->get("catalog_name").extract<String>() == warehouse);
                UnityCatalogFullSchemaName schema_name = parseFullSchemaName(schema_info->get("full_name").extract<String>());
                if (schema_name.schema_name.starts_with(base_prefix))
                    schemas.push_back(schema_name.schema_name);

                if (limit && schemas.size() > limit)
                    break;
            }

            if (limit && schemas.size() > limit)
                break;

            if (object->has("next_page_token") && !object->get("next_page_token").isEmpty())
            {
                auto continuation_token = object->get("next_page_token").extract<String>();

                if (continuation_token.empty())
                    break;

                if (params.size() == 2)
                    params.pop_back();

                params.push_back({"page_token", continuation_token});
            }
            else
            {
                break;
            }
        }
        catch (DB::Exception & e)
        {
            e.addMessage("while parsing JSON: " + json_str);
            throw;
        }

    } while (true);

    return schemas;
}

UnityCatalog::UnityCatalog(
    const std::string & catalog_,
    const std::string & base_url_,
    DB::ContextPtr context_)
    : ICatalog(catalog_)
    , DB::WithContext(context_)
    , base_url(base_url_)
    , log(getLogger("UnityCatalog(" + catalog_ + ")"))
{

}

}

#endif
