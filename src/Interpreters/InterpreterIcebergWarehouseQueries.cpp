#include <Interpreters/InterpreterIcebergWarehouseQueries.h>

#include <Access/Common/AccessFlags.h>
#include <Columns/ColumnString.h>
#include <Common/quoteString.h>
#include <DataTypes/DataTypeString.h>
#include <Interpreters/Context.h>
#include <Interpreters/InterpreterFactory.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Parsers/ASTAlterIcebergWarehouseQuery.h>
#include <Parsers/ASTCreateIcebergWarehouseQuery.h>
#include <Parsers/ASTDropIcebergWarehouseQuery.h>
#include <Parsers/ASTShowIcebergWarehousesQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <QueryPipeline/BlockIO.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouseDefinition.h>
#include <Server/IcebergRESTCatalog/IcebergRESTCatalogWarehouseRegistry.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_ASSIGN_ALTER;
}

namespace
{

constexpr auto BASE_LOCATION = "base_location";
constexpr auto ROLE_ARN = "role_arn";
constexpr auto EXTERNAL_ID = "external_id";
constexpr auto ROLE_SESSION_NAME = "role_session_name";

bool isCredentialSetting(std::string_view name)
{
    return name == ROLE_ARN || name == EXTERNAL_ID || name == ROLE_SESSION_NAME;
}

String getStringValue(const SettingChange & change)
{
    if (change.value.getType() != Field::Types::String)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting `{}` of an Iceberg warehouse must be a string", change.name);
    return change.value.safeGet<String>();
}

/// The settings are the fields of the management API request with the `credential` object flattened.
/// Building that request keeps one validation path for SQL and REST.
IcebergRESTCatalogWarehouseDefinition definitionFromSettings(const SettingsChanges & changes)
{
    Poco::JSON::Object::Ptr credential = new Poco::JSON::Object;
    /// `role_arn` implies the credential type.
    credential->set("type", IcebergRESTCatalogWarehouseCredential::TYPE_AWS_ROLE);

    Poco::JSON::Object json;
    for (const auto & change : changes)
    {
        if (change.name == BASE_LOCATION)
            json.set(change.name, getStringValue(change));
        else if (isCredentialSetting(change.name))
            credential->set(change.name, getStringValue(change));
        else
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown setting `{}` of an Iceberg warehouse", change.name);
    }
    json.set("credential", credential);

    return IcebergRESTCatalogWarehouseDefinition::fromJSON(json);
}

SettingsChanges settingsFromDefinition(const IcebergRESTCatalogWarehouseDefinition & definition)
{
    SettingsChanges changes;
    changes.emplace_back(BASE_LOCATION, definition.base_location);
    changes.emplace_back(ROLE_ARN, definition.credential.role_arn);
    if (!definition.credential.external_id.empty())
        changes.emplace_back(EXTERNAL_ID, definition.credential.external_id);
    if (!definition.credential.role_session_name.empty())
        changes.emplace_back(ROLE_SESSION_NAME, definition.credential.role_session_name);
    return changes;
}

}

BlockIO InterpreterCreateIcebergWarehouseQuery::execute()
{
    const auto & query = query_ptr->as<const ASTCreateIcebergWarehouseQuery &>();

    getContext()->checkAccess(AccessType::CREATE_ICEBERG_WAREHOUSE);

    IcebergRESTCatalogWarehouseDefinition::validateName(query.warehouse_name);
    auto definition = definitionFromSettings(query.changes);

    const auto registry = getContext()->getIcebergRESTCatalogWarehouseRegistry();
    if (!registry->create(query.warehouse_name, std::move(definition)) && !query.if_not_exists)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg warehouse {} already exists", backQuote(query.warehouse_name));

    return {};
}

BlockIO InterpreterAlterIcebergWarehouseQuery::execute()
{
    const auto & query = query_ptr->as<const ASTAlterIcebergWarehouseQuery &>();

    getContext()->checkAccess(AccessType::ALTER_ICEBERG_WAREHOUSE);

    for (const auto & change : query.changes)
    {
        if (!isCredentialSetting(change.name))
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Setting `{}` of an Iceberg warehouse cannot be changed. Only the credential can", change.name);
    }
    for (const auto & key : query.delete_keys)
    {
        if (key != EXTERNAL_ID && key != ROLE_SESSION_NAME)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Setting `{}` of an Iceberg warehouse cannot be deleted", key);
    }

    /// `SET` and `DELETE` apply on top of stored credential. Registry passes credential it read for its versioned write.
    const auto update = [&](const IcebergRESTCatalogWarehouseCredential & stored)
    {
        auto credential = stored.toJSON(/*hide_secrets*/ false);
        for (const auto & change : query.changes)
            credential->set(change.name, getStringValue(change));
        for (const auto & key : query.delete_keys)
            credential->remove(key);
        return IcebergRESTCatalogWarehouseCredential::fromJSON(*credential);
    };

    using Result = IcebergRESTCatalogWarehouseRegistry::ReplaceCredentialResult;
    switch (getContext()->getIcebergRESTCatalogWarehouseRegistry()->updateCredential(query.warehouse_name, update))
    {
        case Result::Replaced:
            return {};
        case Result::NotFound:
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg warehouse {} does not exist", backQuote(query.warehouse_name));
        case Result::Conflict:
            throw Exception(
                ErrorCodes::CANNOT_ASSIGN_ALTER, "Iceberg warehouse {} was changed concurrently, retry", backQuote(query.warehouse_name));
    }
    UNREACHABLE();
}

BlockIO InterpreterDropIcebergWarehouseQuery::execute()
{
    const auto & query = query_ptr->as<const ASTDropIcebergWarehouseQuery &>();

    getContext()->checkAccess(AccessType::DROP_ICEBERG_WAREHOUSE);

    using Result = IcebergRESTCatalogWarehouseRegistry::DropResult;
    switch (getContext()->getIcebergRESTCatalogWarehouseRegistry()->drop(query.warehouse_name))
    {
        case Result::Dropped:
            return {};
        case Result::NotFound:
            if (query.if_exists)
                return {};
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg warehouse {} does not exist", backQuote(query.warehouse_name));
        case Result::NotEmpty:
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS, "Iceberg warehouse {} is not empty. Drop its namespaces first", backQuote(query.warehouse_name));
    }
    UNREACHABLE();
}

BlockIO InterpreterShowIcebergWarehousesQuery::execute()
{
    getContext()->checkAccess(AccessType::SHOW_ICEBERG_WAREHOUSES);

    /// `list` returns a map, so names are sorted.
    MutableColumnPtr column = ColumnString::create();
    for (const auto & [name, definition] : getContext()->getIcebergRESTCatalogWarehouseRegistry()->list())
        column->insert(name);

    BlockIO res;
    res.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(
        std::make_shared<const Block>(Block{{std::move(column), std::make_shared<DataTypeString>(), "name"}})));
    return res;
}

BlockIO InterpreterShowCreateIcebergWarehouseQuery::execute()
{
    const auto & query = query_ptr->as<const ASTShowCreateIcebergWarehouseQuery &>();

    getContext()->checkAccess(AccessType::SHOW_ICEBERG_WAREHOUSES);

    const auto definition = getContext()->getIcebergRESTCatalogWarehouseRegistry()->tryGetDefinition(query.warehouse_name);
    if (!definition)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "Iceberg warehouse {} does not exist", backQuote(query.warehouse_name));

    auto create_query = make_intrusive<ASTCreateIcebergWarehouseQuery>();
    create_query->warehouse_name = query.warehouse_name;
    create_query->changes = settingsFromDefinition(*definition);

    /// Secrets are shown under same rules as in `SHOW CREATE TABLE`.
    MutableColumnPtr column = ColumnString::create();
    column->insert(format({.ctx = getContext(), .query = *create_query, .one_line = false}));

    BlockIO res;
    res.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(
        std::make_shared<const Block>(Block{{std::move(column), std::make_shared<DataTypeString>(), "statement"}})));
    return res;
}

void registerInterpreterIcebergWarehouseQueries(InterpreterFactory & factory);
void registerInterpreterIcebergWarehouseQueries(InterpreterFactory & factory)
{
    factory.registerInterpreter("InterpreterCreateIcebergWarehouseQuery", [](const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterCreateIcebergWarehouseQuery>(args.query, args.context);
    });
    factory.registerInterpreter("InterpreterAlterIcebergWarehouseQuery", [](const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterAlterIcebergWarehouseQuery>(args.query, args.context);
    });
    factory.registerInterpreter("InterpreterDropIcebergWarehouseQuery", [](const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterDropIcebergWarehouseQuery>(args.query, args.context);
    });
    factory.registerInterpreter("InterpreterShowIcebergWarehousesQuery", [](const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowIcebergWarehousesQuery>(args.query, args.context);
    });
    factory.registerInterpreter("InterpreterShowCreateIcebergWarehouseQuery", [](const InterpreterFactory::Arguments & args)
    {
        return std::make_unique<InterpreterShowCreateIcebergWarehouseQuery>(args.query, args.context);
    });
}

}
