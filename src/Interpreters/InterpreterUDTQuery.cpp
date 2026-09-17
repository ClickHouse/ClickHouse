#include <Interpreters/InterpreterUDTQuery.h>

#include <Access/Common/AccessFlags.h>
#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Core/Settings.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <Databases/IDatabase.h>
#include <Databases/UDT/ILifecycleAdapter.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/ProcessList.h>
#include <Interpreters/UDTLifecycleIntrospection.h>
#include <Interpreters/UDTLifecycleRequest.h>
#include <Interpreters/formatWithPossiblyHidingSecrets.h>
#include <Parsers/ASTAlterTypeCommentQuery.h>
#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTDescribeTypeQuery.h>
#include <Parsers/ASTDropTypeQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTPhysicalizeTypeReferencesQuery.h>
#include <Parsers/ASTRenameTypeQuery.h>
#include <Parsers/ASTShowCreateTypeQuery.h>
#include <Parsers/ASTShowTypesQuery.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Storages/StorageView.h>
#include <Common/Base64.h>
#include <Common/Exception.h>
#include <Common/FailPoint.h>
#include <Common/quoteString.h>

#include <boost/algorithm/string/predicate.hpp>

#include <algorithm>
#include <chrono>
#include <exception>
#include <memory>
#include <optional>
#include <span>
#include <utility>
#include <vector>


namespace DB
{
namespace Setting
{
extern const SettingsBool allow_experimental_user_defined_types;
extern const SettingsSeconds lock_acquire_timeout;
}

namespace FailPoints
{
extern const char udt_lifecycle_pause_after_database_lookup[];
}

namespace ErrorCodes
{
extern const int ACCESS_DENIED;
extern const int BAD_ARGUMENTS;
extern const int CORRUPTED_DATA;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int SUPPORT_IS_DISABLED;
extern const int UNKNOWN_TYPE;
}

namespace
{

BlockIO oneStringColumn(String column_name, std::vector<String> values)
{
    MutableColumnPtr column = ColumnString::create();
    column->reserve(values.size());
    for (auto & value : values)
        column->insert(std::move(value));

    Block sample{{ColumnString::create(), std::make_shared<DataTypeString>(), std::move(column_name)}};
    MutableColumns columns;
    columns.emplace_back(std::move(column));
    const std::size_t rows = columns.front()->size();

    BlockIO result;
    result.pipeline = QueryPipeline(
        std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(sample)), Chunk(std::move(columns), rows)));
    return result;
}

BlockIO twoStringColumns(String first_name, String second_name, const UDT::DescribeRows & rows)
{
    MutableColumnPtr first = ColumnString::create();
    MutableColumnPtr second = ColumnString::create();
    first->reserve(rows.size());
    second->reserve(rows.size());
    for (const auto & [property, value] : rows)
    {
        first->insert(property);
        second->insert(value);
    }

    Block sample{
        {ColumnString::create(), std::make_shared<DataTypeString>(), std::move(first_name)},
        {ColumnString::create(), std::make_shared<DataTypeString>(), std::move(second_name)},
    };
    MutableColumns columns;
    columns.emplace_back(std::move(first));
    columns.emplace_back(std::move(second));
    const std::size_t row_count = columns.front()->size();

    BlockIO result;
    result.pipeline = QueryPipeline(
        std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(sample)), Chunk(std::move(columns), row_count)));
    return result;
}

String lowerHex(std::span<const UDT::CanonicalByte> bytes)
{
    static constexpr char digits[] = "0123456789abcdef";
    String result(bytes.size() * 2, '\0');
    for (size_t index = 0; index < bytes.size(); ++index)
    {
        result[2 * index] = digits[bytes[index] >> 4];
        result[2 * index + 1] = digits[bytes[index] & 0x0f];
    }
    return result;
}

UDT::LifecycleActor makeActor(const ContextMutablePtr & context)
{
    const auto principal_uuid = context->getUserID();
    return {
        .principal_uuid = principal_uuid.value_or(UUIDHelpers::Nil),
        .principal_display_name = principal_uuid ? context->getUserName() : String{},
        .internal_query = context->isInternalQuery(),
    };
}

ASTPtr cloneMutationWithDatabase(const ASTPtr & query, const String & database)
{
    ASTPtr result = query->clone();
    if (auto * create = result->as<ASTCreateTypeQuery>())
        create->setDatabase(database);
    else if (auto * comment = result->as<ASTAlterTypeCommentQuery>())
        comment->setDatabase(database);
    else if (auto * rename = result->as<ASTRenameTypeQuery>())
        rename->setDatabase(database);
    else if (auto * drop = result->as<ASTDropTypeQuery>())
        drop->setDatabase(database);
    else
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Non-mutation AST reached the user-defined type mutation clone boundary");
    return result;
}

}

InterpreterUDTQuery::InterpreterUDTQuery(ASTPtr query_, ContextMutablePtr context_)
    : WithMutableContext(std::move(context_))
    , query(std::move(query_))
{
}

BlockIO InterpreterUDTQuery::execute()
{
    if (!getContext()->getSettingsRef()[Setting::allow_experimental_user_defined_types])
        throw Exception(
            ErrorCodes::SUPPORT_IS_DISABLED,
            "User-defined type lifecycle is disabled; enable allow_experimental_user_defined_types to use it");

    const auto request = UDT::classifyLifecycleRequest(*query);

    const auto & factory = DataTypeFactory::instance();
    if (factory.hasQualifiedBuiltInCollision(*query))
        throw Exception(
            ErrorCodes::BAD_ARGUMENTS, "A qualified user-defined type reference cannot use a registered built-in family or alias");

    const auto reject_reserved_name = [&](std::string_view name)
    {
        if (factory.collidesWithRegisteredFamilyOrAlias(name))
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "A user-defined type name cannot use a registered built-in family or alias");
    };

    if (const auto * create = query->as<ASTCreateTypeQuery>())
        reject_reserved_name(create->getTypeName());
    else if (const auto * drop = query->as<ASTDropTypeQuery>())
        reject_reserved_name(drop->getTypeName());
    else if (const auto * rename = query->as<ASTRenameTypeQuery>())
    {
        reject_reserved_name(rename->getTypeName());
        reject_reserved_name(rename->getNewTypeName());
    }
    else if (const auto * comment = query->as<ASTAlterTypeCommentQuery>())
        reject_reserved_name(comment->getTypeName());
    else if (const auto * show_create = query->as<ASTShowCreateTypeQuery>())
        reject_reserved_name(show_create->getTypeName());
    else if (const auto * describe = query->as<ASTDescribeTypeQuery>())
        reject_reserved_name(describe->getTypeName());

    if (!UDT::getLifecycleRequestCluster(*query).empty())
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "{} does not support ON CLUSTER", request.operation);
    if (request.requires_internal_query && !getContext()->isInternalQuery())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "ATTACH TYPE is an internal metadata/recovery form and cannot be executed as user DDL");

    const String database_name = getContext()->resolveDatabase(UDT::getLifecycleRequestDatabase(*query));
    getContext()->checkAccess(AccessFlags{request.required_access}, database_name);

    auto database = DatabaseCatalog::instance().getDatabase(database_name);
    [[maybe_unused]] DDLGuardPtr database_ddl_guard;
    if (request.mutation)
    {
        /// Deterministic coverage for the database lookup-to-lock race.
        FailPointInjection::pauseFailPoint(FailPoints::udt_lifecycle_pause_after_database_lookup);

        /// Serialize the complete durable mutation with DROP/DETACH DATABASE.
        /// Passing the resolved database also closes the lookup-to-lock window:
        /// if another database-level DDL won it, getDDLGuard fails rather than
        /// mutating the detached DatabasePtr kept alive by this interpreter.
        database_ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, "", database.get());
    }

    auto & lifecycle = database->getUDTLifecycleAdapter();
    lifecycle.requireCapabilities(UDT::typeAuthorityCapabilityBit(UDT::TypeAuthorityCapability::DurableAlias), request.operation);

    const auto actor = makeActor(getContext());
    switch (request.kind)
    {
        case UDT::LifecycleQueryKind::Create:
        case UDT::LifecycleQueryKind::Attach: {
            ASTPtr qualified = cloneMutationWithDatabase(query, database_name);
            lifecycle.createOrAttach(qualified->as<ASTCreateTypeQuery &>(), actor);
            return {};
        }
        case UDT::LifecycleQueryKind::Rename: {
            ASTPtr qualified = cloneMutationWithDatabase(query, database_name);
            lifecycle.rename(qualified->as<ASTRenameTypeQuery &>(), actor);
            return {};
        }
        case UDT::LifecycleQueryKind::Comment: {
            ASTPtr qualified = cloneMutationWithDatabase(query, database_name);
            lifecycle.comment(qualified->as<ASTAlterTypeCommentQuery &>(), actor);
            return {};
        }
        case UDT::LifecycleQueryKind::DropRestrict: {
            ASTPtr qualified = cloneMutationWithDatabase(query, database_name);
            lifecycle.dropRestrict(qualified->as<ASTDropTypeQuery &>(), actor);
            return {};
        }
        case UDT::LifecycleQueryKind::ShowTypes: {
            auto snapshot = lifecycle.acquireSnapshot();
            const auto & show = query->as<ASTShowTypesQuery &>();
            std::optional<std::string_view> pattern;
            if (show.like_pattern)
            {
                const auto * literal = show.like_pattern->as<ASTLiteral>();
                if (!literal || literal->value.getType() != Field::Types::String)
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "SHOW TYPES LIKE does not contain its parser-owned String literal");
                pattern = literal->value.safeGet<String>();
            }

            const auto selected = UDT::selectRecordsForShow(snapshot->getDefinitionRecords(), pattern);
            std::vector<String> names;
            names.reserve(selected.size());
            for (const auto * record : selected)
                names.push_back(record->normalized_local_name);
            return oneStringColumn("name", std::move(names));
        }
        case UDT::LifecycleQueryKind::ShowCreate: {
            auto snapshot = lifecycle.acquireSnapshot();
            const String local_name = UDT::getLifecycleRequestLocalName(*query);
            const auto * record = snapshot->findDefinitionRecordByLocalName(local_name);
            if (!record)
                throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown user-defined type {}.{}", database_name, local_name);
            const ASTPtr create = UDT::makeShowCreateTypeQuery(*record);
            return oneStringColumn("statement", {format({.ctx = getContext(), .query = *create, .one_line = false})});
        }
        case UDT::LifecycleQueryKind::Describe: {
            auto snapshot = lifecycle.acquireSnapshot();
            const String local_name = UDT::getLifecycleRequestLocalName(*query);
            const auto * record = snapshot->findDefinitionRecordByLocalName(local_name);
            if (!record)
                throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown user-defined type {}.{}", database_name, local_name);
            return twoStringColumns(
                "property", "value", UDT::makeDescribeTypeRows(database_name, *record, snapshot->getDefinitionStatus(record->identity)));
        }
        case UDT::LifecycleQueryKind::DeferredPhysicalization: break;
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Unhandled user-defined type lifecycle request");
}

void registerInterpreterUDTQuery(InterpreterFactory & factory);
void registerInterpreterUDTQuery(InterpreterFactory & factory)
{
    factory.registerInterpreter(
        "InterpreterUDTQuery",
        [](const InterpreterFactory::Arguments & arguments)
        { return std::make_unique<InterpreterUDTQuery>(arguments.query, arguments.context); });
}

}
