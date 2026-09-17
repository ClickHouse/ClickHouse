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
#include <Databases/UDT/PhysicalizationApplyCoordinator.h>
#include <Databases/UDT/PhysicalizationTokenStore.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/InterpreterFactory.h>
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
#include <Storages/StorageMaterializedView.h>
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

UInt64 currentPhysicalizationTimeMicroseconds()
{
    const auto count = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::steady_clock::now().time_since_epoch()).count();
    if (count <= 0 || !std::in_range<UInt64>(count))
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Current monotonic time is outside the physicalization token domain");
    return static_cast<UInt64>(count);
}

std::string_view physicalizationObjectKindLabel(UDT::SchemaObjectKind kind) noexcept
{
    switch (kind)
    {
        case UDT::SchemaObjectKind::Table: return "TABLE";
        /// Ordinary and materialized views deliberately share the frozen
        /// SchemaObjectKind::View identity domain. The requested selector is
        /// checked against the exact live subtype before this plan is minted.
        case UDT::SchemaObjectKind::View: return "VIEW";
        case UDT::SchemaObjectKind::Dictionary: return "DICTIONARY";
        case UDT::SchemaObjectKind::TypeDefinition: return "TYPE";
        case UDT::SchemaObjectKind::SyntheticTestObject: return "SYNTHETIC OBJECT";
    }
    return "OBJECT";
}

BlockIO physicalizationDryRunBlock(UDT::PhysicalizationDryRunResult result)
{
    const auto & plan = result.plan;
    static constexpr size_t maximum_summary_bytes = 1ULL << 20;
    String summary;
    summary.reserve(std::min<size_t>(maximum_summary_bytes, static_cast<size_t>(plan.getManifestBytes())));
    bool summary_complete = true;
    const auto append_summary_line = [&](String line)
    {
        if (!summary_complete)
            return;
        if (line.size() > maximum_summary_bytes - summary.size())
        {
            static constexpr std::string_view marker
                = "... summary truncated; inspect canonical_loss_manifest_base64 for the complete structured result\n";
            if (marker.size() <= maximum_summary_bytes - summary.size())
                summary.append(marker);
            summary_complete = false;
            return;
        }
        summary += line;
    };
    for (const auto & object : plan.getObjects())
    {
        append_summary_line(
            String(physicalizationObjectKindLabel(object.object.kind)) + " " + backQuote(object.diagnostic_name)
            + " uuid=" + toString(object.object.object_uuid) + " revision=" + std::to_string(object.object_schema_revision)
            + " logical_occurrences=" + std::to_string(object.references.uses.size()) + "\n");
    }
    if (summary_complete)
    {
        for (const auto & definition : plan.getDefinitions())
        {
            append_summary_line(
                "TYPE " + backQuote(definition.normalized_name) + " uuid=" + toString(definition.identity.type_uuid) + " revision="
                + std::to_string(definition.identity.revision) + (definition.selected_for_drop ? " action=DROP\n" : " action=RETAIN\n"));
        }
    }

    auto apply_token = ColumnString::create();
    auto database_uuid = ColumnUUID::create();
    auto database_catalog_epoch = ColumnUInt64::create();
    auto inventory_root = ColumnString::create();
    auto scope_digest = ColumnString::create();
    auto scope_count = ColumnUInt64::create();
    auto manifest_digest = ColumnString::create();
    auto manifest_count = ColumnUInt64::create();
    auto loss_summary = ColumnString::create();
    auto canonical_loss_manifest_base64 = ColumnString::create();

    apply_token->insert(std::move(result.opaque_token));
    database_uuid->insert(plan.getDatabaseUUID());
    database_catalog_epoch->insert(plan.getDatabaseCatalogEpoch());
    inventory_root->insert(lowerHex(plan.getInventoryRoot()));
    scope_digest->insert(lowerHex(plan.getScopeDigest()));
    scope_count->insert(plan.getScopeCount());
    manifest_digest->insert(lowerHex(plan.getManifestDigest()));
    manifest_count->insert(plan.getManifestCount());
    loss_summary->insert(std::move(summary));
    canonical_loss_manifest_base64->insert(base64Encode(plan.getCanonicalManifestBytes()));

    Block block{
        {std::move(apply_token), std::make_shared<DataTypeString>(), "apply_token"},
        {std::move(database_uuid), std::make_shared<DataTypeUUID>(), "database_uuid"},
        {std::move(database_catalog_epoch), std::make_shared<DataTypeUInt64>(), "database_catalog_epoch"},
        {std::move(inventory_root), std::make_shared<DataTypeString>(), "inventory_root"},
        {std::move(scope_digest), std::make_shared<DataTypeString>(), "scope_digest"},
        {std::move(scope_count), std::make_shared<DataTypeUInt64>(), "scope_count"},
        {std::move(manifest_digest), std::make_shared<DataTypeString>(), "manifest_digest"},
        {std::move(manifest_count), std::make_shared<DataTypeUInt64>(), "manifest_count"},
        {std::move(loss_summary), std::make_shared<DataTypeString>(), "loss_summary"},
        {std::move(canonical_loss_manifest_base64), std::make_shared<DataTypeString>(), "canonical_loss_manifest_base64"},
    };

    BlockIO output;
    output.pipeline = QueryPipeline(std::make_shared<SourceFromSingleChunk>(std::make_shared<const Block>(std::move(block))));
    return output;
}

void requirePhysicalizationDatabaseVisibility(const ContextPtr & context, std::string_view database_name)
{
    if (database_name.empty() || !context->getAccess()->isGranted(AccessType::SHOW_TYPES, database_name))
    {
        throw Exception(ErrorCodes::ACCESS_DENIED, "Not enough privileges to inspect the complete PHYSICALIZE TYPE REFERENCES scope");
    }
}

class PhysicalizationAuthorization final : public UDT::IPhysicalizationDryRunAuthorization, public UDT::IPhysicalizationApplyAuthorization
{
public:
    enum class RequestedObjectKind : UInt8
    {
        Table,
        View,
        MaterializedView,
        Dictionary,
    };

    static UDT::SchemaObjectKind schemaObjectKind(RequestedObjectKind kind) noexcept
    {
        switch (kind)
        {
            case RequestedObjectKind::Table: return UDT::SchemaObjectKind::Table;
            case RequestedObjectKind::View:
            case RequestedObjectKind::MaterializedView: return UDT::SchemaObjectKind::View;
            case RequestedObjectKind::Dictionary: return UDT::SchemaObjectKind::Dictionary;
        }
        std::terminate();
    }

    PhysicalizationAuthorization(ContextMutablePtr context_, String database_name_, UUID database_uuid_)
        : context(std::move(context_))
        , database_name(std::move(database_name_))
        , database_uuid(database_uuid_)
    {
        if (database_name.empty() || database_uuid == UUIDHelpers::Nil)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Physicalization authorization received an invalid database identity");
    }

    void checkCancellation() const override
    {
        if (const auto process_list_element = context->getProcessListElementSafe())
            static_cast<void>(process_list_element->checkTimeLimit());
    }

    std::chrono::milliseconds getTableAlterLockAcquireTimeout() const override
    {
        return std::chrono::milliseconds(context->getSettingsRef()[Setting::lock_acquire_timeout].totalMilliseconds());
    }

    void requireDatabaseVisibility() const override { requirePhysicalizationDatabaseVisibility(context, database_name); }

    void requireObjectIdentityVisibility(const UDT::SchemaObjectID & object, std::string_view table_name) const override
    {
        requireStoredObjectIdentity(object, table_name);
        requireStoredObjectVisibility(object.kind, table_name);
    }

    void requireDatabaseObjectDiagnosticsVisibility() const override
    {
        const auto access = context->getAccess();
        if (!access->isGranted(AccessType::SHOW_TABLES, database_name) || !access->isGranted(AccessType::SHOW_COLUMNS, database_name)
            || !access->isGranted(AccessType::SHOW_DICTIONARIES, database_name))
            denyInspection();
    }

    void requireDatabaseDefinitionVisibility() const override
    {
        if (!context->getAccess()->isGranted(AccessType::SHOW_TYPES, database_name))
            denyInspection();
    }

    void requireObjectRewriteIdentity(const UDT::SchemaObjectID & object, std::string_view table_name) const override
    {
        requireStoredObjectIdentity(object, table_name);
        requireStoredObjectRewrite(object.kind, table_name);
    }

    void requireDatabaseObjectRewriteDiagnostics() const override
    {
        const auto access = context->getAccess();
        if (!access->isGranted(AccessType::ALTER_TABLE, database_name) || !access->isGranted(AccessType::ALTER_VIEW, database_name)
            || !access->isGranted(AccessType::CREATE_DICTIONARY | AccessType::DROP_DICTIONARY, database_name))
            denyApply();
    }

    void requireDatabaseDefinitionDrop() const override
    {
        if (!context->getAccess()->isGranted(AccessType::DROP_TYPE, database_name))
            denyApply();
    }

    void requireRequestedObjectVisibility(RequestedObjectKind kind, std::string_view object_name) const
    {
        requireStoredObjectVisibility(schemaObjectKind(kind), object_name);
    }

    StoragePtr resolveRequestedObject(const IDatabase & database, RequestedObjectKind kind, std::string_view object_name) const
    {
        requireRequestedObjectVisibility(kind, object_name);
        auto storage = database.tryGetTable(String(object_name), context);
        if (!storage || !storageMatchesRequestedKind(*storage, kind))
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown Atomic {} {}.{} for physicalization",
                requestedObjectKindLabel(kind),
                database_name,
                object_name);
        }
        const auto storage_id = storage->getStorageID();
        if (storage_id.database_name != database_name || storage_id.table_name != object_name || storage_id.uuid == UUIDHelpers::Nil)
        {
            throw Exception(
                ErrorCodes::BAD_ARGUMENTS,
                "Unknown Atomic {} {}.{} for physicalization",
                requestedObjectKindLabel(kind),
                database_name,
                object_name);
        }
        return storage;
    }

    void requireObjectVisibility(const UDT::PhysicalizationManifestObject & object) const override
    {
        requireStoredObject(object);
        requireStoredObjectVisibility(object.object.kind, object.diagnostic_name);
    }

    void requireDefinitionVisibility(const UDT::PhysicalizationManifestDefinition &) const override
    {
        if (!context->getAccess()->isGranted(AccessType::SHOW_TYPES, database_name))
            denyInspection();
    }

    void requireObjectRewrite(const UDT::PhysicalizationManifestObject & object) const override
    {
        requireStoredObject(object);
        requireStoredObjectRewrite(object.object.kind, object.diagnostic_name);
    }

    void requireDefinitionDrop(const UDT::PhysicalizationManifestDefinition &) const override
    {
        if (!context->getAccess()->isGranted(AccessType::DROP_TYPE, database_name))
            denyApply();
    }

private:
    [[noreturn]] static void denyInspection()
    {
        throw Exception(ErrorCodes::ACCESS_DENIED, "Not enough privileges to inspect the complete PHYSICALIZE TYPE REFERENCES scope");
    }

    [[noreturn]] static void denyApply()
    {
        throw Exception(ErrorCodes::ACCESS_DENIED, "Not enough privileges to apply the complete PHYSICALIZE TYPE REFERENCES plan");
    }

    static std::string_view requestedObjectKindLabel(RequestedObjectKind kind) noexcept
    {
        switch (kind)
        {
            case RequestedObjectKind::Table: return "TABLE";
            case RequestedObjectKind::View: return "VIEW";
            case RequestedObjectKind::MaterializedView: return "MATERIALIZED VIEW";
            case RequestedObjectKind::Dictionary: return "DICTIONARY";
        }
        return "OBJECT";
    }

    static bool storageMatchesRequestedKind(const IStorage & storage, RequestedObjectKind kind) noexcept
    {
        switch (kind)
        {
            case RequestedObjectKind::Table: return !storage.isView() && !storage.isDictionary();
            case RequestedObjectKind::View: return storage.as<StorageView>() != nullptr;
            case RequestedObjectKind::MaterializedView: return storage.as<StorageMaterializedView>() != nullptr;
            case RequestedObjectKind::Dictionary: return storage.isDictionary();
        }
        return false;
    }

    void requireStoredObjectVisibility(UDT::SchemaObjectKind kind, std::string_view object_name) const
    {
        const auto access = context->getAccess();
        switch (kind)
        {
            case UDT::SchemaObjectKind::Table:
            case UDT::SchemaObjectKind::View:
                if (access->isGranted(AccessType::SHOW_TABLES, database_name, object_name)
                    && access->isGranted(AccessType::SHOW_COLUMNS, database_name, object_name))
                    return;
                break;
            case UDT::SchemaObjectKind::Dictionary:
                if (access->isGranted(AccessType::SHOW_DICTIONARIES, database_name, object_name))
                    return;
                break;
            case UDT::SchemaObjectKind::TypeDefinition:
            case UDT::SchemaObjectKind::SyntheticTestObject: break;
        }
        denyInspection();
    }

    void requireStoredObjectRewrite(UDT::SchemaObjectKind kind, std::string_view object_name) const
    {
        const auto access = context->getAccess();
        switch (kind)
        {
            case UDT::SchemaObjectKind::Table:
                if (access->isGranted(AccessType::ALTER_TABLE, database_name, object_name))
                    return;
                break;
            case UDT::SchemaObjectKind::View:
                if (access->isGranted(AccessType::ALTER_VIEW, database_name, object_name))
                    return;
                break;
            case UDT::SchemaObjectKind::Dictionary:
                if (access->isGranted(AccessType::CREATE_DICTIONARY | AccessType::DROP_DICTIONARY, database_name, object_name))
                    return;
                break;
            case UDT::SchemaObjectKind::TypeDefinition:
            case UDT::SchemaObjectKind::SyntheticTestObject: break;
        }
        denyApply();
    }

    void requireStoredObjectIdentity(const UDT::SchemaObjectID & object, std::string_view object_name) const
    {
        const bool supported_kind = object.kind == UDT::SchemaObjectKind::Table || object.kind == UDT::SchemaObjectKind::View
            || object.kind == UDT::SchemaObjectKind::Dictionary;
        if (!supported_kind || object.database_uuid != database_uuid || object.object_uuid == UUIDHelpers::Nil || object_name.empty())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Physicalization authorization received an invalid stored-object identity");
    }

    void requireStoredObject(const UDT::PhysicalizationManifestObject & object) const
    {
        requireStoredObjectIdentity(object.object, object.diagnostic_name);
    }

    ContextMutablePtr context;
    String database_name;
    UUID database_uuid;
};

UDT::PhysicalizationSelector makePhysicalizationSelector(
    const ASTPhysicalizeTypeReferencesQuery & query, const IDatabase & database, const PhysicalizationAuthorization & authorization)
{
    UDT::PhysicalizationSelector selector;
    selector.drop_unused_types = query.drop_unused_types;
    switch (query.scope)
    {
        case ASTPhysicalizeTypeReferencesQuery::Scope::Object: selector.scope = UDT::PhysicalizationScope::Object; break;
        case ASTPhysicalizeTypeReferencesQuery::Scope::DependentClosure:
            selector.scope = UDT::PhysicalizationScope::DependentClosure;
            break;
        case ASTPhysicalizeTypeReferencesQuery::Scope::Database:
            if (query.getDatabase().empty())
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "PHYSICALIZE TYPE REFERENCES DATABASE requires a database name");
            selector.scope = UDT::PhysicalizationScope::Database;
            return selector;
    }

    if (!query.object_kind)
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PHYSICALIZE TYPE REFERENCES object kind is missing");
    const String object_kind = getIdentifierName(query.object_kind);
    const auto requested_kind = [&]
    {
        using Kind = PhysicalizationAuthorization::RequestedObjectKind;
        if (boost::iequals(object_kind, "TABLE"))
            return Kind::Table;
        if (boost::iequals(object_kind, "VIEW"))
            return Kind::View;
        if (boost::iequals(object_kind, "MATERIALIZED VIEW") || boost::iequals(object_kind, "MATERIALIZED_VIEW"))
            return Kind::MaterializedView;
        if (boost::iequals(object_kind, "DICTIONARY"))
            return Kind::Dictionary;
        throw Exception(
            ErrorCodes::NOT_IMPLEMENTED,
            "PHYSICALIZE TYPE REFERENCES supports only Atomic TABLE, VIEW, MATERIALIZED VIEW, and DICTIONARY objects");
    }();
    const String object_name = query.getObjectName();
    if (object_name.empty())
        throw Exception(ErrorCodes::BAD_ARGUMENTS, "PHYSICALIZE TYPE REFERENCES object name is empty");

    /// Check the requested namespace before lookup, so an unauthorized caller
    /// cannot distinguish absence from a kind mismatch. Resolve the exact live
    /// subtype before minting the shared View identity used by ordinary and
    /// materialized views in the durable graph.
    const auto storage = authorization.resolveRequestedObject(database, requested_kind, object_name);
    const auto storage_id = storage->getStorageID();
    selector.object = UDT::SchemaObjectID{
        .kind = PhysicalizationAuthorization::schemaObjectKind(requested_kind),
        .database_uuid = database.getUUID(),
        .object_uuid = storage_id.uuid,
    };
    return selector;
}

int physicalizationPlanErrorCode(UDT::PhysicalizationPlanError::Code code)
{
    using Code = UDT::PhysicalizationPlanError::Code;
    switch (code)
    {
        case Code::UnsupportedObjectKind: return ErrorCodes::NOT_IMPLEMENTED;
        case Code::IncompleteScope:
        case Code::IntegrityMismatch:
        case Code::GraphMismatch: return ErrorCodes::CORRUPTED_DATA;
        case Code::InvalidConfiguration: return ErrorCodes::LOGICAL_ERROR;
        case Code::InvalidSelector:
        case Code::ObjectNotFound:
        case Code::LimitExceeded: return ErrorCodes::BAD_ARGUMENTS;
    }
    return ErrorCodes::LOGICAL_ERROR;
}

int physicalizationMutationPlannerErrorCode(UDT::PhysicalizationMutationPlannerError::Code code)
{
    using Code = UDT::PhysicalizationMutationPlannerError::Code;
    switch (code)
    {
        case Code::ExpectedEpochMismatch:
        case Code::StalePlan:
        case Code::RemainingDependent:
        case Code::LimitExceeded: return ErrorCodes::BAD_ARGUMENTS;
        case Code::IntegrityMismatch: return ErrorCodes::CORRUPTED_DATA;
        case Code::InvalidConfiguration:
        case Code::InvalidRequest:
        case Code::DatabaseMismatch:
        case Code::InvalidPlan:
        case Code::InvalidRewriteImages:
        case Code::InvalidTransition: return ErrorCodes::LOGICAL_ERROR;
    }
    return ErrorCodes::LOGICAL_ERROR;
}

int physicalizationTokenStoreErrorCode(UDT::PhysicalizationTokenStoreError::Code code)
{
    using Code = UDT::PhysicalizationTokenStoreError::Code;
    switch (code)
    {
        case Code::InvalidPrincipal:
        case Code::InvalidPlan:
        case Code::LimitExceeded:
        case Code::TokenRejected: return ErrorCodes::BAD_ARGUMENTS;
        case Code::InvalidConfiguration:
        case Code::EntropyFailure: return ErrorCodes::LOGICAL_ERROR;
    }
    return ErrorCodes::LOGICAL_ERROR;
}

int physicalizationApplyCoordinatorErrorCode(UDT::PhysicalizationApplyCoordinatorError::Code code)
{
    using Code = UDT::PhysicalizationApplyCoordinatorError::Code;
    switch (code)
    {
        case Code::StaleToken: return ErrorCodes::BAD_ARGUMENTS;
        case Code::InvalidRequest: return ErrorCodes::LOGICAL_ERROR;
    }
    return ErrorCodes::LOGICAL_ERROR;
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
    if (request.kind == UDT::LifecycleQueryKind::DeferredPhysicalization)
    {
        const ASTApplyPhysicalizeTypeReferencesQuery * routed_apply = nullptr;
        UUID routed_database_uuid = UUIDHelpers::Nil;
        try
        {
            const auto actor = makeActor(getContext());
            if (const auto * dry_run = query->as<ASTPhysicalizeTypeReferencesQuery>())
            {
                if (!dry_run->cluster.empty())
                    throw Exception(ErrorCodes::NOT_IMPLEMENTED, "PHYSICALIZE TYPE REFERENCES does not support ON CLUSTER");
                const String database_name = getContext()->resolveDatabase(dry_run->getDatabase());
                /// Coarse visibility must precede catalog lookup and engine
                /// dispatch so hidden database state is not an error oracle.
                requirePhysicalizationDatabaseVisibility(getContext(), database_name);
                auto database = DatabaseCatalog::instance().getDatabase(database_name);
                [[maybe_unused]] auto database_ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, "", database.get());
                if (database->getEngineName() != "Atomic")
                    throw Exception(
                        ErrorCodes::NOT_IMPLEMENTED,
                        "PHYSICALIZE TYPE REFERENCES supports only Atomic databases; {} uses engine {}",
                        database_name,
                        database->getEngineName());
                PhysicalizationAuthorization authorization(getContext(), database_name, database->getUUID());
                auto selector = makePhysicalizationSelector(*dry_run, *database, authorization);
                auto dry_run_result = database->getUDTLifecycleAdapter().physicalizationDryRun(std::move(selector), actor, authorization);
                const String issued_token = dry_run_result.opaque_token;
                try
                {
                    return physicalizationDryRunBlock(std::move(dry_run_result));
                }
                catch (...)
                {
                    /// The client did not receive this token, so remove both
                    /// the database-owned binding and its process route.
                    database->getUDTLifecycleAdapter().discardPhysicalizationToken(issued_token, actor);
                    throw;
                }
            }
            if (const auto * apply = query->as<ASTApplyPhysicalizeTypeReferencesQuery>())
            {
                routed_apply = apply;
                const UInt64 now_microseconds = currentPhysicalizationTimeMicroseconds();
                routed_database_uuid
                    = UDT::PhysicalizationTokenRouter::resolveDatabase(apply->getToken(), actor.principal_uuid, now_microseconds);
                auto database = DatabaseCatalog::instance().getDatabase(routed_database_uuid);
                const String database_name = database->getDatabaseName();
                [[maybe_unused]] auto database_ddl_guard = DatabaseCatalog::instance().getDDLGuard(database_name, "", database.get());
                if (database->getEngineName() != "Atomic")
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "A physicalization token routed to a non-Atomic database");
                PhysicalizationAuthorization authorization(getContext(), database_name, routed_database_uuid);
                database->getUDTLifecycleAdapter().physicalizationApply(apply->getToken(), actor, authorization);
                return {};
            }
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Unknown physicalization AST at the user-defined type lifecycle boundary");
        }
        catch (const UDT::PhysicalizationTokenStoreError & error)
        {
            if (error.code == UDT::PhysicalizationTokenStoreError::Code::TokenRejected && routed_apply
                && routed_database_uuid != UUIDHelpers::Nil)
            {
                UDT::PhysicalizationTokenRouter::unregisterToken(routed_apply->getToken(), routed_database_uuid);
            }
            throw Exception(physicalizationTokenStoreErrorCode(error.code), "{}", error.what());
        }
        catch (const UDT::PhysicalizationPlanError & error)
        {
            throw Exception(physicalizationPlanErrorCode(error.code), "{}", error.what());
        }
        catch (const UDT::PhysicalizationApplyCoordinatorError & error)
        {
            if (error.code == UDT::PhysicalizationApplyCoordinatorError::Code::StaleToken && routed_apply
                && routed_database_uuid != UUIDHelpers::Nil)
            {
                UDT::PhysicalizationTokenRouter::unregisterToken(routed_apply->getToken(), routed_database_uuid);
            }
            throw Exception(physicalizationApplyCoordinatorErrorCode(error.code), "{}", error.what());
        }
        catch (const UDT::PhysicalizationMutationPlannerError & error)
        {
            throw Exception(physicalizationMutationPlannerErrorCode(error.code), "{}", error.what());
        }
    }

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
