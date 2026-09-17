#include <Storages/System/StorageSystemUDTs.h>

#include <Access/ContextAccess.h>
#include <Columns/ColumnString.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeUUID.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/UDT/Definition.h>
#include <Databases/IDatabase.h>
#include <Databases/UDT/ILifecycleAdapter.h>
#include <Interpreters/Context.h>
#include <Interpreters/DatabaseCatalog.h>
#include <Interpreters/UDTLifecycleIntrospection.h>
#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTUDTReference.h>
#include <Storages/System/SystemTableSourceRegistry.h>
#include <Common/Exception.h>

#include <algorithm>
#include <array>
#include <memory>
#include <tuple>
#include <utility>
#include <vector>


namespace DB
{
namespace ErrorCodes
{
extern const int LOGICAL_ERROR;
}

namespace
{
String lowerHexDigest(const UDT::Digest & digest)
{
    static constexpr std::array<char, 16> digits{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};
    String result(digest.size() * 2, '\0');
    for (size_t index = 0; index < digest.size(); ++index)
    {
        result[2 * index] = digits[digest[index] >> 4];
        result[2 * index + 1] = digits[digest[index] & 0x0f];
    }
    return result;
}

String taggedDefinitionHash(const UDT::Digest & digest)
{
    return "v2:" + lowerHexDigest(digest);
}

String taggedStorageFingerprint(const UDT::Digest & digest)
{
    return "v2:" + lowerHexDigest(digest);
}

std::string_view parameterKindName(UDT::ParameterKind kind)
{
    switch (kind)
    {
        case UDT::ParameterKind::Type: return "Type";
        case UDT::ParameterKind::Bool: return "Bool";
        case UDT::ParameterKind::UInt8: return "UInt8";
        case UDT::ParameterKind::UInt16: return "UInt16";
        case UDT::ParameterKind::UInt32: return "UInt32";
        case UDT::ParameterKind::UInt64: return "UInt64";
        case UDT::ParameterKind::Int8: return "Int8";
        case UDT::ParameterKind::Int16: return "Int16";
        case UDT::ParameterKind::Int32: return "Int32";
        case UDT::ParameterKind::Int64: return "Int64";
        case UDT::ParameterKind::String: return "String";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Published user-defined type has an unknown parameter kind");
}

String semanticABIIdentity(const UDT::Record & record)
{
    return "checker_abi=" + std::to_string(record.checker_abi) + ";checker_charge_abi=" + std::to_string(record.checker_charge_abi)
        + ";policy_abi=" + std::to_string(record.policy_abi) + ";function_registry_abi=" + std::to_string(record.function_registry_abi);
}

String storageBackendName(UDT::StorageBackend backend)
{
    switch (backend)
    {
        case UDT::StorageBackend::AtomicDisk: return "AtomicDisk";
    }
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Published user-defined type has an unknown storage backend");
}

struct DependencyRow
{
    UUID database_uuid = UUIDHelpers::Nil;
    UUID type_uuid = UUIDHelpers::Nil;
    UInt64 revision = 0;
    String definition_hash;
    String application;
    String name;

    auto tie() const noexcept { return std::tie(database_uuid, type_uuid, revision, definition_hash, application, name); }
};

std::vector<const ASTUDTReference *> collectReferenceApplications(const IAST & root)
{
    std::vector<const ASTUDTReference *> result;
    std::vector<const IAST *> pending{&root};
    while (!pending.empty())
    {
        const IAST * node = pending.back();
        pending.pop_back();
        if (const auto * reference = node->as<ASTUDTReference>())
            result.push_back(reference);
        for (auto it = node->children.rbegin(); it != node->children.rend(); ++it)
            pending.push_back(it->get());
    }
    return result;
}

std::vector<std::pair<UDT::TemplateNodeKind, UInt16>> collectCheckedCalls(const UDT::Definition & definition)
{
    std::vector<std::pair<UDT::TemplateNodeKind, UInt16>> result;
    for (const auto & node : definition.getNodes())
    {
        if (node.kind == UDT::TemplateNodeKind::SelfCall || node.kind == UDT::TemplateNodeKind::DefinitionCall)
            result.emplace_back(node.kind, node.dependency_ordinal);
    }
    return result;
}

Array makeDependencies(
    const UDT::ILifecycleSnapshot & snapshot,
    const UDT::Record & record,
    const UDT::Definition & definition,
    const ASTCreateTypeQuery & create)
{
    if (!create.definition)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Published user-defined type has no canonical definition AST");

    const auto references = collectReferenceApplications(*create.definition);
    const auto calls = collectCheckedCalls(definition);
    if (references.size() != calls.size())
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Published user-defined type reference applications disagree with checked calls");

    std::vector<DependencyRow> rows;
    for (size_t index = 0; index < calls.size(); ++index)
    {
        const auto [kind, ordinal] = calls[index];
        if (kind == UDT::TemplateNodeKind::SelfCall)
            continue;
        if (ordinal >= record.dependencies.size())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Published user-defined type dependency ordinal is out of range");

        const auto & dependency = record.dependencies[ordinal];
        const UDT::DefinitionIdentity identity{record.identity.database_uuid, dependency.type_uuid, dependency.revision};
        const auto target = snapshot.findCheckedDefinitionByIdentity(identity);
        if (!target || target->getDefinitionHash() != dependency.target_definition_hash)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Published user-defined type dependency is absent from its immutable snapshot");

        rows.push_back({
            .database_uuid = record.identity.database_uuid,
            .type_uuid = dependency.type_uuid,
            .revision = dependency.revision,
            .definition_hash = taggedDefinitionHash(dependency.target_definition_hash),
            .application = references[index]->formatWithSecretsOneLine(),
            .name = target->getNormalizedName(),
        });
    }

    std::sort(rows.begin(), rows.end(), [](const auto & lhs, const auto & rhs) { return lhs.tie() < rhs.tie(); });
    Array result;
    result.reserve(rows.size());
    for (auto & row : rows)
    {
        result.push_back(
            Tuple{
                row.database_uuid,
                row.type_uuid,
                row.revision,
                std::move(row.definition_hash),
                std::move(row.application),
                std::move(row.name),
            });
    }
    return result;
}

Array makeParameters(const UDT::Record & record)
{
    Array result;
    result.reserve(record.parameters.size());
    for (const auto & parameter : record.parameters)
        result.push_back(Tuple{parameter.normalized_name, String{parameterKindName(parameter.kind)}});
    return result;
}

Array makeUnavailableDependencies(const UDT::Record & record)
{
    Array result;
    result.reserve(record.dependencies.size());
    for (const auto & dependency : record.dependencies)
    {
        result.push_back(
            Tuple{
                record.identity.database_uuid,
                dependency.type_uuid,
                dependency.revision,
                taggedDefinitionHash(dependency.target_definition_hash),
                String{},
                String{},
            });
    }
    return result;
}

void insertUnavailableDefinition(
    MutableColumns & columns, std::string_view database_name, const UDT::AtomicAuthorityStartupDefinitionDiagnostic & diagnostic)
{
    const UDT::Record * record = diagnostic.record ? std::addressof(*diagnostic.record) : nullptr;
    size_t column = 0;
    columns[column++]->insert(String(database_name));
    columns[column++]->insert(record ? record->normalized_local_name : String{});
    columns[column++]->insert(diagnostic.key.object_uuid);
    columns[column++]->insert(diagnostic.revision);
    columns[column++]->insert(record ? makeParameters(*record) : Array{});
    if (record && record->decreasing_parameter && *record->decreasing_parameter < record->parameters.size())
        columns[column++]->insert(record->parameters[*record->decreasing_parameter].normalized_name);
    else
        columns[column++]->insertDefault();
    columns[column++]->insert(record ? semanticABIIdentity(*record) : String{});
    columns[column++]->insert(record ? lowerHexDigest(record->checker_certificate_digest) : String{});
    columns[column++]->insert(record ? record->canonical_physical_template_sql : String{});
    columns[column++]->insert(record ? taggedDefinitionHash(record->definition_hash) : String{});
    columns[column++]->insertDefault();

    String create_query;
    if (record)
    {
        try
        {
            create_query = UDT::makeShowCreateTypeQuery(*record)->formatWithSecretsOneLine();
        }
        catch (const UDT::LifecycleIntrospectionError &)
        {
            /// INVALID rows must remain observable even when their canonical
            /// SQL is precisely the malformed component. No raw parser error
            /// or record payload is copied into last_error.
        }
    }
    columns[column++]->insert(std::move(create_query));
    columns[column++]->insert(UInt8{0});
    columns[column++]->insert(UInt8{0});
    columns[column++]->insertDefault();
    columns[column++]->insert(Array{});
    columns[column++]->insert(record ? makeUnavailableDependencies(*record) : Array{});
    columns[column++]->insert(record ? record->owner_display_name : String{});
    columns[column++]->insert(record ? record->comment : String{});
    columns[column++]->insert(DecimalField<DateTime64>(record ? DateTime64(record->creation_time_us_utc) : DateTime64(0), 6));
    columns[column++]->insert(record ? storageBackendName(record->storage_backend) : String{"AtomicDisk"});
    columns[column++]->insert(static_cast<Int8>(diagnostic.status));
    columns[column++]->insert(diagnostic.last_error);
}
}

ColumnsDescription StorageSystemUDTs::getColumnsDescription()
{
    auto string = std::make_shared<DataTypeString>();
    auto nullable_string = std::make_shared<DataTypeNullable>(string);
    auto parameter_tuple = std::make_shared<DataTypeTuple>(DataTypes{string, string}, Names{"name", "kind"});
    auto dependency_tuple = std::make_shared<DataTypeTuple>(
        DataTypes{
            std::make_shared<DataTypeUUID>(),
            std::make_shared<DataTypeUUID>(),
            std::make_shared<DataTypeUInt64>(),
            string,
            string,
            string,
        },
        Names{"database_uuid", "type_uuid", "revision", "definition_hash", "application", "name"});
    auto constraint_kind = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{{"INPUT", 1}, {"VALUE", 2}});
    auto constraint_tuple
        = std::make_shared<DataTypeTuple>(DataTypes{string, constraint_kind, string}, Names{"name", "kind", "expression"});
    auto status = std::make_shared<DataTypeEnum8>(DataTypeEnum8::Values{
        {"ACTIVE", 1},
        {"CONFLICTED", 2},
        {"INVALID", 3},
        {"INCOMPLETE", 4},
        {"QUARANTINED", 5},
        {"OVER_QUOTA", 6},
    });

    return ColumnsDescription{
        {"database", string, "Current database display name; rows are filtered by SHOW TYPES."},
        {"name", string, "Current local type display name."},
        {"uuid", std::make_shared<DataTypeUUID>(), "Stable type object identity."},
        {"revision", std::make_shared<DataTypeUInt64>(), "Immutable definition revision."},
        {"parameters", std::make_shared<DataTypeArray>(parameter_tuple), "Canonical ordered parameter declarations."},
        {"decreases_parameter", nullable_string, "Certified decreasing parameter, or NULL."},
        {"semantic_abi_identity", string, "Combined checker, checker-charge, policy, and function-registry ABI identity."},
        {"checker_certificate_hash", string, "Lowercase SHA-256 checker-certificate digest."},
        {"underlying_type", string, "Exact monomorphic expansion, or canonical parameter-referencing template."},
        {"definition_hash", string, "Format-tagged canonical definition digest."},
        {"storage_fingerprint", nullable_string, "Monomorphic storage fingerprint, or NULL for a template."},
        {"create_query", string, "Canonical current-name CREATE TYPE declaration."},
        /// These columns intentionally reserve a stable introspection schema for
        /// semantic capabilities. They return their neutral values until the
        /// corresponding capability contract is activated; consumers may rely
        /// on their presence, but must not infer support from the schema alone.
        {"has_input", std::make_shared<DataTypeUInt8>(), "Reserved semantic INPUT capability flag; zero while the capability is inactive."},
        {"has_output",
         std::make_shared<DataTypeUInt8>(),
         "Reserved semantic OUTPUT capability flag; zero while the capability is inactive."},
        {"default_expression", nullable_string, "Reserved normalized default expression; NULL while the capability is inactive."},
        {"constraints",
         std::make_shared<DataTypeArray>(constraint_tuple),
         "Reserved ordered INPUT/VALUE constraints; empty while the capability is inactive."},
        {"dependencies", std::make_shared<DataTypeArray>(dependency_tuple), "Canonical direct dependency applications."},
        {"owner", string, "Creating principal display name."},
        {"comment", string, "Exact logical COMMENT value."},
        {"creation_time", std::make_shared<DataTypeDateTime64>(6), "Durable commit timestamp in UTC."},
        {"storage_backend", std::make_shared<DataTypeLowCardinality>(string), "Authority storage backend."},
        {"status", status, "Published authority status."},
        {"last_error", string, "Bounded stable diagnostic; empty for ACTIVE."},
    };
}

void StorageSystemUDTs::fillData(
    MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    const auto access = context->getAccess();
    auto databases = DatabaseCatalog::instance().getDatabases(GetDatabasesOptions{});
    std::vector<std::pair<String, DatabasePtr>> ordered_databases(databases.begin(), databases.end());
    std::sort(ordered_databases.begin(), ordered_databases.end(), [](const auto & lhs, const auto & rhs) { return lhs.first < rhs.first; });

    for (const auto & [database_name, database] : ordered_databases)
    {
        if (!access->isGranted(AccessType::SHOW_TYPES, database_name))
            continue;
        const auto & capabilities = database->getSupportedUDTAuthorityCapabilities();
        if (!capabilities.contains(UDT::TypeAuthorityCapability::DurableAlias))
            continue;

        auto snapshot = database->getUDTLifecycleAdapter().acquireSnapshot();
        if (!snapshot || snapshot->getDatabaseUUID() != database->getUUID())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "User-defined type lifecycle snapshot belongs to another database");

        std::vector<const UDT::Record *> records;
        records.reserve(snapshot->getDefinitionRecords().size());
        for (const auto & record : snapshot->getDefinitionRecords())
            records.push_back(&record);
        std::sort(
            records.begin(),
            records.end(),
            [](const auto * lhs, const auto * rhs)
            {
                return std::tie(lhs->normalized_local_name, lhs->identity.type_uuid, lhs->identity.revision)
                    < std::tie(rhs->normalized_local_name, rhs->identity.type_uuid, rhs->identity.revision);
            });

        for (const auto * record : records)
        {
            const auto checked = snapshot->findCheckedDefinitionByIdentity(record->identity);
            if (!checked || !UDT::recordMatchesCheckedDefinition(*record, *checked))
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Published user-defined type record disagrees with its checked definition");

            const ASTPtr shown = UDT::makeShowCreateTypeQuery(*record);
            const auto * create = shown->as<ASTCreateTypeQuery>();
            if (!create)
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Published user-defined type CREATE projection is not a CREATE TYPE AST");
            const auto projection = snapshot->getMonomorphicProjection(record->identity);

            size_t column = 0;
            res_columns[column++]->insert(database_name);
            res_columns[column++]->insert(record->normalized_local_name);
            res_columns[column++]->insert(record->identity.type_uuid);
            res_columns[column++]->insert(record->identity.revision);
            res_columns[column++]->insert(makeParameters(*record));
            if (record->decreasing_parameter)
            {
                if (*record->decreasing_parameter >= record->parameters.size())
                    throw Exception(ErrorCodes::LOGICAL_ERROR, "Published user-defined type decreasing parameter is out of range");
                res_columns[column++]->insert(record->parameters[*record->decreasing_parameter].normalized_name);
            }
            else
                res_columns[column++]->insertDefault();
            res_columns[column++]->insert(semanticABIIdentity(*record));
            res_columns[column++]->insert(lowerHexDigest(record->checker_certificate_digest));
            res_columns[column++]->insert(projection ? projection->canonical_physical_type : record->canonical_physical_template_sql);
            res_columns[column++]->insert(taggedDefinitionHash(record->definition_hash));
            if (projection)
                res_columns[column++]->insert(taggedStorageFingerprint(projection->storage_fingerprint));
            else
                res_columns[column++]->insertDefault();
            res_columns[column++]->insert(shown->formatWithSecretsOneLine());
            res_columns[column++]->insert(UInt8{0});
            res_columns[column++]->insert(UInt8{0});
            res_columns[column++]->insertDefault();
            res_columns[column++]->insert(Array{});
            res_columns[column++]->insert(makeDependencies(*snapshot, *record, *checked, *create));
            res_columns[column++]->insert(record->owner_display_name);
            res_columns[column++]->insert(record->comment);
            res_columns[column++]->insert(DecimalField<DateTime64>(DateTime64(record->creation_time_us_utc), 6));
            res_columns[column++]->insert(storageBackendName(record->storage_backend));
            const auto status = snapshot->getDefinitionStatus(record->identity);
            res_columns[column++]->insert(static_cast<Int8>(status));
            res_columns[column++]->insert(String(snapshot->getDefinitionLastError(record->identity)));
        }

        const auto unavailable = snapshot->getUnavailableDefinitionDiagnostics();
        std::vector<const UDT::AtomicAuthorityStartupDefinitionDiagnostic *> ordered_unavailable;
        ordered_unavailable.reserve(unavailable.size());
        for (const auto & diagnostic : unavailable)
            ordered_unavailable.push_back(&diagnostic);
        std::sort(
            ordered_unavailable.begin(),
            ordered_unavailable.end(),
            [](const auto * lhs, const auto * rhs)
            {
                const std::string_view lhs_name = lhs->record ? std::string_view(lhs->record->normalized_local_name) : std::string_view{};
                const std::string_view rhs_name = rhs->record ? std::string_view(rhs->record->normalized_local_name) : std::string_view{};
                if (lhs_name != rhs_name)
                    return lhs_name < rhs_name;
                if (lhs->key.object_uuid != rhs->key.object_uuid)
                    return lhs->key.object_uuid < rhs->key.object_uuid;
                return lhs->revision < rhs->revision;
            });
        for (const auto * diagnostic : ordered_unavailable)
            insertUnavailableDefinition(res_columns, database_name, *diagnostic);
    }
}

}

namespace DB
{
REGISTER_SYSTEM_TABLE_SOURCE(StorageSystemUDTs)
}
