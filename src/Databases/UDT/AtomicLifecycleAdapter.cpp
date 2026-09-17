#include <Databases/UDT/AtomicLifecycleAdapter.h>

#include <Databases/DatabaseAtomic.h>
#include <Databases/DatabaseSchemaMutationTransaction.h>
#include <Databases/UDT/AtomicAuthority.h>
#include <Databases/UDT/AtomicDatabaseSchemaMutationStorage.h>
#include <Databases/UDT/AuthorityQuarantineAdmission.h>
#include <Databases/UDT/AuthorityVerificationRuntimeState.h>
#include <Databases/UDT/AuthorityVerificationScheduler.h>
#include <Databases/UDT/DefinitionMutationPlanner.h>
#include <Databases/UDT/ResourceLimitAdapters.h>

#include <DataTypes/BuiltInDataTypeFamilyClassifier.h>
#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/UDT/CanonicalHash.h>
#include <DataTypes/UDT/DefinitionLowering.h>
#include <DataTypes/UDT/PhysicalTypeFingerprint.h>
#include <DataTypes/UDT/ResourceLimitAdapters.h>
#include <DataTypes/UDT/TemplateChecker.h>
#include <DataTypes/UDT/TemplateSpecializer.h>

#include <Core/Field.h>

#include <Interpreters/DatabaseCatalog.h>

#include <IO/WriteHelpers.h>

#include <Parsers/ASTAlterTypeCommentQuery.h>
#include <Parsers/ASTCreateTypeQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTDropTypeQuery.h>
#include <Parsers/ASTEnumDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTFunction.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Parsers/ASTRenameTypeQuery.h>
#include <Parsers/ASTTupleDataType.h>
#include <Parsers/ASTUDTReference.h>
#include <Parsers/ParserCreateTypeQuery.h>
#include <Parsers/parseQuery.h>

#include <Common/Exception.h>
#include <Common/UniqueLock.h>
#include <Common/logger_useful.h>

#include <algorithm>
#include <array>
#include <chrono>
#include <exception>
#include <functional>
#include <limits>
#include <map>
#include <mutex>
#include <optional>
#include <ranges>
#include <set>
#include <span>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

namespace DB::ErrorCodes
{
extern const int ABORTED;
extern const int BAD_ARGUMENTS;
extern const int DEADLOCK_AVOIDED;
extern const int LOGICAL_ERROR;
extern const int NOT_IMPLEMENTED;
extern const int UNKNOWN_TYPE;
}

namespace DB::UDT
{
namespace
{

using MutationKind = DefinitionMutationKind;
using MutationRequest = DefinitionMutationRequest;
using CompositeRoot = AtomicAuthority::CompositeRoot;

constexpr UInt64 canonical_attach_maximum_parser_depth = 256;
constexpr UInt64 canonical_attach_maximum_parser_backtracks = 1'000'000;

void logNeverEnabledScaffoldCleanupFailureNoThrow(UUID database_uuid, Int32 error_code) noexcept
{
    try
    {
        LOG_ERROR(
            getLogger("UDTAtomicLifecycle"),
            "Failed to clean an uncommitted UDT authority scaffold for database UUID {} (error code {})",
            database_uuid,
            error_code);
    }
    catch (...)
    {
    }
}

[[noreturn]] void invalid(std::string_view message)
{
    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid Atomic user-defined type lifecycle request: {}", message);
}

[[noreturn]] void logicalError(std::string_view message)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Atomic user-defined type lifecycle invariant failed: {}", message);
}

using CanonicalLifecycleTouchSet = std::set<SchemaObjectID>;

SchemaObjectID definitionObject(const DefinitionIdentity & identity)
{
    return {
        .kind = SchemaObjectKind::TypeDefinition,
        .database_uuid = identity.database_uuid,
        .object_uuid = identity.type_uuid,
    };
}

bool definitionIdentityLess(const DefinitionIdentity & lhs, const DefinitionIdentity & rhs) noexcept
{
    const auto lhs_database = uuidToCanonicalBytes(lhs.database_uuid);
    const auto rhs_database = uuidToCanonicalBytes(rhs.database_uuid);
    if (lhs_database != rhs_database)
        return lhs_database < rhs_database;
    const auto lhs_type = uuidToCanonicalBytes(lhs.type_uuid);
    const auto rhs_type = uuidToCanonicalBytes(rhs.type_uuid);
    if (lhs_type != rhs_type)
        return lhs_type < rhs_type;
    return lhs.revision < rhs.revision;
}

void addLifecycleTouch(CanonicalLifecycleTouchSet & touched, const SchemaObjectID & object)
{
    if (!object.isValid())
        logicalError("a lifecycle quarantine touch identity is invalid");
    if (touched.contains(object))
        return;
    if (touched.size() >= AuthorityQuarantineAdmissionLimits{}.maximum_touched_objects)
    {
        throw Exception(ErrorCodes::ABORTED, "Atomic user-defined type lifecycle touch closure exceeds the quarantine admission limit");
    }
    touched.insert(object);
}

void expandLifecycleTouchForwardClosure(
    CanonicalLifecycleTouchSet & touched,
    const SchemaObjectDependencyGraph & primary_graph,
    const SchemaObjectDependencyGraph * secondary_graph = nullptr)
{
    std::vector<SchemaObjectID> pending(touched.begin(), touched.end());
    for (size_t index = 0; index < pending.size(); ++index)
    {
        const auto add_dependencies = [&](const SchemaObjectDependencyGraph & graph)
        {
            if (!graph.containsNode(pending[index]))
                return;
            for (const auto & dependency : graph.getDependencies(pending[index]))
            {
                const bool already_touched = touched.contains(dependency.object);
                addLifecycleTouch(touched, dependency.object);
                if (!already_touched)
                    pending.push_back(dependency.object);
            }
        };
        add_dependencies(primary_graph);
        if (secondary_graph)
            add_dependencies(*secondary_graph);
    }
}

std::vector<SchemaObjectID> collectDefinitionMutationTouchSet(const CompositeRoot & current_root, const CompositeRoot & replacement_root)
{
    if (current_root.getDatabaseUUID() != replacement_root.getDatabaseUUID())
        logicalError("a definition mutation replacement root belongs to another database");

    CanonicalLifecycleTouchSet touched;
    const auto current_records = current_root.getDefinitionRecords();
    const auto replacement_records = replacement_root.getDefinitionRecords();
    size_t current_index = 0;
    size_t replacement_index = 0;
    while (current_index < current_records.size() || replacement_index < replacement_records.size())
    {
        if (current_index == current_records.size())
        {
            addLifecycleTouch(touched, definitionObject(replacement_records[replacement_index].identity));
            ++replacement_index;
            continue;
        }
        if (replacement_index == replacement_records.size())
        {
            addLifecycleTouch(touched, definitionObject(current_records[current_index].identity));
            ++current_index;
            continue;
        }

        const auto & current_record = current_records[current_index];
        const auto & replacement_record = replacement_records[replacement_index];
        if (definitionIdentityLess(current_record.identity, replacement_record.identity))
        {
            addLifecycleTouch(touched, definitionObject(current_record.identity));
            ++current_index;
            continue;
        }
        if (definitionIdentityLess(replacement_record.identity, current_record.identity))
        {
            addLifecycleTouch(touched, definitionObject(replacement_record.identity));
            ++replacement_index;
            continue;
        }
        if (current_record != replacement_record)
            addLifecycleTouch(touched, definitionObject(current_record.identity));
        ++current_index;
        ++replacement_index;
    }
    if (touched.empty())
        logicalError("a material definition mutation changed no durable definition record");

    expandLifecycleTouchForwardClosure(
        touched, current_root.getSchemaObjectDependencyGraph(), std::addressof(replacement_root.getSchemaObjectDependencyGraph()));
    return {touched.begin(), touched.end()};
}

std::string_view definitionMutationOperationName(DefinitionMutationKind kind)
{
    switch (kind)
    {
        case DefinitionMutationKind::Create: return "CREATE or ATTACH TYPE";
        case DefinitionMutationKind::ReplaceSemantic: return "ALTER TYPE";
        case DefinitionMutationKind::Rename: return "RENAME TYPE";
        case DefinitionMutationKind::Comment: return "ALTER TYPE COMMENT";
        case DefinitionMutationKind::Drop: return "DROP TYPE";
    }
    logicalError("a definition mutation has an unknown operation kind");
}

[[noreturn]] void rejectAfterShutdown(std::string_view operation)
{
    throw Exception(ErrorCodes::ABORTED, "Cannot {} because the owning Atomic database has been shut down", operation);
}

[[noreturn]] void rejectDuringDegradedAuthorityStartup(std::string_view operation)
{
    throw Exception(
        ErrorCodes::ABORTED,
        "Cannot {} because the owning Atomic user-defined type authority is invalid or incomplete after recovery",
        operation);
}

String lowerHexDigest(const Digest & digest)
{
    static constexpr char digits[] = "0123456789abcdef";
    String result(digest.size() * 2, '\0');
    for (std::size_t index = 0; index < digest.size(); ++index)
    {
        const UInt8 value = digest[index];
        result[2 * index] = digits[value >> 4];
        result[2 * index + 1] = digits[value & 0x0f];
    }
    return result;
}

Int64 currentTimeMicroseconds()
{
    const auto count = std::chrono::duration_cast<std::chrono::microseconds>(std::chrono::system_clock::now().time_since_epoch()).count();
    if (!std::in_range<Int64>(count))
        invalid("current creation time is outside the Int64 microsecond domain");
    return static_cast<Int64>(count);
}

String commentFromCreateQuery(const ASTCreateTypeQuery & query)
{
    if (!query.comment)
        return {};
    const auto * literal = query.comment->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::String)
        invalid("CREATE TYPE comment is not a String literal");
    return literal->value.safeGet<String>();
}

String commentFromAlterQuery(const ASTAlterTypeCommentQuery & query)
{
    if (!query.comment)
        invalid("ALTER TYPE COMMENT has an invalid operation shape");
    const auto * literal = query.comment->as<ASTLiteral>();
    if (!literal || literal->value.getType() != Field::Types::String)
        invalid("ALTER TYPE comment is not a String literal");
    return literal->value.safeGet<String>();
}

StructuredDefinitionName structuredName(const String & database_name, const String & local_name)
{
    if (database_name.empty() || local_name.empty())
        invalid("database and local type names must be nonempty");
    return {
        .normalized_database_name = database_name,
        .normalized_qualified_name = database_name + "." + local_name,
        .normalized_local_name = local_name,
    };
}

void validateQualifiedMutation(const String & actual_database_name, const String & query_database_name, std::string_view cluster)
{
    if (query_database_name != actual_database_name)
        invalid("query database does not match the owning Atomic database");
    if (!cluster.empty())
        invalid("ON CLUSTER crossed the local Atomic lifecycle boundary");
}

const Record * findRecord(const CompositeRoot * root, std::string_view local_name) noexcept
{
    if (!root)
        return nullptr;
    const auto records = root->getDefinitionRecords();
    const auto it
        = std::find_if(records.begin(), records.end(), [&](const Record & record) { return record.normalized_local_name == local_name; });
    return it == records.end() ? nullptr : std::addressof(*it);
}

std::vector<AvailableDefinitionBinding>
makeBindings(const CompositeRoot * root, const String & database_name, std::optional<std::string_view> excluded_local_name = std::nullopt)
{
    std::vector<AvailableDefinitionBinding> bindings;
    if (!root)
        return bindings;
    const auto records = root->getDefinitionRecords();
    bindings.reserve(records.size());
    for (const auto & record : records)
    {
        if (excluded_local_name && record.normalized_local_name == *excluded_local_name)
            continue;
        if (record.normalized_name != database_name + "." + record.normalized_local_name)
            logicalError("a current definition record is not qualified by the current database name");
        auto definition = root->findByIdentity(record.identity);
        if (!definition || !recordMatchesCheckedDefinition(record, *definition))
            logicalError("a current definition record has no exact checked definition");
        std::vector<ParameterKind> parameter_kinds;
        parameter_kinds.reserve(definition->getParameters().size());
        for (const auto & parameter : definition->getParameters())
            parameter_kinds.push_back(parameter.kind);
        bindings.push_back({
            .name = structuredName(database_name, record.normalized_local_name),
            .identity = record.identity,
            .definition_hash = record.definition_hash,
            .parameter_kinds = std::move(parameter_kinds),
        });
    }
    return bindings;
}

DefinitionLoweringLimits atomicDefinitionLoweringLimits(const EffectiveResourceLimits & effective_limits)
{
    DefinitionLoweringLimits limits;
    limits.maximum_definitions = effective_limits.get(ResourceLimit::DefinitionsPerDatabase);
    limits.maximum_catalog_string_bytes = effective_limits.get(ResourceLimit::DeterministicCatalogBytesPerDatabase);
    return limits;
}

TemplateCheckerLimits atomicCheckerLimits(const EffectiveResourceLimits & effective_limits)
{
    return makeTemplateCheckerLimits(effective_limits);
}

const EffectiveResourceLimits & implementationEffectiveResourceLimits()
{
    static const EffectiveResourceLimits limits = calculateEffectiveResourceLimits(std::span<const ResourceLimitLayer>{});
    return limits;
}

std::vector<DefinitionInput> copyCurrentDefinitionInputs(const CompositeRoot * root)
{
    std::vector<DefinitionInput> inputs;
    if (!root)
        return inputs;
    const auto records = root->getDefinitionRecords();
    inputs.reserve(records.size());
    for (const auto & record : records)
    {
        auto definition = root->findByIdentity(record.identity);
        if (!definition || !recordMatchesCheckedDefinition(record, *definition))
            logicalError("a current definition cannot be copied into a complete checker batch");
        inputs.push_back(definitionInputFromCheckedDefinition(*definition));
    }
    return inputs;
}

Definition::Ptr findCheckedDefinition(std::span<const Definition::Ptr> definitions, const DefinitionIdentity & identity)
{
    const auto it = std::find_if(
        definitions.begin(), definitions.end(), [&](const auto & definition) { return definition->getIdentity() == identity; });
    if (it == definitions.end())
        logicalError("the complete checker batch omitted the mutation target");
    return *it;
}

class CanonicalDefinitionPresentationRewriter final
{
public:
    CanonicalDefinitionPresentationRewriter(
        const Definition & definition_,
        const CompositeRoot * root_,
        String database_name_,
        std::optional<DefinitionIdentity> renamed_identity_ = std::nullopt,
        String renamed_local_name_ = {})
        : definition(definition_)
        , root(root_)
        , database_name(std::move(database_name_))
        , renamed_identity(std::move(renamed_identity_))
        , renamed_local_name(std::move(renamed_local_name_))
    {
        for (const auto & node : definition.getNodes())
            if (node.kind == TemplateNodeKind::SelfCall || node.kind == TemplateNodeKind::DefinitionCall)
                checked_calls.push_back(std::addressof(node));
    }

    ASTPtr rewrite(const ASTPtr & source)
    {
        if (!source)
            logicalError("a checked definition has no parser presentation body");
        ASTPtr result = source->clone();
        result = rewriteNode(std::move(result));
        if (next_checked_call != checked_calls.size())
            logicalError("canonical presentation omitted a checked definition-call occurrence");
        return result;
    }

private:
    struct ObjectArgumentOrderKey
    {
        UInt8 rank = 0;
        String key;
    };

    ObjectArgumentOrderKey objectArgumentOrderKey(const ASTPtr & node) const
    {
        const auto * argument = node ? node->as<ASTObjectTypeArgument>() : nullptr;
        if (!argument)
            logicalError("canonical JSON presentation contains a non-object argument");
        if (argument->parameter)
        {
            const auto * function = argument->parameter->as<ASTFunction>();
            const auto * identifier = function && function->arguments && function->arguments->children.size() == 2
                ? function->arguments->children.front()->as<ASTIdentifier>()
                : nullptr;
            if (!identifier)
                logicalError("canonical JSON setting has an invalid checked shape");
            const String name = identifier->name();
            if (name == "max_dynamic_types")
                return {.rank = 0, .key = name};
            if (name == "max_dynamic_paths")
                return {.rank = 1, .key = name};
            logicalError("canonical JSON setting has an unknown checked name");
        }
        if (argument->path_with_type)
        {
            const auto * path = argument->path_with_type->as<ASTObjectTypedPathArgument>();
            if (!path)
                logicalError("canonical JSON typed path has an invalid checked shape");
            return {.rank = 2, .key = path->path};
        }
        if (argument->skip_path)
        {
            const auto * path = argument->skip_path->as<ASTIdentifier>();
            if (!path)
                logicalError("canonical JSON skipped path has an invalid checked shape");
            return {.rank = 3, .key = path->name()};
        }
        const auto * regexp = argument->skip_path_regexp ? argument->skip_path_regexp->as<ASTLiteral>() : nullptr;
        if (!regexp || regexp->value.getType() != Field::Types::String)
            logicalError("canonical JSON skipped regexp has an invalid checked shape");
        return {.rank = 4, .key = regexp->value.safeGet<String>()};
    }

    String resolveCallLocalName(const TemplateNode & call) const
    {
        if (call.kind == TemplateNodeKind::SelfCall)
            return definition.getNormalizedLocalName();
        if (call.kind != TemplateNodeKind::DefinitionCall || call.dependency_ordinal >= definition.getDependencies().size())
            logicalError("canonical presentation call has an invalid checked dependency ordinal");

        const auto & dependency = definition.getDependencies()[call.dependency_ordinal];
        const DefinitionIdentity identity{
            .database_uuid = definition.getIdentity().database_uuid,
            .type_uuid = dependency.type_uuid,
            .revision = dependency.revision,
        };
        if (renamed_identity && identity == *renamed_identity)
            return renamed_local_name;
        if (!root)
            logicalError("canonical presentation cannot resolve a checked dependency without a root");
        auto target = root->findByIdentity(identity);
        if (!target || target->getDefinitionHash() != dependency.target_definition_hash)
            logicalError("canonical presentation cannot resolve its exact checked dependency");
        return target->getNormalizedLocalName();
    }

    ASTPtr rewriteCall(ASTPtr node, ASTPtr arguments)
    {
        if (next_checked_call >= checked_calls.size())
            logicalError("canonical presentation has more reference occurrences than checked calls");
        const auto & call = *checked_calls[next_checked_call++];
        auto reference = make_intrusive<ASTUDTReference>();
        reference->database_name = database_name;
        reference->type_name = resolveCallLocalName(call);
        if (arguments)
            reference->children.push_back(std::move(arguments));
        node = std::move(reference);
        return node;
    }

    ASTPtr rewriteNode(ASTPtr node)
    {
        if (auto * enumeration = node->as<ASTEnumDataType>())
        {
            const auto classification = BuiltInDataTypeFamilyClassifier::classifySpecializedEnum(enumeration->name);
            if (!classification)
                logicalError("checked specialized Enum presentation is no longer registered");
            const auto registered = classification.family->registered_name;
            bool enum16 = registered == "Enum16";
            if (registered == "Enum")
            {
                enum16 = std::ranges::any_of(
                    enumeration->values,
                    [](const auto & entry)
                    { return entry.second < std::numeric_limits<Int8>::min() || entry.second > std::numeric_limits<Int8>::max(); });
            }
            enumeration->name = enum16 ? "Enum16" : "Enum8";
            std::sort(
                enumeration->values.begin(),
                enumeration->values.end(),
                [](const auto & lhs, const auto & rhs) { return lhs.second < rhs.second; });
        }
        else if (auto * tuple = node->as<ASTTupleDataType>())
        {
            tuple->name = "Tuple";
        }
        else if (auto * data_type = node->as<ASTDataType>())
        {
            const auto classification = BuiltInDataTypeFamilyClassifier::classifyGeneric(data_type->name);
            if (!classification)
                return rewriteCall(std::move(node), data_type->getArguments());
            if (classification.input_class == BuiltInDataTypeCreatorInputClass::CanonicalizeGenericEnumArguments)
                invalid("generic Enum shorthand has no canonical durable template projection; use explicit Enum8 or Enum16 values");
            data_type->name = classification.family->canonical_creator_name;
        }
        else if (auto * reference = node->as<ASTUDTReference>())
        {
            return rewriteCall(std::move(node), reference->getArguments());
        }

        if (const auto * data_type = node->as<ASTDataType>(); data_type && data_type->name == "JSON")
        {
            const auto arguments = data_type->getArguments();
            if (arguments)
            {
                auto & children = arguments->children;
                std::stable_sort(
                    children.begin(),
                    children.end(),
                    [&](const ASTPtr & lhs, const ASTPtr & rhs)
                    {
                        const auto lhs_key = objectArgumentOrderKey(lhs);
                        const auto rhs_key = objectArgumentOrderKey(rhs);
                        return std::tie(lhs_key.rank, lhs_key.key) < std::tie(rhs_key.rank, rhs_key.key);
                    });
            }
        }

        for (size_t index = 0; index < node->children.size(); ++index)
        {
            ASTPtr before = node->children[index];
            ASTPtr after = rewriteNode(before);
            if (after.get() != before.get())
            {
                const auto * before_ptr = before.get();
                node->replace(before, after);
                node->updatePointerToChild(before_ptr, after);
            }
        }
        return node;
    }

    const Definition & definition;
    const CompositeRoot * root;
    String database_name;
    std::optional<DefinitionIdentity> renamed_identity;
    String renamed_local_name;
    std::vector<const TemplateNode *> checked_calls;
    size_t next_checked_call = 0;
};

ASTPtr canonicalAttachAST(
    const ASTCreateTypeQuery & source,
    const String & database_name,
    const String & local_name,
    const Definition & definition,
    ASTPtr canonical_definition)
{
    ASTPtr result = source.clone();
    auto & attach = result->as<ASTCreateTypeQuery &>();
    attach.attach = true;
    attach.if_not_exists = false;
    attach.cluster.clear();
    attach.setDatabase(database_name);
    attach.type_name = make_intrusive<ASTIdentifier>(local_name);
    attach.uuid = definition.getIdentity().type_uuid;
    attach.revision = definition.getIdentity().revision;
    attach.definition_hash = lowerHexDigest(definition.getDefinitionHash());
    attach.setOrReplace(attach.definition, std::move(canonical_definition));
    if (commentFromCreateQuery(attach).empty())
        attach.comment.reset();
    attach.normalizeChildrenOrder();
    return result;
}

Record makeCreateRecord(
    const ASTCreateTypeQuery & query,
    const String & database_name,
    const Definition & definition,
    const LifecycleActor & actor,
    const CompositeRoot * current_root)
{
    if (actor.principal_uuid == UUIDHelpers::Nil || actor.principal_display_name.empty())
        invalid("CREATE or ATTACH TYPE requires an authenticated nonempty owner identity");
    CanonicalDefinitionPresentationRewriter rewriter(definition, current_root, database_name);
    ASTPtr canonical_definition = rewriter.rewrite(query.definition);
    const String canonical_physical_template_sql = canonical_definition->formatWithSecretsOneLine();
    const ASTPtr attach = canonicalAttachAST(query, database_name, query.getTypeName(), definition, std::move(canonical_definition));
    return makeRecord(
        definition,
        {
            .canonical_definition_sql = attach->formatWithSecretsOneLine(),
            .canonical_physical_template_sql = canonical_physical_template_sql,
            .owner_uuid = actor.principal_uuid,
            .owner_display_name = actor.principal_display_name,
            .comment = commentFromCreateQuery(query),
            .creation_time_us_utc = currentTimeMicroseconds(),
        });
}

ASTPtr parseCanonicalAttach(const Record & record)
{
    ParserCreateTypeQuery parser;
    ASTPtr ast = parseQuery(
        parser,
        record.canonical_definition_sql,
        "Atomic user-defined type canonical ATTACH record",
        record.canonical_definition_sql.size(),
        canonical_attach_maximum_parser_depth,
        canonical_attach_maximum_parser_backtracks);
    const auto * query = ast->as<ASTCreateTypeQuery>();
    if (!query || !query->attach || query->if_not_exists || !query->cluster.empty() || !query->uuid || !query->revision
        || !query->definition_hash || *query->uuid != record.identity.type_uuid || *query->revision != record.identity.revision
        || *query->definition_hash != lowerHexDigest(record.definition_hash) || query->getDatabase().empty()
        || query->getTypeName() != record.normalized_local_name
        || record.normalized_name != query->getDatabase() + "." + query->getTypeName() || !query->definition
        || query->definition->formatWithSecretsOneLine() != record.canonical_physical_template_sql
        || (record.comment.empty() && query->comment) || commentFromCreateQuery(*query) != record.comment
        || ast->formatWithSecretsOneLine() != record.canonical_definition_sql)
    {
        logicalError("a current canonical ATTACH record is not an exact internal identity image");
    }
    return ast;
}

void validateCanonicalRecordSetAgainstCheckedDefinitions(
    UUID database_uuid,
    const String & database_name,
    std::span<const Definition::Ptr> expected_definitions,
    std::span<const Record> records,
    const EffectiveResourceLimits & effective_limits)
{
    if (expected_definitions.size() != records.size())
        logicalError("canonical record validation has different definition and record counts");

    std::vector<AvailableDefinitionBinding> bindings;
    bindings.reserve(expected_definitions.size());
    for (const auto & definition : expected_definitions)
    {
        std::vector<ParameterKind> parameter_kinds;
        parameter_kinds.reserve(definition->getParameters().size());
        for (const auto & parameter : definition->getParameters())
            parameter_kinds.push_back(parameter.kind);
        bindings.push_back({
            .name = structuredName(database_name, definition->getNormalizedLocalName()),
            .identity = definition->getIdentity(),
            .definition_hash = definition->getDefinitionHash(),
            .parameter_kinds = std::move(parameter_kinds),
        });
    }
    auto prepared_bindings = prepareDefinitionLoweringBindings(
        database_uuid, database_name, std::move(bindings), atomicDefinitionLoweringLimits(effective_limits));

    std::vector<DefinitionInput> inputs;
    inputs.reserve(records.size());
    for (const auto & record : records)
    {
        ASTPtr attach = parseCanonicalAttach(record);
        const auto & query = attach->as<ASTCreateTypeQuery &>();
        if (query.getDatabase() != database_name)
            logicalError("canonical ATTACH record is qualified by another database name");
        inputs.push_back(lowerCreateTypeQueryToDefinitionInput(
            query, record.identity, structuredName(database_name, record.normalized_local_name), prepared_bindings));
    }

    auto recovered_definitions = TemplateChecker::checkAll(std::move(inputs), atomicCheckerLimits(effective_limits));
    for (const auto & expected : expected_definitions)
    {
        auto recovered = findCheckedDefinition(recovered_definitions, expected->getIdentity());
        if (!expected->hasSameCheckedSemantics(*recovered) || expected->getNormalizedName() != recovered->getNormalizedName()
            || expected->getNormalizedLocalName() != recovered->getNormalizedLocalName()
            || expected->getDefinitionHash() != recovered->getDefinitionHash())
        {
            logicalError("canonical ATTACH record set does not recover the exact checked definitions");
        }
    }
}

Record makeRenamePresentationRecord(
    const Record & before,
    const String & database_name,
    const String & record_local_name,
    const Definition & definition,
    const CompositeRoot & current_root,
    const DefinitionIdentity & renamed_identity,
    const String & renamed_local_name)
{
    ASTPtr attach = parseCanonicalAttach(before);
    auto & query = attach->as<ASTCreateTypeQuery &>();
    CanonicalDefinitionPresentationRewriter rewriter(
        definition, std::addressof(current_root), database_name, renamed_identity, renamed_local_name);
    ASTPtr canonical_definition = rewriter.rewrite(query.definition);
    const String canonical_physical_template_sql = canonical_definition->formatWithSecretsOneLine();
    query.setDatabase(database_name);
    query.type_name = make_intrusive<ASTIdentifier>(record_local_name);
    query.setOrReplace(query.definition, std::move(canonical_definition));
    query.normalizeChildrenOrder();
    return makeRecord(
        definition,
        {
            .canonical_definition_sql = attach->formatWithSecretsOneLine(),
            .canonical_physical_template_sql = canonical_physical_template_sql,
            .owner_uuid = before.owner_uuid,
            .owner_display_name = before.owner_display_name,
            .comment = before.comment,
            .creation_time_us_utc = before.creation_time_us_utc,
            .storage_backend = before.storage_backend,
            .semantic_extension_version = before.semantic_extension_version,
            .semantic_extension_flags = before.semantic_extension_flags,
        });
}

Record makeCommentPresentationRecord(const Record & before, const String & comment)
{
    ASTPtr attach = parseCanonicalAttach(before);
    auto & query = attach->as<ASTCreateTypeQuery &>();
    if (comment.empty())
        query.comment.reset();
    else
        query.comment = make_intrusive<ASTLiteral>(comment);
    query.normalizeChildrenOrder();

    auto after = before;
    after.comment = comment;
    after.canonical_definition_sql = attach->formatWithSecretsOneLine();
    return after;
}

class RootBoundAuthorityAdapter final : public IAuthorityAdapter
{
public:
    RootBoundAuthorityAdapter(const CompositeRoot & root_, const TypeAuthorityCapabilities & capabilities_) noexcept
        : root(root_)
        , capabilities(capabilities_)
    {
    }

    const TypeAuthorityCapabilities & getCapabilities() const noexcept override { return capabilities; }
    UUID getDatabaseUUID() const noexcept override { return root.getDatabaseUUID(); }

    ResolutionSession beginResolutionSession() const override
    {
        return makeSnapshotResolutionSession(
            &root,
            {
                .find_by_identity = findByIdentity,
                .find_by_name = findByName,
                .get_generation = getGeneration,
                .get_effective_resource_limits = getEffectiveResourceLimits,
            });
    }

    void requireCapabilities(TypeAuthorityCapabilityMask required, std::string_view operation) const override
    {
        if (!capabilities.containsAll(required))
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Atomic snapshot lacks capabilities required for {}", operation);
    }

private:
    static Definition::Ptr findByIdentity(const void * view, const DefinitionIdentity & identity)
    {
        return static_cast<const CompositeRoot *>(view)->findByIdentity(identity);
    }

    static Definition::Ptr findByName(const void * view, std::string_view local_name)
    {
        return static_cast<const CompositeRoot *>(view)->findByName(local_name);
    }

    static UInt64 getGeneration(const void * view) noexcept { return static_cast<const CompositeRoot *>(view)->getTypeIndexGeneration(); }
    static const EffectiveResourceLimits * getEffectiveResourceLimits(const void * view) noexcept
    {
        return &static_cast<const CompositeRoot *>(view)->getDatabaseResourceQuota().getLimits();
    }

    const CompositeRoot & root;
    const TypeAuthorityCapabilities & capabilities;
};

class AtomicLifecycleSnapshot final : public ILifecycleSnapshot
{
public:
    explicit AtomicLifecycleSnapshot(UUID database_uuid_, AtomicAuthorityStartupStatusSnapshot::Ptr degraded_status_ = {}) noexcept
        : database_uuid(database_uuid_)
        , degraded_status(std::move(degraded_status_))
    {
    }

    AtomicLifecycleSnapshot(
        UUID database_uuid_,
        AtomicAuthority::RootSnapshot root_,
        const TypeAuthorityCapabilities & capabilities_,
        AuthorityQuarantinePlan::Ptr quarantine_,
        bool verification_runtime_fail_closed_) noexcept
        : database_uuid(database_uuid_)
        , root(std::move(root_))
        , capabilities(std::addressof(capabilities_))
        , quarantine(std::move(quarantine_))
        , verification_runtime_fail_closed(verification_runtime_fail_closed_)
    {
        if (quarantine)
        {
            const bool exact_root = root && *root
                && quarantine->getRoot()
                    == AuthorityRootGraphIdentity{
                        .authority_root = {
                            .database_uuid = (*root)->getAuthorityState().database_uuid,
                            .database_catalog_epoch = (*root)->getAuthorityState().database_catalog_epoch,
                            .authority_anchor = (*root)->getAuthorityState().anchor_hash,
                        },
                        .schema_graph_root = (*root)->getAuthorityState().schema_graph_root,
                    };
            if (!exact_root)
            {
                /// Never project a quarantine closure onto a different root.
                /// A concurrently advanced runtime/root pair is diagnostic-only
                /// here, so collapse to the safer database-wide status.
                quarantine.reset();
                verification_runtime_fail_closed = true;
            }
        }
        if (root && *root)
            resolution_authority.emplace((*root).get(), capabilities_);
    }

    UUID getDatabaseUUID() const noexcept override { return database_uuid; }
    UInt64 getDatabaseCatalogEpoch() const noexcept override { return root && *root ? (*root)->getDatabaseCatalogEpoch() : 0; }

    std::span<const Record> getDefinitionRecords() const noexcept override
    {
        return root && *root ? (*root)->getDefinitionRecords() : std::span<const Record>{};
    }

    const Record * findDefinitionRecordByLocalName(std::string_view local_name) const noexcept override
    {
        const auto records = getDefinitionRecords();
        const auto it = std::find_if(
            records.begin(), records.end(), [&](const Record & record) { return record.normalized_local_name == local_name; });
        return it == records.end() ? nullptr : std::addressof(*it);
    }

    Definition::Ptr findCheckedDefinitionByIdentity(const DefinitionIdentity & identity) const noexcept override
    {
        return root && *root ? (*root)->findByIdentity(identity) : Definition::Ptr{};
    }

    AuthorityDefinitionStatus getDefinitionStatus(const DefinitionIdentity & identity) const noexcept override
    {
        if (!root || !*root || identity.database_uuid != database_uuid)
            return AuthorityDefinitionStatus::Incomplete;
        if (verification_runtime_fail_closed)
            return AuthorityDefinitionStatus::Quarantined;
        if (quarantine
            && quarantine->contains({
                .kind = SchemaObjectKind::TypeDefinition,
                .database_uuid = database_uuid,
                .object_uuid = identity.type_uuid,
            }))
            return AuthorityDefinitionStatus::Quarantined;
        if ((*root)->getDatabaseResourceQuota().getState() == DatabaseResourceQuotaState::OverQuota)
            return AuthorityDefinitionStatus::OverQuota;
        return AuthorityDefinitionStatus::Active;
    }

    std::string_view getDefinitionLastError(const DefinitionIdentity & identity) const noexcept override
    {
        const auto status = getDefinitionStatus(identity);
        if (status == AuthorityDefinitionStatus::Quarantined)
        {
            if (verification_runtime_fail_closed)
                return "authority integrity-verification runtime is fail-closed";
            return "definition belongs to an integrity-quarantined dependency closure";
        }
        if (status == AuthorityDefinitionStatus::OverQuota)
            return "database authority usage exceeds its effective persisted resource quota";
        return {};
    }

    std::span<const AtomicAuthorityStartupDefinitionDiagnostic> getUnavailableDefinitionDiagnostics() const noexcept override
    {
        return degraded_status ? degraded_status->getDefinitionDiagnostics()
                               : std::span<const AtomicAuthorityStartupDefinitionDiagnostic>{};
    }

    const SidecarExpectationRecord * findSidecarExpectation(const SchemaObjectID & object) const noexcept override
    {
        return root && *root ? (*root)->findExpectationRecord(object) : nullptr;
    }

    const IAuthorityAdapter * getResolutionAuthorityAdapter() const noexcept override
    {
        return resolution_authority ? std::addressof(*resolution_authority) : nullptr;
    }

    std::optional<MonomorphicProjection> getMonomorphicProjection(const DefinitionIdentity & identity) const override
    {
        if (!root || !*root || !capabilities)
            return std::nullopt;
        const auto records = getDefinitionRecords();
        const auto exact_record
            = std::find_if(records.begin(), records.end(), [&](const Record & record) { return record.identity == identity; });
        if (exact_record == records.end())
            invalid("monomorphic projection identity does not belong to this snapshot");
        auto definition = (*root)->findByIdentity(identity);
        if (!definition || !recordMatchesCheckedDefinition(*exact_record, *definition))
            logicalError("snapshot projection record has no exact checked definition");
        if (!definition->getParameters().empty())
            return std::nullopt;

        RootBoundAuthorityAdapter authority((*root).get(), *capabilities);
        auto attempt = TemplateSpecializer::Attempt::begin(authority);
        auto arguments = CanonicalTypeArguments::validate(definition->getParameters(), {});
        const auto specialization = attempt.specialize(definition->getIdentity(), arguments);
        DataTypePtr physical_type = DataTypeFactory::instance().get(attempt.getCanonicalPhysicalAST(specialization));
        static_cast<void>(attempt.finish());
        return MonomorphicProjection{
            .canonical_physical_type = physical_type->getName(),
            .storage_fingerprint = physicalTypeFingerprint(physical_type),
        };
    }

private:
    UUID database_uuid;
    std::optional<AtomicAuthority::RootSnapshot> root;
    const TypeAuthorityCapabilities * capabilities = nullptr;
    std::optional<RootBoundAuthorityAdapter> resolution_authority;
    AtomicAuthorityStartupStatusSnapshot::Ptr degraded_status;
    AuthorityQuarantinePlan::Ptr quarantine;
    bool verification_runtime_fail_closed = false;
}

bool isMappedStorageObjectKind(SchemaObjectKind kind) noexcept
{
    return kind == SchemaObjectKind::Table || kind == SchemaObjectKind::View || kind == SchemaObjectKind::Dictionary;
}

template <typename RequireExactAuthorization, typename RequireDatabaseDiagnosticsAuthorization>
StoragePtr findAuthorizedLivePhysicalizationTable(
    DatabaseAtomic & database,
    const SchemaObjectID & object,
    RequireExactAuthorization && require_exact_authorization,
    RequireDatabaseDiagnosticsAuthorization && require_database_diagnostics_authorization)
{
    const auto require_diagnostics_and_rethrow = [&](std::exception_ptr error)
    {
        std::invoke(require_database_diagnostics_authorization);
        std::rethrow_exception(error);
    };

    if (!isMappedStorageObjectKind(object.kind) || object.database_uuid != database.getUUID() || object.object_uuid == UUIDHelpers::Nil)
    {
        std::invoke(require_database_diagnostics_authorization);
        logicalError("physicalization selected a non-local storage identity");
    }

    DatabasePtr mapped_database;
    StoragePtr table;
    try
    {
        std::tie(mapped_database, table) = DatabaseCatalog::instance().tryGetByUUID(object.object_uuid);
    }
    catch (...)
    {
        require_diagnostics_and_rethrow(std::current_exception());
    }
    if (!mapped_database || mapped_database.get() != std::addressof(database) || !table)
    {
        std::invoke(require_database_diagnostics_authorization);
        logicalError("the physicalization-selected Atomic table is absent from the live catalog");
    }

    const auto storage_id = [&]
    {
        try
        {
            return table->getStorageID();
        }
        catch (...)
        {
            require_diagnostics_and_rethrow(std::current_exception());
            throw;
        }
    }();
    if (storage_id.uuid != object.object_uuid || storage_id.database_name != database.getDatabaseName() || storage_id.table_name.empty()
        || !storageMatchesMappedObjectKind(*table, object.kind))
    {
        std::invoke(require_database_diagnostics_authorization);
        logicalError("the live Atomic storage identity differs from the physicalization authority root");
    }

    std::invoke(require_exact_authorization, std::string_view(storage_id.table_name));
    return table;
}
}

std::unique_ptr<const ILifecycleSnapshot> AtomicAuthority::acquireLifecycleSnapshot() const
{
    return std::make_unique<AtomicLifecycleSnapshot>(database_uuid, acquireCurrentRoot(), capabilities, nullptr, false);
}

AtomicLifecycleAdapter::AtomicLifecycleAdapter(DatabaseAtomic & database_)
    : database(database_)
{
}

AtomicLifecycleAdapter::~AtomicLifecycleAdapter()
{
    PhysicalizationTokenRouter::unregisterDatabase(database.getUUID());
    if (physicalization_tokens)
        physicalization_tokens->invalidateAllForRestart();
}

void AtomicLifecycleAdapter::configureEffectiveDatabaseResourceLimitsForStartup(const EffectiveResourceLimits & effective_limits)
{
    if (physicalization_tokens)
        physicalization_tokens->invalidateAllForRestart();
    physicalization_tokens
        = std::make_unique<PhysicalizationTokenStore>(database.getUUID(), makePhysicalizationTokenStoreLimits(effective_limits));
}

const TypeAuthorityCapabilities & AtomicLifecycleAdapter::getCapabilities() const noexcept
{
    return database.getSupportedUDTAuthorityCapabilities();
}

UUID AtomicLifecycleAdapter::getDatabaseUUID() const noexcept
{
    return database.getUUID();
}

void AtomicLifecycleAdapter::requireCapabilities(TypeAuthorityCapabilityMask required, std::string_view operation) const
{
    if (!getCapabilities().containsAll(required))
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Atomic user-defined type authority lacks capabilities required for {}", operation);
}

std::unique_ptr<const ILifecycleSnapshot> AtomicLifecycleAdapter::acquireSnapshot() const
{
    std::lock_guard lock(database.udt_authority_mutex);
    if (database.udt_authority_shutdown)
        rejectAfterShutdown("acquire a user-defined type lifecycle snapshot");
    if (database.udt_table_startup_state)
        rejectDuringMappedTableStartup("acquire a user-defined type lifecycle snapshot");
    if (database.udt_degraded_startup_status)
    {
        if (database.udt_authority || database.udt_verification_runtime || database.udt_verification_scheduler)
            logicalError("a degraded Atomic startup status coexists with executable authority components");
        return std::make_unique<AtomicLifecycleSnapshot>(database.db_uuid, database.udt_degraded_startup_status);
    }
    if (!database.udt_authority)
        return std::make_unique<AtomicLifecycleSnapshot>(database.db_uuid);

    AuthorityQuarantinePlan::Ptr quarantine;
    bool verification_runtime_fail_closed = false;
    if (database.udt_verification_runtime)
    {
        try
        {
            auto runtime = database.udt_verification_runtime->acquireSnapshot();
            quarantine = runtime.getQuarantine();
            verification_runtime_fail_closed = runtime.isFailClosed();
        }
        catch (const AuthorityVerificationRuntimeStateError &)
        {
            /// Introspection must not report ACTIVE after the runtime itself
            /// became unavailable. This is diagnostic-only and grants no
            /// operation; ordinary gates independently remain fail-closed.
            verification_runtime_fail_closed = true;
        }
    }
    else if (database.active_udt_authority.load(std::memory_order_acquire) == database.udt_authority.get())
        verification_runtime_fail_closed = true;

    return std::make_unique<AtomicLifecycleSnapshot>(
        database.db_uuid,
        database.udt_authority->acquireCurrentRoot(),
        database.getSupportedUDTAuthorityCapabilities(),
        std::move(quarantine),
        verification_runtime_fail_closed);
}

AtomicAuthority * AtomicLifecycleAdapter::executeMutationLocked(const AuthorityRoot * current_root, DefinitionMutationRequest request)
    TSA_REQUIRES(database.udt_schema_mutation_mutex)
{
    const UInt64 current_epoch = current_root ? current_root->getDatabaseCatalogEpoch() : 0;
    request.expected_database_catalog_epoch = current_epoch;
    /// The preliminary transaction ID is only a planner placeholder. In
    /// particular, a proven no-op must remain valid at the terminal catalog
    /// epoch; material mutations are rejected by the planner before I/O.
    request.transaction_id = current_epoch == std::numeric_limits<UInt64>::max() ? 1 : current_epoch + 1;

    DefinitionMutationPlannerLimits planner_limits;
    if (!current_root)
    {
        planner_limits.initial_effective_database_limits = database.getConfiguredUDTEffectiveDatabaseLimitsForFirstActivation();
        database.applyConfiguredUDTVerificationLimitsForFirstActivation(planner_limits.authority_root);
    }
    auto preliminary = DefinitionMutationPlanner::plan(current_root, request, planner_limits);
    if (preliminary.isNoOp())
        return nullptr;

    const auto operation = definitionMutationOperationName(request.kind);
    if (current_root)
    {
        const auto touched = collectDefinitionMutationTouchSet(*current_root, preliminary.getReplacementRoot());
        database.assertUDTTypeLifecycleOperationAllowed(current_root, touched, operation);
    }
    else
    {
        database.assertUDTTypeLifecycleOperationAllowed(nullptr, std::span<const SchemaObjectID>{}, operation);
    }

    const String database_name = database.getDatabaseName();
    const AuthorityState expected_after_state = preliminary.getReplacementRoot().getAuthorityState();
    const UInt64 expected_generation = preliminary.getReplacementRoot().getTypeIndexGeneration();
    const Digest expected_content_digest = preliminary.getReplacementRoot().getTypeIndexContentDigest();

    AtomicAuthority * authority = nullptr;
    AtomicDatabaseSchemaMutationStorage * storage = nullptr;
    AuthorityVerificationRuntimeState * installed_verification_runtime = nullptr;
    AuthorityVerificationScheduler * installed_verification_scheduler = nullptr;
    bool installed_first_activation_components = false;
    {
        std::lock_guard lock(database.udt_authority_mutex);
        if (database.udt_authority_shutdown)
            logicalError("a schema mutation started after database shutdown");
        if (database.udt_table_startup_state)
            rejectDuringMappedTableStartup("mutate user-defined types");
        if (database.udt_degraded_startup_status)
            rejectDuringDegradedAuthorityStartup("mutate user-defined types");
        if (current_root && !database.udt_authority)
            logicalError("a pinned authority root has no owning Atomic authority");
        if (database.udt_mutation_storage && !database.udt_authority)
            logicalError("durable mutation storage exists without an Atomic authority holder");

        if (!database.udt_mutation_storage)
        {
            auto new_storage = std::make_unique<AtomicDatabaseSchemaMutationStorage>(
                database.getDisk(), database.db_uuid, database.metadata_path, database_name);
            database.udt_mutation_storage = std::move(new_storage);
        }

        if (!database.udt_authority)
        {
            try
            {
                /// First publication uses the same component factory as
                /// startup recovery. This keeps the inactive authority,
                /// verification runtime, scheduler, and durable scheduler-policy
                /// staging indivisible under the authority
                /// mutex; publication below is still what makes them active.
                authority = std::addressof(database.initializeUDTAuthorityUnlocked(nullptr, false));
                installed_verification_runtime = database.udt_verification_runtime.get();
                installed_verification_scheduler = database.udt_verification_scheduler.get();
                installed_first_activation_components = true;
            }
            catch (...)
            {
                /// No mutation guard or durable staging exists yet. Do not
                /// leave storage without its authority/runtime holders when
                /// component construction fails.
                database.udt_mutation_storage.reset();
                throw;
            }
        }
        else
        {
            authority = database.udt_authority.get();
        }
        storage = database.udt_mutation_storage.get();
    }

    const bool first_activation = current_root == nullptr;
    std::optional<PreparedAtomicDatabaseUDTConfigurationV2> first_activation_configuration;
    const auto rollback_first_activation_components = [&]() noexcept
    {
        if (!installed_first_activation_components)
            return;

        /// The move-only configuration capability first removes only this
        /// attempt's pre-activation V2 files after proving that no durable
        /// Prepare/marker owns them. Indeterminate durability never enters
        /// this rollback path and therefore retains exact startup evidence.
        first_activation_configuration.reset();

        try
        {
            if (storage->hasDurableAuthorityMarker())
                return;
            static_cast<void>(storage->cleanupNeverEnabledScaffold());
            if (storage->hasDurableAuthorityMarker())
                return;
        }
        catch (...)
        {
            logNeverEnabledScaffoldCleanupFailureNoThrow(database.getUUID(), static_cast<Int32>(getCurrentExceptionCode()));
            return;
        }

        std::unique_ptr<AuthorityVerificationScheduler> retired_scheduler;
        std::unique_ptr<AuthorityVerificationRuntimeState> retired_runtime;
        std::unique_ptr<AtomicAuthority> retired_authority;
        std::unique_ptr<AtomicDatabaseSchemaMutationStorage> retired_storage;
        {
            std::lock_guard lock(database.udt_authority_mutex);
            if (database.udt_authority_shutdown || database.udt_table_startup_state
                || database.active_udt_authority.load(std::memory_order_acquire)
                || database.active_udt_verification_runtime.load(std::memory_order_acquire) || database.udt_authority.get() != authority
                || database.udt_mutation_storage.get() != storage
                || database.udt_verification_runtime.get() != installed_verification_runtime
                || database.udt_verification_scheduler.get() != installed_verification_scheduler)
            {
                return;
            }

            database.udt_authority->setPublicationObserver(nullptr);
            retired_scheduler = std::move(database.udt_verification_scheduler);
            retired_runtime = std::move(database.udt_verification_runtime);
            retired_authority = std::move(database.udt_authority);
            retired_storage = std::move(database.udt_mutation_storage);
            installed_first_activation_components = false;
        }

        /// The dormant scheduler still owns a background-task holder. Drain
        /// dependents before their runtime/authority/storage owners and never
        /// run these destructors while holding the authority mutex.
        if (retired_scheduler)
            retired_scheduler->shutdownAndDrain();
        if (retired_runtime)
            retired_runtime->shutdownAndDrain();
        if (retired_authority)
            retired_authority->shutdownAndDrain();
        retired_scheduler.reset();
        retired_runtime.reset();
        retired_authority.reset();
        retired_storage.reset();
    };

    std::optional<AtomicAuthority::PreparedPublication> publication;
    try
    {
        publication.emplace(authority->preparePublication(preliminary.releaseReplacementRoot()));
    }
    catch (...)
    {
        rollback_first_activation_components();
        throw;
    }

    if (first_activation)
    {
        try
        {
            first_activation_configuration.emplace(database.prepareConfiguredUDTConfigurationForFirstActivationV2());
        }
        catch (...)
        {
            rollback_first_activation_components();
            throw;
        }
    }

    if (current_root)
        storage->maintainCheckpointBeforeMutation(*current_root);

    std::optional<DatabaseSchemaMutationGuard> guard;
    try
    {
        guard.emplace(storage->issueMutationGuard());
    }
    catch (...)
    {
        if (first_activation)
            rollback_first_activation_components();
        throw;
    }

    const auto discard_unprepared = [&]
    {
        discardUnpreparedDatabaseSchemaMutationStaging(*storage, *guard, request.transaction_id);
        if (first_activation)
            rollback_first_activation_components();
    };

    const UInt64 durable_predecessor = guard->getDurablePredecessorTransactionID();
    if (durable_predecessor == std::numeric_limits<UInt64>::max())
    {
        if (first_activation)
            rollback_first_activation_components();
        invalid("durable schema transaction ID domain is exhausted");
    }
    const UInt64 transaction_id = durable_predecessor + 1;

    PreparedDefinitionMutation * durable_prepared = std::addressof(preliminary);
    std::optional<PreparedDefinitionMutation> rebound;
    if (transaction_id != request.transaction_id)
    {
        request.transaction_id = transaction_id;
        try
        {
            rebound.emplace(DefinitionMutationPlanner::plan(current_root, request, planner_limits));
            if (rebound->isNoOp())
                logicalError("transaction-ID rebinding changed a material mutation into a no-op");
            const auto & rebound_root = rebound->getReplacementRoot();
            if (rebound_root.getAuthorityState() != expected_after_state || rebound_root.getTypeIndexGeneration() != expected_generation
                || rebound_root.getTypeIndexContentDigest() != expected_content_digest)
            {
                logicalError("transaction-ID rebinding changed the prepared publication value");
            }
            auto unused_replacement = rebound->releaseReplacementRoot();
            unused_replacement.reset();
            durable_prepared = std::addressof(*rebound);
        }
        catch (...)
        {
            discard_unprepared();
            throw;
        }
    }

    try
    {
        static_cast<void>(executeDatabaseSchemaMutation(*storage, *guard, durable_prepared->getValidatedTransition()));
    }
    catch (...)
    {
        const auto original = std::current_exception();
        if (guard->getState() == DatabaseSchemaMutationGuard::State::Ready)
            discard_unprepared();
        std::rethrow_exception(original);
    }

    if (first_activation)
    {
        if (!first_activation_configuration)
            std::terminate();
        first_activation_configuration->disarmAfterDurableActivation();
    }
    authority->publish(std::move(*publication));
    if (first_activation)
        database.activateUDTAuthorityAfterFirstPublication();
    return authority;
}

void AtomicLifecycleAdapter::createOrAttach(const ASTCreateTypeQuery & query, const LifecycleActor & actor)
{
    AtomicAuthority * authority_to_scan = nullptr;
    {
        UniqueLock schema_lock(database.udt_schema_mutation_mutex);
        const String database_name = database.getDatabaseName();
        validateQualifiedMutation(database_name, query.getDatabase(), query.cluster);
        const String local_name = query.getTypeName();
        if (local_name.empty())
            invalid("CREATE or ATTACH TYPE local name is empty");
        if (query.attach && !actor.internal_query)
            invalid("ATTACH TYPE requires an authenticated internal query");
        if (!query.attach && (query.uuid || query.revision || query.definition_hash))
            invalid("CREATE TYPE carries ATTACH-only internal identity fields");
        if (query.attach && (!query.uuid || !query.revision || !query.definition_hash))
            invalid("ATTACH TYPE is missing UUID, revision, or definition hash");

        std::optional<AtomicAuthority::RootSnapshot> current_snapshot;
        {
            std::lock_guard lock(database.udt_authority_mutex);
            if (database.udt_authority_shutdown)
                rejectAfterShutdown("CREATE or ATTACH TYPE");
            if (database.udt_table_startup_state)
                rejectDuringMappedTableStartup("CREATE or ATTACH TYPE");
            if (database.udt_degraded_startup_status)
                rejectDuringDegradedAuthorityStartup("CREATE or ATTACH TYPE");
            if (database.udt_authority)
                current_snapshot.emplace(database.udt_authority->acquireCurrentRoot());
        }
        const CompositeRoot * current_root = current_snapshot && *current_snapshot ? std::addressof(current_snapshot->get()) : nullptr;
        const auto * existing_record = findRecord(current_root, local_name);
        if (existing_record && !query.if_not_exists)
            invalid("a user-defined type with the requested normalized local name already exists");

        DefinitionIdentity identity{
            .database_uuid = database.db_uuid,
            .type_uuid = query.attach ? *query.uuid : UUIDHelpers::generateV4(),
            .revision = query.attach ? *query.revision : 1,
        };
        if (existing_record && query.if_not_exists)
        {
            if (query.attach && existing_record->identity.revision != identity.revision)
                invalid("ATTACH TYPE IF NOT EXISTS does not match the existing exact internal revision");
            if (!query.attach)
                identity = existing_record->identity;
        }

        const EffectiveResourceLimits & effective_limits = current_root
            ? current_root->getDatabaseResourceQuota().getLimits()
            : database.getConfiguredUDTEffectiveDatabaseLimitsForFirstActivation();
        const EffectiveResourceLimits & validation_limits
            = existing_record && query.if_not_exists ? implementationEffectiveResourceLimits() : effective_limits;
        auto bindings = makeBindings(current_root, database_name, local_name);
        auto prepared_bindings = prepareDefinitionLoweringBindings(
            database.db_uuid, database_name, std::move(bindings), atomicDefinitionLoweringLimits(validation_limits));
        DefinitionInput requested_input
            = lowerCreateTypeQueryToDefinitionInput(query, identity, structuredName(database_name, local_name), prepared_bindings);

        auto checker_inputs = copyCurrentDefinitionInputs(current_root);
        if (existing_record)
        {
            const auto current_records = current_root->getDefinitionRecords();
            const auto existing = std::find_if(
                current_records.begin(),
                current_records.end(),
                [&](const Record & record) { return record.identity == existing_record->identity; });
            if (existing == current_records.end())
                logicalError("the existing name collision is absent from the current record span");
            checker_inputs[static_cast<std::size_t>(existing - current_records.begin())] = std::move(requested_input);
            /// This is a hypothetical full-catalog check used only to prove an
            /// IF NOT EXISTS no-op or conflict. Re-derive every compositional
            /// dependency hash so a mismatching requested body reaches the
            /// planner's strict DefinitionConflict branch instead of failing
            /// incidentally in one transitive dependent.
            for (auto & input : checker_inputs)
            {
                for (auto & dependency : input.dependencies)
                {
                    if (dependency.type_uuid == existing_record->identity.type_uuid
                        && dependency.revision == existing_record->identity.revision)
                    {
                        dependency.type_uuid = identity.type_uuid;
                        dependency.revision = identity.revision;
                    }
                    dependency.target_definition_hash = {};
                }
            }
        }
        else
        {
            checker_inputs.push_back(std::move(requested_input));
        }

        auto checked = TemplateChecker::checkAll(std::move(checker_inputs), atomicCheckerLimits(validation_limits));
        auto after_definition = findCheckedDefinition(checked, identity);
        if (query.attach && lowerHexDigest(after_definition->getDefinitionHash()) != *query.definition_hash)
            invalid("ATTACH TYPE definition hash differs from freshly checked executable semantics");
        auto after_record = makeCreateRecord(query, database_name, *after_definition, actor, current_root);

        if (!existing_record)
        {
            std::vector<Record> prospective_records;
            if (current_root)
                prospective_records.assign(current_root->getDefinitionRecords().begin(), current_root->getDefinitionRecords().end());
            prospective_records.push_back(after_record);
            validateCanonicalRecordSetAgainstCheckedDefinitions(
                database.db_uuid, database_name, checked, prospective_records, effective_limits);
        }

        MutationRequest request;
        request.kind = MutationKind::Create;
        request.database_uuid = database.db_uuid;
        request.after_definition = std::move(after_definition);
        request.after_record = std::move(after_record);
        request.if_not_exists = query.if_not_exists;
        request.require_exact_type_uuid_on_noop = query.attach && query.if_not_exists;
        authority_to_scan = executeMutationLocked(current_root, std::move(request));
    }

    if (authority_to_scan)
    {
        try
        {
            static_cast<void>(authority_to_scan->scanRetired());
        }
        catch (...)
        {
        }
    }
}

void AtomicLifecycleAdapter::rename(const ASTRenameTypeQuery & query, const LifecycleActor &)
{
    AtomicAuthority * authority_to_scan = nullptr;
    {
        UniqueLock schema_lock(database.udt_schema_mutation_mutex);
        const String database_name = database.getDatabaseName();
        validateQualifiedMutation(database_name, query.getDatabase(), query.cluster);
        const String old_local_name = query.getTypeName();
        const String new_local_name = query.getNewTypeName();
        if (old_local_name.empty() || new_local_name.empty())
            invalid("ALTER TYPE RENAME requires two nonempty local names");

        std::optional<AtomicAuthority::RootSnapshot> current_snapshot;
        {
            std::lock_guard lock(database.udt_authority_mutex);
            if (database.udt_authority_shutdown)
                rejectAfterShutdown("ALTER TYPE RENAME");
            if (database.udt_table_startup_state)
                rejectDuringMappedTableStartup("ALTER TYPE RENAME");
            if (database.udt_degraded_startup_status)
                rejectDuringDegradedAuthorityStartup("ALTER TYPE RENAME");
            if (database.udt_authority)
                current_snapshot.emplace(database.udt_authority->acquireCurrentRoot());
        }
        const CompositeRoot * current_root = current_snapshot && *current_snapshot ? std::addressof(current_snapshot->get()) : nullptr;
        const auto * before_record = findRecord(current_root, old_local_name);
        if (!before_record)
        {
            if (query.if_exists)
                return;
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown user-defined type {}.{}", database_name, old_local_name);
        }

        auto checker_inputs = copyCurrentDefinitionInputs(current_root);
        const auto records = current_root->getDefinitionRecords();
        const auto before = std::find_if(
            records.begin(), records.end(), [&](const Record & record) { return record.identity == before_record->identity; });
        if (before == records.end())
            logicalError("rename source is absent from the current record span");
        auto & renamed_input = checker_inputs[static_cast<std::size_t>(before - records.begin())];
        renamed_input.normalized_local_name = new_local_name;
        renamed_input.normalized_name = database_name + "." + new_local_name;

        /// RENAME must be able to re-prove an already-published root after a
        /// policy decrease. Immutable implementation limits validate the
        /// exact semantics; the root quota transition below independently
        /// rejects every forbidden growth while OVER_QUOTA.
        const auto & validation_limits = implementationEffectiveResourceLimits();
        auto checked = TemplateChecker::checkAll(std::move(checker_inputs), atomicCheckerLimits(validation_limits));
        auto after_definition = findCheckedDefinition(checked, before_record->identity);
        auto after_record = makeRenamePresentationRecord(
            *before_record, database_name, new_local_name, *after_definition, *current_root, before_record->identity, new_local_name);

        std::vector<Record> dependent_record_rewrites;
        for (const auto & record : records)
        {
            if (record.identity == before_record->identity)
                continue;
            const bool directly_depends_on_target = std::ranges::any_of(
                record.dependencies,
                [&](const DefinitionDependency & dependency)
                {
                    return dependency.type_uuid == before_record->identity.type_uuid
                        && dependency.revision == before_record->identity.revision
                        && dependency.target_definition_hash == before_record->definition_hash;
                });
            if (!directly_depends_on_target)
                continue;
            auto dependent_definition = findCheckedDefinition(checked, record.identity);
            dependent_record_rewrites.push_back(makeRenamePresentationRecord(
                record,
                database_name,
                record.normalized_local_name,
                *dependent_definition,
                *current_root,
                before_record->identity,
                new_local_name));
        }


        std::vector<Record> prospective_records(records.begin(), records.end());
        const auto replace_record = [&](const Record & replacement)
        {
            const auto existing = std::find_if(
                prospective_records.begin(),
                prospective_records.end(),
                [&](const Record & record) { return record.identity == replacement.identity; });
            if (existing == prospective_records.end())
                logicalError("RENAME presentation rewrite identity disappeared from the current record set");
            *existing = replacement;
        };
        replace_record(after_record);
        for (const auto & rewrite : dependent_record_rewrites)
            replace_record(rewrite);
        validateCanonicalRecordSetAgainstCheckedDefinitions(
            database.db_uuid, database_name, checked, prospective_records, validation_limits);

        MutationRequest request;
        request.kind = MutationKind::Rename;
        request.database_uuid = database.db_uuid;
        request.expected_before_identity = before_record->identity;
        request.after_definition = std::move(after_definition);
        request.after_record = std::move(after_record);
        request.rename_dependent_record_rewrites = std::move(dependent_record_rewrites);
        authority_to_scan = executeMutationLocked(current_root, std::move(request));
    }

    if (authority_to_scan)
    {
        try
        {
            static_cast<void>(authority_to_scan->scanRetired());
        }
        catch (...)
        {
        }
    }
}

void AtomicLifecycleAdapter::comment(const ASTAlterTypeCommentQuery & query, const LifecycleActor &)
{
    AtomicAuthority * authority_to_scan = nullptr;
    {
        std::lock_guard schema_lock(database.udt_schema_mutation_mutex);
        const String database_name = database.getDatabaseName();
        validateQualifiedMutation(database_name, query.getDatabase(), query.cluster);
        const String local_name = query.getTypeName();
        if (local_name.empty())
            invalid("ALTER TYPE COMMENT requires a nonempty local name");
        const String new_comment = commentFromAlterQuery(query);

        std::optional<AtomicAuthority::RootSnapshot> current_snapshot;
        {
            std::lock_guard lock(database.udt_authority_mutex);
            if (database.udt_authority_shutdown)
                rejectAfterShutdown("ALTER TYPE COMMENT");
            if (database.udt_table_startup_state)
                rejectDuringMappedTableStartup("ALTER TYPE COMMENT");
            if (database.udt_degraded_startup_status)
                rejectDuringDegradedAuthorityStartup("ALTER TYPE COMMENT");
            if (database.udt_authority)
                current_snapshot.emplace(database.udt_authority->acquireCurrentRoot());
        }
        const CompositeRoot * current_root = current_snapshot && *current_snapshot ? std::addressof(current_snapshot->get()) : nullptr;
        const auto * before_record = findRecord(current_root, local_name);
        if (!before_record)
        {
            if (query.if_exists)
                return;
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown user-defined type {}.{}", database_name, local_name);
        }
        if (before_record->comment == new_comment)
            return;

        auto after_definition = current_root->findByIdentity(before_record->identity);
        if (!after_definition)
            logicalError("comment target has no checked definition in the current root");
        auto after_record = makeCommentPresentationRecord(*before_record, new_comment);

        MutationRequest request;
        request.kind = MutationKind::Comment;
        request.database_uuid = database.db_uuid;
        request.expected_before_identity = before_record->identity;
        request.after_definition = std::move(after_definition);
        request.after_record = std::move(after_record);
        authority_to_scan = executeMutationLocked(current_root, std::move(request));
    }

    if (authority_to_scan)
    {
        try
        {
            static_cast<void>(authority_to_scan->scanRetired());
        }
        catch (...)
        {
        }
    }
}

void AtomicLifecycleAdapter::dropRestrict(const ASTDropTypeQuery & query, const LifecycleActor &)
{
    AtomicAuthority * authority_to_scan = nullptr;
    {
        std::lock_guard schema_lock(database.udt_schema_mutation_mutex);
        const String database_name = database.getDatabaseName();
        validateQualifiedMutation(database_name, query.getDatabase(), query.cluster);
        const String local_name = query.getTypeName();
        if (local_name.empty())
            invalid("DROP TYPE local name is empty");

        std::optional<AtomicAuthority::RootSnapshot> current_snapshot;
        {
            std::lock_guard lock(database.udt_authority_mutex);
            if (database.udt_authority_shutdown)
                rejectAfterShutdown("DROP TYPE");
            if (database.udt_table_startup_state)
                rejectDuringMappedTableStartup("DROP TYPE");
            if (database.udt_degraded_startup_status)
                rejectDuringDegradedAuthorityStartup("DROP TYPE");
            if (database.udt_authority)
                current_snapshot.emplace(database.udt_authority->acquireCurrentRoot());
        }
        const CompositeRoot * current_root = current_snapshot && *current_snapshot ? std::addressof(current_snapshot->get()) : nullptr;
        const auto * before_record = findRecord(current_root, local_name);
        if (!before_record)
        {
            if (query.if_exists)
                return;
            throw Exception(ErrorCodes::UNKNOWN_TYPE, "Unknown user-defined type {}.{}", database_name, local_name);
        }

        MutationRequest request;
        request.kind = MutationKind::Drop;
        request.database_uuid = database.db_uuid;
        request.expected_before_identity = before_record->identity;
        authority_to_scan = executeMutationLocked(current_root, std::move(request));
    }

    if (authority_to_scan)
    {
        try
        {
            static_cast<void>(authority_to_scan->scanRetired());
        }
        catch (...)
        {
        }
    }
}

}
