#include <Interpreters/UDTScalarAliasColumnBinder.h>

#include <Access/Common/UDTAccessTarget.h>
#include <Access/UDTUsageAccess.h>

#include <Common/Exception.h>
#include <Common/checkStackSize.h>

#include <DataTypes/BuiltInDataTypeFamilyClassifier.h>
#include <DataTypes/UDT/CanonicalTypeArguments.h>
#include <DataTypes/UDT/IAuthorityAdapter.h>
#include <DataTypes/UDT/ResourceAccounting.h>
#include <DataTypes/UDT/ResourceLimitAdapters.h>
#include <DataTypes/UDT/TableColumnTypeAlterBindings.h>
#include <DataTypes/UDT/TemplateSpecializer.h>
#include <DataTypes/UDT/TypeResolver.h>
#include <DataTypes/dataTypeToAST.h>

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTEnumDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Parsers/ASTTupleDataType.h>
#include <Parsers/ASTUDTReference.h>

#include <algorithm>
#include <array>
#include <limits>
#include <map>
#include <optional>
#include <set>
#include <span>
#include <unordered_map>
#include <utility>
#include <vector>

namespace DB::ErrorCodes
{
extern const int LIMIT_EXCEEDED;
}

namespace DB::UDT
{
namespace
{

using BinderError = ScalarAliasColumnBinderError;

[[noreturn]] void fail(BinderError::Code code, std::string_view message)
{
    throw BinderError(code, message);
}

[[noreturn]] void failLimit(std::string_view message)
{
    throw Exception(ErrorCodes::LIMIT_EXCEEDED, "{}", message);
}

void requireAdmission(const ResourceAdmissionResult & admission)
{
    if (!admission.isAccepted())
        failLimit(formatResourceAdmissionFailure(admission));
}

UInt64 resourceSize(std::size_t size) noexcept
{
    static_assert(sizeof(std::size_t) <= sizeof(UInt64));
    return static_cast<UInt64>(size);
}

UInt64 checkedResourceProduct(UInt64 lhs, UInt64 rhs)
{
    if (lhs != 0 && rhs > std::numeric_limits<UInt64>::max() / lhs)
        failLimit("UDT type-expression resource accounting overflows UInt64");
    return lhs * rhs;
}

template <typename Callback>
decltype(auto) translateResolutionLimitErrors(Callback && callback)
{
    try
    {
        return std::forward<Callback>(callback)();
    }
    catch (const TemplateSpecializerError & error)
    {
        if (error.code == TemplateSpecializerError::Code::LimitExceeded)
            failLimit(error.what());
        throw;
    }
    catch (const TypeResolverError & error)
    {
        if (error.code == TypeResolverError::Code::LimitExceeded)
            failLimit(error.what());
        throw;
    }
    catch (const DescriptorError & error)
    {
        if (error.code == DescriptorError::Code::LimitExceeded)
            failLimit(error.what());
        throw;
    }
}

/// Adapts one move-only authority session into arbitrarily many borrowed
/// sessions over the same immutable generation. Every TypeResolver and
/// TemplateSpecializer attempt created during one admission or query scope can
/// therefore use the ordinary authority ABI without acquiring a newer root.
/// Borrowed sessions never outlive this adapter.
class PinnedResolutionAuthorityAdapter final : public IAuthorityAdapter
{
public:
    explicit PinnedResolutionAuthorityAdapter(const IAuthorityAdapter & upstream_)
        : upstream(upstream_)
        , capabilities(upstream.getCapabilities())
        , database_uuid(upstream.getDatabaseUUID())
        , pinned_session(upstream.beginResolutionSession())
    {
    }

    const TypeAuthorityCapabilities & getCapabilities() const noexcept override { return capabilities; }
    UUID getDatabaseUUID() const noexcept override { return database_uuid; }

    ResolutionSession beginResolutionSession() const override
    {
        return makeSnapshotResolutionSession(
            this,
            SnapshotResolutionOperations{
                .find_by_identity = &findByIdentity,
                .find_by_name = &findByName,
                .get_generation = &getGeneration,
                .get_effective_resource_limits = &getEffectiveResourceLimits,
            });
    }

    void requireCapabilities(TypeAuthorityCapabilityMask required, std::string_view operation) const override
    {
        upstream.requireCapabilities(required, operation);
    }

    UInt64 getPinnedGeneration() const noexcept { return pinned_session.getGeneration(); }

private:
    static Definition::Ptr findByIdentity(const void * view, const DefinitionIdentity & identity)
    {
        return static_cast<const PinnedResolutionAuthorityAdapter *>(view)->pinned_session.findByIdentity(identity);
    }

    static Definition::Ptr findByName(const void * view, std::string_view name)
    {
        return static_cast<const PinnedResolutionAuthorityAdapter *>(view)->pinned_session.findByName(name);
    }

    static UInt64 getGeneration(const void * view) noexcept
    {
        return static_cast<const PinnedResolutionAuthorityAdapter *>(view)->pinned_session.getGeneration();
    }

    static const EffectiveResourceLimits * getEffectiveResourceLimits(const void * view) noexcept
    {
        return static_cast<const PinnedResolutionAuthorityAdapter *>(view)->pinned_session.getEffectiveResourceLimits();
    }

public:
    const EffectiveResourceLimits * getPinnedEffectiveResourceLimits() const noexcept
    {
        return pinned_session.getEffectiveResourceLimits();
    }

private:
    const IAuthorityAdapter & upstream;
    const TypeAuthorityCapabilities capabilities;
    const UUID database_uuid;
    const ResolutionSession pinned_session;
};

bool containsUDTReferenceRecursively(const IAST & root)
{
    checkStackSize();
    if (root.as<ASTUDTReference>())
        return true;
    for (const auto & child : root.children)
        if (child && containsUDTReferenceRecursively(*child))
            return true;
    return false;
}

bool containsUDTReference(const ASTPtr & root)
{
    if (!root)
        return false;
    if (root->as<ASTUDTReference>())
        return true;

    struct Frame
    {
        const IAST * node = nullptr;
        size_t next_child = 0;
    };

    constexpr size_t maximum_routing_depth = TypeResolverLimits{}.maximum_declaration_ast_depth;
    static_assert(maximum_routing_depth != 0);
    std::array<Frame, maximum_routing_depth> path{};
    size_t path_size = 1;
    path.front() = {.node = root.get()};
    while (path_size != 0)
    {
        auto & frame = path[path_size - 1];
        if (frame.next_child == frame.node->children.size())
        {
            --path_size;
            continue;
        }

        const auto & child = frame.node->children[frame.next_child++];
        if (!child)
            continue;
        if (child->as<ASTUDTReference>())
            return true;
        /// Keep the common resolver-supported tree iterative and allocation
        /// free. Overflow is scanned exactly through ClickHouse's stack-checked
        /// visitor convention, so a deeper built-in-only type stays inactive.
        if (path_size == path.size())
        {
            if (containsUDTReferenceRecursively(*child))
                return true;
            continue;
        }
        path[path_size++] = {.node = child.get()};
    }
    return false;
}

struct TypeExpressionResourceUsage
{
    UInt64 nodes = 0;
    UInt64 edges = 0;
};

struct TypeExpressionPendingNode
{
    const IAST * node = nullptr;
    UInt64 depth = 0;
};

struct SyntacticReferencePreflightUsage
{
    UInt64 nodes = 0;
    UInt64 edges = 0;
    UInt64 references = 0;
};

void preflightSyntacticReferenceDatabases(const ASTPtr & root, std::string_view database_name, SyntacticReferencePreflightUsage & usage)
{
    if (!root)
        return;

    constexpr TypeResolverLimits implementation_limits{};
    constexpr size_t maximum_depth = implementation_limits.maximum_declaration_ast_depth;
    static_assert(
        maximum_depth != 0 && implementation_limits.maximum_declaration_ast_nodes != 0
        && implementation_limits.maximum_declaration_ast_edges != 0 && implementation_limits.maximum_input_references != 0);

    struct Frame
    {
        const IAST * node = nullptr;
        size_t next_child = 0;
    };

    std::array<Frame, maximum_depth> path{};
    size_t path_size = 0;
    const auto enter = [&](const IAST * node)
    {
        if (path_size == path.size())
            failLimit("UDT declaration AST depth exceeds the resolver implementation limit");
        if (usage.nodes == implementation_limits.maximum_declaration_ast_nodes)
            failLimit("UDT declaration AST nodes exceed the resolver implementation limit");
        ++usage.nodes;

        if (const auto * reference = node->as<ASTUDTReference>())
        {
            if (reference->database_name != database_name)
                fail(BinderError::Code::CrossDatabaseReference, "a UDT expression cannot span database authorities");
            if (usage.references == implementation_limits.maximum_input_references)
                failLimit("UDT occurrences exceed the resolver implementation limit");
            ++usage.references;
        }

        const UInt64 child_edges = resourceSize(node->children.size());
        if (usage.edges > implementation_limits.maximum_declaration_ast_edges
            || child_edges > implementation_limits.maximum_declaration_ast_edges - usage.edges)
        {
            failLimit("UDT declaration AST edges exceed the resolver implementation limit");
        }
        usage.edges += child_edges;
        path[path_size++] = {.node = node};
    };

    enter(root.get());
    while (path_size != 0)
    {
        auto & frame = path[path_size - 1];
        if (frame.next_child == frame.node->children.size())
        {
            --path_size;
            continue;
        }
        const auto & child = frame.node->children[frame.next_child++];
        if (child)
            enter(child.get());
    }
}

void preflightSyntacticReferenceDatabases(
    ASTCreateQuery * create_source, std::span<ASTColumnDeclaration * const> declarations, std::string_view database_name)
{
    SyntacticReferencePreflightUsage usage;
    if (create_source)
    {
        if (!create_source->columns_list || !create_source->columns_list->columns || create_source->columns_list->columns->children.empty())
            fail(BinderError::Code::InvalidInput, "an activated CREATE TABLE has no explicit columns");
        for (const auto & child : create_source->columns_list->columns->children)
        {
            const auto * declaration = child ? child->as<ASTColumnDeclaration>() : nullptr;
            if (!declaration)
                fail(BinderError::Code::UnsupportedColumnShape, "an activated CREATE TABLE column declaration is malformed");
            preflightSyntacticReferenceDatabases(declaration->getType(), database_name, usage);
        }
        return;
    }

    for (const auto * declaration : declarations)
    {
        if (!declaration)
            fail(BinderError::Code::UnsupportedColumnShape, "an activated table column declaration is malformed");
        preflightSyntacticReferenceDatabases(declaration->getType(), database_name, usage);
    }
}

constexpr UInt64 type_expression_node_scratch_bytes = sizeof(IAST) + sizeof(TypeExpressionPendingNode) + 2 * sizeof(ASTPtr);

void addTypeExpressionResourceDelta(ResourceDelta & delta, const TypeExpressionResourceUsage & usage)
{
    /// Declaration-AST traversal is resolver/checker work, not a generation-
    /// local semantic (node, path) proof or semantic transfer edge.
    delta.add(ResourceLimit::CheckerExpansionWorkUnits, usage.nodes);
    delta.add(ResourceLimit::CheckerExpansionWorkUnits, usage.edges);
    delta.add(ResourceLimit::SemanticScratchBytesPerQuery, checkedResourceProduct(usage.nodes, type_expression_node_scratch_bytes));
    delta.add(ResourceLimit::SemanticScratchBytesPerQuery, checkedResourceProduct(usage.edges, static_cast<UInt64>(sizeof(ASTPtr))));
}

void collectUDTReferences(
    const ASTPtr & root,
    std::string_view database_name,
    const TypeResolverLimits & limits,
    ProspectiveResourceBudget & query_budget,
    std::vector<const ASTUDTReference *> & output)
{
    if (!root)
        return;
    if (limits.maximum_input_references == 0 || limits.maximum_declaration_ast_nodes == 0 || limits.maximum_declaration_ast_edges == 0
        || limits.maximum_declaration_ast_depth == 0)
    {
        failLimit("UDT declaration AST limits must be nonzero");
    }

    TypeExpressionResourceUsage usage{.nodes = 1};
    ResourceDelta root_delta;
    addTypeExpressionResourceDelta(root_delta, usage);
    requireAdmission(query_budget.charge(root_delta));

    std::vector<TypeExpressionPendingNode> pending;
    pending.push_back({.node = root.get(), .depth = 1});
    while (!pending.empty())
    {
        const auto current = pending.back();
        pending.pop_back();
        const IAST * node = current.node;
        if (const auto * reference = node->as<ASTUDTReference>())
        {
            if (reference->database_name != database_name)
                fail(BinderError::Code::CrossDatabaseReference, "a UDT expression cannot span database authorities");
            if (reference->type_name.empty())
                fail(BinderError::Code::UnsupportedColumnShape, "a UDT expression has an empty local name");
            if (output.size() >= limits.maximum_input_references)
                failLimit("UDT occurrences exceed the resolver input limit");
            requireAdmission(query_budget.charge(ResourceLimit::SemanticScratchBytesPerQuery, static_cast<UInt64>(sizeof(reference))));
            output.push_back(reference);
        }

        const UInt64 edge_count = resourceSize(node->children.size());
        if (usage.edges > limits.maximum_declaration_ast_edges || edge_count > limits.maximum_declaration_ast_edges - usage.edges)
        {
            failLimit("UDT declaration AST edges exceed the resolver limit");
        }
        ResourceDelta edge_delta;
        addTypeExpressionResourceDelta(edge_delta, TypeExpressionResourceUsage{.edges = edge_count});
        requireAdmission(query_budget.charge(edge_delta));
        UInt64 child_count = 0;
        for (const auto & child : node->children)
            if (child)
                ++child_count;
        if (child_count != 0 && current.depth >= limits.maximum_declaration_ast_depth)
            failLimit("UDT declaration AST depth exceeds the resolver limit");
        if (usage.nodes > limits.maximum_declaration_ast_nodes || child_count > limits.maximum_declaration_ast_nodes - usage.nodes)
        {
            failLimit("UDT declaration AST nodes exceed the resolver limit");
        }
        TypeExpressionResourceUsage children_usage{.nodes = child_count};
        ResourceDelta children_delta;
        addTypeExpressionResourceDelta(children_delta, children_usage);
        requireAdmission(query_budget.charge(children_delta));
        usage.nodes += child_count;
        usage.edges += edge_count;
        for (const auto & child : node->children)
            if (child)
                pending.push_back({.node = child.get(), .depth = current.depth + 1});
    }
}

struct SourceColumn
{
    ASTColumnDeclaration * declaration = nullptr;
    ASTPtr declared_type;
    std::vector<const ASTUDTReference *> references;
};

std::vector<SourceColumn> collectAndValidateSourceColumns(
    std::span<ASTColumnDeclaration * const> declarations,
    std::string_view database_name,
    const TypeResolverLimits & resolver_limits,
    ProspectiveResourceBudget & query_budget)
{
    if (database_name.empty())
        fail(BinderError::Code::InvalidInput, "the owning database name is empty");
    if (declarations.empty())
        fail(BinderError::Code::InvalidInput, "an activated table column binding has no declarations");

    requireAdmission(query_budget.charge(
        ResourceLimit::SemanticScratchBytesPerQuery,
        checkedResourceProduct(resourceSize(declarations.size()), static_cast<UInt64>(sizeof(SourceColumn)))));
    std::vector<SourceColumn> columns;
    columns.reserve(declarations.size());
    for (auto * declaration : declarations)
    {
        if (!declaration || declaration->name.empty())
            fail(BinderError::Code::UnsupportedColumnShape, "an activated table column declaration is malformed");
        if (declaration->default_specifier == ColumnDefaultSpecifier::Alias
            || declaration->default_specifier == ColumnDefaultSpecifier::Ephemeral)
        {
            fail(BinderError::Code::UnsupportedColumnShape, "the initial UDT table slice does not admit ALIAS or EPHEMERAL columns");
        }
        if (declaration->null_modifier.has_value())
            fail(BinderError::Code::UnsupportedColumnShape, "the initial UDT table slice does not admit column NULL modifiers");

        ASTPtr declared_type = declaration->getType();
        if (!declared_type)
            fail(BinderError::Code::UnsupportedColumnShape, "every activated table column must have an explicit type");

        std::vector<const ASTUDTReference *> references;
        collectUDTReferences(declared_type, database_name, resolver_limits, query_budget, references);

        columns.push_back({
            .declaration = declaration,
            .declared_type = std::move(declared_type),
            .references = std::move(references),
        });
    }
    return columns;
}

bool binaryLess(std::string_view lhs, std::string_view rhs) noexcept
{
    return std::lexicographical_compare(
        lhs.begin(),
        lhs.end(),
        rhs.begin(),
        rhs.end(),
        [](char left, char right) { return static_cast<unsigned char>(left) < static_cast<unsigned char>(right); });
}

bool isExactDataTypeAST(const ASTPtr & ast) noexcept
{
    return ast && (ast->as<ASTDataType>() || ast->as<ASTTupleDataType>() || ast->as<ASTEnumDataType>());
}

const ASTExpressionList * getTypeArguments(const ASTPtr & ast)
{
    const auto * data_type = ast ? ast->as<ASTDataType>() : nullptr;
    const auto * tuple_type = ast ? ast->as<ASTTupleDataType>() : nullptr;
    const auto * enum_type = ast ? ast->as<ASTEnumDataType>() : nullptr;
    const ASTDataType * any_data_type = data_type ? data_type : (tuple_type ? static_cast<const ASTDataType *>(tuple_type) : enum_type);
    if (!any_data_type)
        fail(BinderError::Code::UnsupportedColumnShape, "a UDT TYPE argument contains an unsupported AST category");
    if (any_data_type->children.size() > 1)
        fail(BinderError::Code::UnsupportedColumnShape, "a UDT TYPE argument data-type node has multiple argument lists");
    if (any_data_type->children.empty())
        return nullptr;
    const auto * arguments = any_data_type->children.front()->as<ASTExpressionList>();
    if (!arguments)
        fail(BinderError::Code::UnsupportedColumnShape, "a UDT TYPE argument has a malformed argument list");
    return arguments;
}

ASTExpressionList * getMutableTypeArguments(ASTPtr & ast)
{
    auto * data_type = ast ? ast->as<ASTDataType>() : nullptr;
    auto * tuple_type = ast ? ast->as<ASTTupleDataType>() : nullptr;
    auto * enum_type = ast ? ast->as<ASTEnumDataType>() : nullptr;
    ASTDataType * any_data_type = data_type ? data_type : (tuple_type ? static_cast<ASTDataType *>(tuple_type) : enum_type);
    if (!any_data_type)
        fail(BinderError::Code::UnsupportedColumnShape, "a cloned UDT TYPE argument contains an unsupported AST category");
    if (any_data_type->children.size() > 1)
        fail(BinderError::Code::UnsupportedColumnShape, "a cloned UDT TYPE argument data-type node has multiple argument lists");
    if (any_data_type->children.empty())
        return nullptr;
    auto * arguments = any_data_type->children.front()->as<ASTExpressionList>();
    if (!arguments)
        fail(BinderError::Code::UnsupportedColumnShape, "a cloned UDT TYPE argument has a malformed argument list");
    return arguments;
}

std::string_view canonicalFamily(const ASTPtr & ast)
{
    BuiltInDataTypeFamilyClassification classification;
    if (const auto * tuple = ast ? ast->as<ASTTupleDataType>() : nullptr)
        classification = BuiltInDataTypeFamilyClassifier::classifySpecializedTuple(tuple->name);
    else if (const auto * enumeration = ast ? ast->as<ASTEnumDataType>() : nullptr)
        classification = BuiltInDataTypeFamilyClassifier::classifySpecializedEnum(enumeration->name);
    else if (const auto * data_type = ast ? ast->as<ASTDataType>() : nullptr)
        classification = BuiltInDataTypeFamilyClassifier::classifyGeneric(data_type->name);
    else
        fail(BinderError::Code::UnsupportedColumnShape, "a UDT TYPE argument contains an unsupported exact type node");

    if (!classification)
        fail(BinderError::Code::UnsupportedColumnShape, "an unbound family remains in a UDT TYPE argument");
    return classification.family->canonical_creator_name;
}

struct LogicalReferenceBuildState
{
    enum class State : UInt8
    {
        Unbuilt,
        Building,
        Built,
    };

    ASTPtr marker;
    Definition::Ptr definition;
    std::optional<DeclaredTypeReferenceInput> input;
    std::optional<TemplateSpecializationID> auxiliary_specialization;
    State state = State::Unbuilt;
};

constexpr UInt64 logical_reference_scratch_bytes = sizeof(LogicalReferenceBuildState) + sizeof(DeclaredTypeReferenceInput) + sizeof(IAST);
constexpr UInt64 resolver_column_scratch_bytes = sizeof(ASTPtr) + 2 * sizeof(std::size_t);

void chargeLogicalReferenceScratch(ProspectiveResourceBudget & query_budget, UInt64 reference_count, UInt64 column_count)
{
    ResourceDelta delta;
    delta.add(ResourceLimit::SemanticScratchBytesPerQuery, checkedResourceProduct(reference_count, logical_reference_scratch_bytes));
    delta.add(ResourceLimit::SemanticScratchBytesPerQuery, checkedResourceProduct(column_count, resolver_column_scratch_bytes));
    requireAdmission(query_budget.charge(delta));
}

ASTPtr rewriteLogicalReferences(
    ASTPtr node,
    const std::map<String, Definition::Ptr, std::less<>> & definitions_by_name,
    std::vector<LogicalReferenceBuildState> & references)
{
    if (!node)
        fail(BinderError::Code::UnsupportedColumnShape, "a UDT type expression contains a null AST node");

    if (const auto * logical = node->as<ASTUDTReference>())
    {
        const auto found = definitions_by_name.find(logical->type_name);
        if (found == definitions_by_name.end() || !found->second)
            fail(BinderError::Code::UnknownDefinition, "a UDT expression names no current user-defined type");

        auto marker = makeASTDataType(found->second->getNormalizedName());
        if (auto arguments = logical->getArguments())
        {
            ASTPtr rewritten_arguments = rewriteLogicalReferences(arguments, definitions_by_name, references);
            marker->children.push_back(std::move(rewritten_arguments));
        }
        references.push_back({
            .marker = marker,
            .definition = found->second,
            .input = std::nullopt,
            .auxiliary_specialization = std::nullopt,
            .state = LogicalReferenceBuildState::State::Unbuilt,
        });
        return marker;
    }

    for (size_t index = 0; index < node->children.size(); ++index)
    {
        ASTPtr before = node->children[index];
        ASTPtr after = rewriteLogicalReferences(before, definitions_by_name, references);
        if (after.get() == before.get())
            continue;
        const auto * before_ptr = before.get();
        node->replace(before, after);
        node->updatePointerToChild(before_ptr, after);
    }
    return node;
}

CanonicalTypeArgumentValue canonicalValueArgument(ParameterKind kind, ASTPtr & syntax)
{
    auto * literal = syntax ? syntax->as<ASTLiteral>() : nullptr;
    if (!literal || !literal->children.empty())
        fail(BinderError::Code::UnsupportedColumnShape, "a non-TYPE UDT argument must be one literal");

    const Field & value = literal->value;
    if (kind == ParameterKind::Bool)
    {
        if (value.getType() != Field::Types::Bool)
            fail(BinderError::Code::UnsupportedColumnShape, "a Bool UDT argument must be a boolean literal");
        return CanonicalTypeArgumentValue::boolean(value.safeGet<bool>());
    }
    if (isUnsignedIntegerParameter(kind))
    {
        if (value.getType() != Field::Types::UInt64)
            fail(BinderError::Code::UnsupportedColumnShape, "an unsigned UDT argument must be an unsigned integer literal");
        return CanonicalTypeArgumentValue::unsignedInteger(kind, value.safeGet<UInt64>());
    }
    if (isSignedIntegerParameter(kind))
    {
        Int64 signed_value = 0;
        if (value.getType() == Field::Types::Int64)
            signed_value = value.safeGet<Int64>();
        else if (
            value.getType() == Field::Types::UInt64 && value.safeGet<UInt64>() <= static_cast<UInt64>(std::numeric_limits<Int64>::max()))
        {
            signed_value = static_cast<Int64>(value.safeGet<UInt64>());
            syntax = make_intrusive<ASTLiteral>(signed_value);
        }
        else
            fail(BinderError::Code::UnsupportedColumnShape, "a signed UDT argument must be an integer literal in Int64 range");
        return CanonicalTypeArgumentValue::signedInteger(kind, signed_value);
    }
    if (kind == ParameterKind::String)
    {
        if (value.getType() != Field::Types::String)
            fail(BinderError::Code::UnsupportedColumnShape, "a String UDT argument must be a string literal");
        return CanonicalTypeArgumentValue::string(value.safeGet<String>());
    }
    fail(BinderError::Code::UnsupportedColumnShape, "a TYPE UDT argument was routed as a value literal");
}

class LogicalReferenceInputBuilder final
{
public:
    LogicalReferenceInputBuilder(
        std::vector<LogicalReferenceBuildState> & references_,
        const IAuthorityAdapter & authority_,
        TemplateSpecializerLimits specializer_limits_,
        UInt64 maximum_path_depth_,
        ProspectiveResourceBudget & query_budget_,
        TemplateSpecializer::QueryMemo * query_memo_ = nullptr)
        : references(references_)
        , authority(authority_)
        , specializer_limits(std::move(specializer_limits_))
        , maximum_path_depth(maximum_path_depth_)
        , query_budget(query_budget_)
        , query_memo(query_memo_)
    {
        reference_index.reserve(references.size());
        for (size_t index = 0; index < references.size(); ++index)
        {
            if (!references[index].marker || !reference_index.emplace(references[index].marker.get(), index).second)
                fail(BinderError::Code::InvalidState, "the UDT binder produced duplicate logical marker identities");
        }
    }

    std::vector<DeclaredTypeReferenceInput> finish()
    {
        for (size_t index = 0; index < references.size(); ++index)
            static_cast<void>(buildReference(index));

        if (auxiliary_attempt)
        {
            /// The auxiliary attempt exists only to canonicalize TYPE actuals
            /// containing nested logical applications. Its session and ASTs
            /// must be closed before the authoritative whole-column resolver
            /// starts its own single attempt.
            static_cast<void>(auxiliary_attempt->finish());
            auxiliary_attempt.reset();
        }

        std::vector<DeclaredTypeReferenceInput> result;
        result.reserve(references.size());
        for (auto & reference : references)
        {
            if (!reference.input || reference.state != LogicalReferenceBuildState::State::Built)
                fail(BinderError::Code::InvalidState, "the UDT binder left an incomplete logical-reference input");
            result.push_back(std::move(*reference.input));
            reference.input.reset();
        }
        return result;
    }

private:
    const DeclaredTypeReferenceInput & buildReference(size_t index)
    {
        if (index >= references.size())
            fail(BinderError::Code::InvalidState, "the UDT binder reached an absent logical-reference input");
        auto & reference = references[index];
        if (reference.state == LogicalReferenceBuildState::State::Built)
            return *reference.input;
        if (reference.state == LogicalReferenceBuildState::State::Building)
            fail(BinderError::Code::UnsupportedColumnShape, "UDT TYPE arguments contain a logical-reference cycle");
        if (!reference.definition)
            fail(BinderError::Code::InvalidState, "the UDT binder lost a resolved definition handle");

        auto * marker = reference.marker ? reference.marker->as<ASTDataType>() : nullptr;
        if (!marker)
            fail(BinderError::Code::InvalidState, "the UDT binder produced a non-data-type logical marker");
        ASTPtr arguments_ast = marker->getArguments();
        auto * arguments = arguments_ast ? arguments_ast->as<ASTExpressionList>() : nullptr;
        if (arguments_ast && !arguments)
            fail(BinderError::Code::UnsupportedColumnShape, "a UDT application has a malformed argument list");

        const auto & parameters = reference.definition->getParameters();
        const size_t argument_count = arguments ? arguments->children.size() : 0;
        if (argument_count != parameters.size())
            fail(BinderError::Code::UnsupportedColumnShape, "a UDT application argument count does not match its definition");

        reference.state = LogicalReferenceBuildState::State::Building;
        std::vector<CanonicalTypeArgumentValue> canonical_values;
        canonical_values.reserve(parameters.size());
        std::vector<DeclaredTypeArgumentLineageInput> lineage;
        for (size_t parameter = 0; parameter < parameters.size(); ++parameter)
        {
            if (!std::in_range<UInt16>(parameter))
                fail(BinderError::Code::UnsupportedColumnShape, "a UDT parameter ordinal exceeds UInt16");
            ASTPtr & syntax = arguments->children[parameter];
            if (parameters[parameter].kind == ParameterKind::Type)
            {
                RelativePhysicalTypePath path;
                ASTPtr physicalized = physicalizeTypeArgument(syntax, static_cast<UInt16>(parameter), path, lineage);
                canonical_values.push_back(CanonicalTypeArgumentValue::type(physicalized));
            }
            else
            {
                canonical_values.push_back(canonicalValueArgument(parameters[parameter].kind, syntax));
            }
        }

        auto canonical_arguments = CanonicalTypeArguments::validate(
            parameters,
            std::move(canonical_values),
            specializer_limits.maximum_canonical_argument_bytes,
            specializer_limits.maximum_canonical_argument_item_bytes);
        reference.input.emplace(
            DeclaredTypeReferenceInput{
                .reference_node = marker,
                .definition_identity = reference.definition->getIdentity(),
                .canonical_arguments = std::move(canonical_arguments),
                .type_argument_lineage = std::move(lineage),
            });
        reference.state = LogicalReferenceBuildState::State::Built;
        return *reference.input;
    }

    TemplateSpecializer::Attempt & getAuxiliaryAttempt()
    {
        if (!auxiliary_attempt)
            auxiliary_attempt.emplace(TemplateSpecializer::Attempt::begin(authority, specializer_limits, std::addressof(query_budget)));
        return *auxiliary_attempt;
    }

    const ASTPtr & physicalASTForReference(size_t index)
    {
        const auto & input = buildReference(index);
        auto & reference = references[index];
        if (query_memo)
        {
            if (!reference.auxiliary_specialization)
                reference.auxiliary_specialization = query_memo->specialize(authority, input.definition_identity, input.canonical_arguments);
            return query_memo->getCanonicalPhysicalAST(*reference.auxiliary_specialization);
        }
        auto & attempt = getAuxiliaryAttempt();
        if (!reference.auxiliary_specialization)
            reference.auxiliary_specialization = attempt.specialize(input.definition_identity, input.canonical_arguments);
        return attempt.getCanonicalPhysicalAST(*reference.auxiliary_specialization);
    }

    void pushPath(RelativePhysicalTypePath & path, PhysicalTypeChildLocator locator)
    {
        if (path.size() >= maximum_path_depth)
            failLimit("a nested UDT TYPE argument exceeds its depth limit");
        path.push_back(locator);
    }

    ASTPtr physicalizeTypeArgument(
        const ASTPtr & original, UInt16 parameter, RelativePhysicalTypePath & path, std::vector<DeclaredTypeArgumentLineageInput> & lineage)
    {
        if (!original)
            fail(BinderError::Code::UnsupportedColumnShape, "a UDT TYPE argument contains a null node");
        if (const auto found = reference_index.find(original.get()); found != reference_index.end())
        {
            lineage.push_back({
                .parameter = parameter,
                .path = path,
                .reference_node = references[found->second].marker.get(),
            });
            return physicalASTForReference(found->second);
        }
        if (!isExactDataTypeAST(original))
            fail(BinderError::Code::UnsupportedColumnShape, "a UDT TYPE argument is not an exact supported data type");

        ASTPtr clone = original->clone();
        const std::string_view family = canonicalFamily(original);
        const auto * original_arguments = getTypeArguments(original);
        auto * cloned_arguments = getMutableTypeArguments(clone);
        if (static_cast<bool>(original_arguments) != static_cast<bool>(cloned_arguments)
            || (original_arguments && original_arguments->children.size() != cloned_arguments->children.size()))
        {
            fail(BinderError::Code::InvalidState, "cloning changed the shape of a UDT TYPE argument");
        }

        const auto visit_direct = [&](size_t argument_index, PhysicalTypeChildLocator locator)
        {
            if (!original_arguments || !cloned_arguments || argument_index >= original_arguments->children.size()
                || argument_index >= cloned_arguments->children.size())
            {
                fail(BinderError::Code::UnsupportedColumnShape, "a UDT TYPE argument child ordinal is out of range");
            }
            pushPath(path, locator);
            cloned_arguments->children[argument_index]
                = physicalizeTypeArgument(original_arguments->children[argument_index], parameter, path, lineage);
            path.pop_back();
        };

        if (family == "Array" || family == "Nullable" || family == "LowCardinality")
        {
            if (!original_arguments || original_arguments->children.size() != 1)
                fail(BinderError::Code::UnsupportedColumnShape, "a unary UDT TYPE argument family has invalid arity");
            visit_direct(0, {PhysicalTypeChildLocatorKind::StableOrdinal, 0});
            return clone;
        }
        if (family == "Map")
        {
            if (!original_arguments || original_arguments->children.size() != 2)
                fail(BinderError::Code::UnsupportedColumnShape, "a Map UDT TYPE argument has invalid arity");
            for (UInt32 ordinal = 0; ordinal < 2; ++ordinal)
                visit_direct(ordinal, {PhysicalTypeChildLocatorKind::StableOrdinal, ordinal});
            return clone;
        }
        if (family == "QBit")
        {
            if (!original_arguments || (original_arguments->children.size() != 2 && original_arguments->children.size() != 3))
                fail(BinderError::Code::UnsupportedColumnShape, "a QBit UDT TYPE argument has invalid arity");
            visit_direct(0, {PhysicalTypeChildLocatorKind::StableOrdinal, 0});
            return clone;
        }
        if (family == "Variant")
        {
            if (!original_arguments || original_arguments->children.empty())
                fail(BinderError::Code::UnsupportedColumnShape, "a Variant UDT TYPE argument has no branches");
            for (size_t index = 0; index < original_arguments->children.size(); ++index)
            {
                if (!std::in_range<UInt32>(index))
                    fail(BinderError::Code::UnsupportedColumnShape, "a Variant UDT TYPE argument branch ordinal exceeds UInt32");
                visit_direct(index, {PhysicalTypeChildLocatorKind::VariantNormalizedBranch, static_cast<UInt32>(index)});
            }
            return clone;
        }
        if (family == "Tuple" || family == "Nested")
        {
            if (!original_arguments)
            {
                if (family == "Nested")
                    fail(BinderError::Code::UnsupportedColumnShape, "a Nested UDT TYPE argument has no fields");
                return clone;
            }
            for (size_t index = 0; index < original_arguments->children.size(); ++index)
            {
                if (!std::in_range<UInt32>(index))
                    fail(BinderError::Code::UnsupportedColumnShape, "a Tuple/Nested UDT TYPE argument ordinal exceeds UInt32");
                const PhysicalTypeChildLocator locator{PhysicalTypeChildLocatorKind::StableOrdinal, static_cast<UInt32>(index)};
                const auto * original_pair = original_arguments->children[index]->as<ASTNameTypePair>();
                auto * cloned_pair = cloned_arguments->children[index]->as<ASTNameTypePair>();
                if (original_pair || cloned_pair)
                {
                    if (!original_pair || !cloned_pair || !original_pair->type || !cloned_pair->type || original_pair->children.size() != 1
                        || cloned_pair->children.size() != 1)
                    {
                        fail(BinderError::Code::UnsupportedColumnShape, "a Tuple/Nested UDT TYPE argument field is malformed");
                    }
                    pushPath(path, locator);
                    ASTPtr child = physicalizeTypeArgument(original_pair->type, parameter, path, lineage);
                    path.pop_back();
                    cloned_pair->type = child;
                    cloned_pair->children[0] = std::move(child);
                }
                else
                {
                    if (family == "Nested")
                        fail(BinderError::Code::UnsupportedColumnShape, "a Nested UDT TYPE argument has an unnamed field");
                    visit_direct(index, locator);
                }
            }
            return clone;
        }
        if (family == "AggregateFunction" || family == "SimpleAggregateFunction")
        {
            if (!original_arguments || original_arguments->children.empty())
                fail(BinderError::Code::UnsupportedColumnShape, "an aggregate-function UDT TYPE argument has no function name");
            size_t first_type = 1;
            if (family == "AggregateFunction" && original_arguments->children.front()->as<ASTLiteral>())
                first_type = 2;
            if (first_type > original_arguments->children.size())
                fail(BinderError::Code::UnsupportedColumnShape, "aggregate-function metadata exceeds its UDT TYPE argument list");
            for (size_t index = first_type; index < original_arguments->children.size(); ++index)
            {
                const size_t ordinal = index - first_type;
                if (!std::in_range<UInt32>(ordinal))
                    fail(BinderError::Code::UnsupportedColumnShape, "an aggregate-function UDT TYPE child ordinal exceeds UInt32");
                visit_direct(index, {PhysicalTypeChildLocatorKind::StableOrdinal, static_cast<UInt32>(ordinal)});
            }
            return clone;
        }
        if (family == "JSON")
        {
            if (!original_arguments)
                return clone;
            struct TypedPath
            {
                std::string_view name;
                size_t argument_index = 0;
            };
            std::vector<TypedPath> typed_paths;
            typed_paths.reserve(original_arguments->children.size());
            for (size_t index = 0; index < original_arguments->children.size(); ++index)
            {
                const auto * object_argument = original_arguments->children[index]->as<ASTObjectTypeArgument>();
                if (!object_argument || !object_argument->path_with_type)
                    continue;
                const auto * typed = object_argument->path_with_type->as<ASTObjectTypedPathArgument>();
                if (!typed || !typed->type)
                    fail(BinderError::Code::UnsupportedColumnShape, "a JSON UDT TYPE argument typed path is malformed");
                typed_paths.push_back({.name = typed->path, .argument_index = index});
            }
            std::sort(
                typed_paths.begin(), typed_paths.end(), [](const auto & lhs, const auto & rhs) { return binaryLess(lhs.name, rhs.name); });
            for (size_t ordinal = 0; ordinal < typed_paths.size(); ++ordinal)
            {
                if (ordinal && typed_paths[ordinal - 1].name == typed_paths[ordinal].name)
                    fail(BinderError::Code::UnsupportedColumnShape, "a JSON UDT TYPE argument has duplicate typed paths");
                if (!std::in_range<UInt32>(ordinal))
                    fail(BinderError::Code::UnsupportedColumnShape, "a JSON UDT TYPE argument typed-path ordinal exceeds UInt32");
                const size_t argument_index = typed_paths[ordinal].argument_index;
                const auto * original_object = original_arguments->children[argument_index]->as<ASTObjectTypeArgument>();
                auto * cloned_object = cloned_arguments->children[argument_index]->as<ASTObjectTypeArgument>();
                const auto * original_typed = original_object->path_with_type->as<ASTObjectTypedPathArgument>();
                auto * cloned_typed = cloned_object && cloned_object->path_with_type
                    ? cloned_object->path_with_type->as<ASTObjectTypedPathArgument>()
                    : nullptr;
                if (!cloned_object || !cloned_typed || !cloned_typed->type || cloned_typed->children.size() != 1)
                    fail(BinderError::Code::UnsupportedColumnShape, "a cloned JSON UDT TYPE argument typed path is malformed");
                pushPath(path, {PhysicalTypeChildLocatorKind::StableOrdinal, static_cast<UInt32>(ordinal)});
                ASTPtr child = physicalizeTypeArgument(original_typed->type, parameter, path, lineage);
                path.pop_back();
                cloned_typed->type = child;
                cloned_typed->children[0] = std::move(child);
            }
            return clone;
        }

        /// Every other registered family is opaque in the stable physical
        /// topology. A marker hidden in one of its value arguments remains
        /// unmatched and TypeResolver rejects it instead of guessing a path.
        return clone;
    }

    std::vector<LogicalReferenceBuildState> & references;
    const IAuthorityAdapter & authority;
    TemplateSpecializerLimits specializer_limits;
    UInt64 maximum_path_depth;
    ProspectiveResourceBudget & query_budget;
    TemplateSpecializer::QueryMemo * query_memo = nullptr;
    std::unordered_map<const IAST *, size_t> reference_index;
    std::optional<TemplateSpecializer::Attempt> auxiliary_attempt;
};

}

struct PreparedScalarAliasColumns::Impl
{
    struct Replacement
    {
        ASTColumnDeclaration * declaration = nullptr;
        ASTPtr original_type;
        ASTPtr physical_type;
    };

    NamesAndTypesList expected_physical_columns;
    std::vector<TableColumnTypeBindingInput> bindings;
    std::vector<Replacement> replacements;
    TableColumnTypeBindingLimits table_binding_limits;
    bool replacements_applied = false;
    bool finished = false;
};

ScalarAliasColumnBinderError::ScalarAliasColumnBinderError(Code code_, std::string_view message)
    : std::runtime_error(String("User-defined type scalar-alias column binding failed: ") + String(message))
    , code(code_)
{
}

PreparedScalarAliasColumns::PreparedScalarAliasColumns(std::unique_ptr<Impl> impl_)
    : impl(std::move(impl_))
{
}

PreparedScalarAliasColumns::PreparedScalarAliasColumns(PreparedScalarAliasColumns &&) noexcept = default;

PreparedScalarAliasColumns & PreparedScalarAliasColumns::operator=(PreparedScalarAliasColumns &&) noexcept = default;

PreparedScalarAliasColumns::~PreparedScalarAliasColumns() = default;

const NamesAndTypesList & PreparedScalarAliasColumns::getExpectedPhysicalColumns() const noexcept
{
    return impl->expected_physical_columns;
}

void PreparedScalarAliasColumns::applyPhysicalTypeASTs()
{
    if (!impl || impl->finished || impl->replacements_applied)
        fail(BinderError::Code::InvalidState, "physical column AST replacements may be applied exactly once");

    for (const auto & replacement : impl->replacements)
    {
        if (!replacement.declaration || replacement.declaration->getType().get() != replacement.original_type.get()
            || !replacement.physical_type)
        {
            fail(BinderError::Code::QueryChanged, "the table column AST changed after UDT binding preparation");
        }
    }

    for (auto & replacement : impl->replacements)
        replacement.declaration->setType(std::move(replacement.physical_type));
    impl->replacements_applied = true;
}

PreparedTableColumnTypeBindings PreparedScalarAliasColumns::finish(
    const SchemaObjectID & table, UInt64 object_schema_revision, const NamesAndTypesList & normalized_physical_columns) &&
{
    if (!impl || impl->finished || !impl->replacements_applied)
        fail(BinderError::Code::InvalidState, "the UDT column binder was not applied or was already finished");

    impl->finished = true;
    auto bindings = prepareTableColumnTypeBindings(table, object_schema_revision, impl->bindings, impl->table_binding_limits);
    if (normalized_physical_columns == impl->expected_physical_columns)
        return bindings;

    /// Ordinary flatten_nested normalization is represented by the same
    /// one-column Replace fragments used by ALTER. The shared composer rebases
    /// each root Nested child onto its permanent flattened column/path and
    /// rejects every other unmodelled schema drift.
    std::vector<TableColumnTypeAlterOperation> normalization_operations;
    normalization_operations.reserve(impl->bindings.size());
    for (const auto & column : impl->bindings)
    {
        auto fragment = prepareTableColumnTypeBindings(
            table,
            object_schema_revision,
            std::span<const TableColumnTypeBindingInput>(std::addressof(column), 1),
            impl->table_binding_limits);
        normalization_operations.push_back({
            .kind = TableColumnTypeAlterOperationKind::Replace,
            .column_name = column.column_name,
            .target_name = {},
            .replacement_physical_type = column.declared_type.getPhysicalType(),
            .replacement_column_references = std::move(fragment.persisted_references),
            .physical_columns_after_operation = {},
        });
    }
    try
    {
        return rebaseInitialTableColumnTypeBindingsAfterNormalization(
            std::move(bindings), normalized_physical_columns, normalization_operations, impl->table_binding_limits);
    }
    catch (const TableColumnTypeBindingError & error)
    {
        fail(
            BinderError::Code::NormalizedSchemaMismatch,
            String("ordinary schema normalization cannot preserve the pre-resolved logical column schema: ") + error.what());
    }
}

std::vector<std::optional<PersistedTypeReferences>>
PreparedScalarAliasColumns::finishIndividualColumns(const SchemaObjectID & table, UInt64 object_schema_revision) &&
{
    if (!impl || impl->finished || !impl->replacements_applied)
        fail(BinderError::Code::InvalidState, "the aggregate UDT ALTER binder was not applied or was already finished");
    if (impl->bindings.size() != impl->expected_physical_columns.size())
        fail(BinderError::Code::InvalidState, "the aggregate UDT ALTER binder has inconsistent column outputs");

    std::vector<std::optional<PersistedTypeReferences>> result;
    result.reserve(impl->bindings.size());
    auto expected_physical_column = impl->expected_physical_columns.begin();
    for (size_t index = 0; index < impl->bindings.size(); ++index)
    {
        const std::span<const TableColumnTypeBindingInput> one_column(&impl->bindings[index], 1);
        auto prepared = prepareTableColumnTypeBindings(table, object_schema_revision, one_column, impl->table_binding_limits);
        if (prepared.physical_columns.size() != 1 || prepared.physical_columns.front() != *expected_physical_column)
        {
            fail(BinderError::Code::NormalizedSchemaMismatch, "an aggregate UDT ALTER column changed after resolution");
        }
        result.push_back(std::move(prepared.persisted_references));
        ++expected_physical_column;
    }
    impl->finished = true;
    return result;
}

bool hasReferencesInCreateTableColumns(const ASTCreateQuery & create)
{
    if (!create.columns_list || !create.columns_list->columns)
        return false;
    for (const auto & child : create.columns_list->columns->children)
    {
        const auto * declaration = child ? child->as<ASTColumnDeclaration>() : nullptr;
        if (declaration && containsUDTReference(declaration->getType()))
            return true;
    }
    return false;
}

bool hasReferencesInAlterColumn(const ASTColumnDeclaration & declaration)
{
    return containsUDTReference(declaration.getType());
}

namespace
{

std::vector<BoundDeclaredTypeResult> resolveSourceColumnTypes(
    const std::vector<SourceColumn> & source_columns,
    std::string_view database_name,
    const ContextPtr & context,
    const IAuthorityAdapter & authority,
    const TypeResolverLimits & resolver_limits,
    ProspectiveResourceBudget & query_budget,
    UDTTypeExpressionResolutionStatistics * statistics)
{
    if (source_columns.empty() || database_name.empty())
        fail(BinderError::Code::InvalidInput, "an activated UDT type-resolution batch is empty");
    if (!context)
        fail(BinderError::Code::InvalidInput, "an activated UDT type-resolution batch has no query context");

    const UUID authority_database_uuid = authority.getDatabaseUUID();
    if (authority_database_uuid == UUIDHelpers::Nil)
        fail(BinderError::Code::AuthorityMismatch, "the UDT authority has a nil database identity");

    /// Pin before the first name lookup and route every subsequent resolver
    /// and specializer attempt through the same immutable authority generation.
    PinnedResolutionAuthorityAdapter pinned_authority(authority);
    if (statistics)
        ++statistics->catalog_root_loads;
    if (pinned_authority.getDatabaseUUID() != authority_database_uuid)
        fail(BinderError::Code::AuthorityMismatch, "the UDT authority changed database identity while pinning its DDL snapshot");

    std::map<String, Definition::Ptr, std::less<>> definitions_by_name;
    std::map<UUID, Definition::Ptr> definitions_by_uuid;
    {
        auto session = pinned_authority.beginResolutionSession();
        for (auto & column : source_columns)
        {
            for (const auto * reference : column.references)
            {
                auto name_it = definitions_by_name.find(reference->type_name);
                if (name_it == definitions_by_name.end())
                {
                    ResourceDelta delta;
                    delta.add(ResourceLimit::ExplicitNamesPerQuery, 1);
                    delta.add(ResourceLimit::SemanticScratchBytesPerQuery, resourceSize(reference->type_name.size()));
                    requireAdmission(query_budget.charge(delta));
                    if (statistics)
                        ++statistics->catalog_name_lookups;
                    auto definition = session.findByName(reference->type_name);
                    name_it = definitions_by_name.try_emplace(reference->type_name, std::move(definition)).first;
                }
                const auto & definition = name_it->second;
                if (!definition)
                    fail(BinderError::Code::UnknownDefinition, "a type expression names no current user-defined type");
                if (definition->getIdentity().database_uuid != authority_database_uuid)
                    fail(BinderError::Code::AuthorityMismatch, "a resolved UDT belongs to another authority identity");

                const auto [uuid_it, uuid_inserted] = definitions_by_uuid.try_emplace(definition->getIdentity().type_uuid, definition);
                if (!uuid_inserted
                    && (uuid_it->second->getIdentity() != definition->getIdentity()
                        || !uuid_it->second->hasSameCheckedSemantics(*definition)))
                {
                    fail(BinderError::Code::AuthorityMismatch, "one stable UDT identity resolved to conflicting checked definitions");
                }
            }
        }
    }

    std::vector<AccessTarget> access_targets;
    access_targets.reserve(definitions_by_uuid.size());
    for (const auto & [type_uuid, definition] : definitions_by_uuid)
    {
        static_cast<void>(type_uuid);
        access_targets.push_back({
            .database_uuid = definition->getIdentity().database_uuid,
            .type_uuid = definition->getIdentity().type_uuid,
        });
    }
    checkUsageAccess(context, access_targets);

    constexpr TypeAuthorityCapabilityMask required_capabilities = typeAuthorityCapabilityBit(TypeAuthorityCapability::TransientResolution)
        | typeAuthorityCapabilityBit(TypeAuthorityCapability::Limits) | typeAuthorityCapabilityBit(TypeAuthorityCapability::Templates);
    authority.requireCapabilities(required_capabilities, "UDT type expression resolution");

    struct ResolverColumn
    {
        ASTPtr declared_type;
        size_t first_reference = 0;
        size_t reference_count = 0;
    };

    std::vector<LogicalReferenceBuildState> logical_references;
    size_t logical_reference_count = 0;
    for (const auto & column : source_columns)
    {
        if (logical_reference_count > resolver_limits.maximum_input_references)
            fail(BinderError::Code::UnsupportedColumnShape, "UDT occurrences exceed the resolver input limit");
        if (column.references.size() > resolver_limits.maximum_input_references - logical_reference_count)
            fail(BinderError::Code::UnsupportedColumnShape, "UDT occurrences exceed the resolver input limit");
        logical_reference_count += column.references.size();
    }
    chargeLogicalReferenceScratch(query_budget, resourceSize(logical_reference_count), resourceSize(source_columns.size()));
    logical_references.reserve(logical_reference_count);
    std::vector<ResolverColumn> resolver_columns;
    resolver_columns.reserve(source_columns.size());
    for (const auto & column : source_columns)
    {
        const size_t first_reference = logical_references.size();
        ASTPtr declared_type = column.declared_type->clone();
        if (!column.references.empty())
            declared_type = rewriteLogicalReferences(std::move(declared_type), definitions_by_name, logical_references);
        const size_t reference_count = logical_references.size() - first_reference;
        if (reference_count != column.references.size())
            fail(BinderError::Code::InvalidState, "the UDT binder changed the logical-reference occurrence count");
        resolver_columns.push_back({
            .declared_type = std::move(declared_type),
            .first_reference = first_reference,
            .reference_count = reference_count,
        });
    }

    LogicalReferenceInputBuilder input_builder(
        logical_references,
        pinned_authority,
        resolver_limits.specializer,
        resolver_limits.maximum_argument_validation_ast_depth,
        query_budget);
    auto reference_inputs = input_builder.finish();
    if (reference_inputs.size() != logical_reference_count)
        fail(BinderError::Code::InvalidState, "the UDT binder produced an incomplete logical-reference side table");

    std::vector<BoundDeclaredTypeResult> resolved_types;
    resolved_types.reserve(source_columns.size());
    for (size_t column_index = 0; column_index < source_columns.size(); ++column_index)
    {
        const auto & resolver_column = resolver_columns[column_index];
        const std::span<const DeclaredTypeReferenceInput> column_references(reference_inputs);
        auto resolved = TypeResolver::resolve(
            resolver_column.declared_type,
            column_references.subspan(resolver_column.first_reference, resolver_column.reference_count),
            pinned_authority,
            resolver_limits,
            nullptr,
            &query_budget);
        if (resolver_column.reference_count && !resolved.hasLogicalTree())
            fail(BinderError::Code::InvalidState, "an activated UDT type resolution produced no logical binding");

        if (!resolved.getPhysicalType())
            fail(BinderError::Code::InvalidState, "a resolved UDT type expression has no physical type");
        resolved_types.push_back(std::move(resolved));
    }

    return resolved_types;
}

}

struct UDTTypeExpressionResolutionScope::Impl
{
    Impl(
        String database_name_,
        ContextPtr context_,
        const IAuthorityAdapter & authority_,
        std::shared_ptr<QueryResourceLedger> query_resource_ledger)
        : database_name(std::move(database_name_))
        , context(std::move(context_))
        , authority(authority_)
        , resource_ledger(std::move(query_resource_ledger))
    {
        if (database_name.empty() || !context)
            fail(BinderError::Code::InvalidInput, "a UDT type-resolution scope has no database or query context");

        constexpr TypeAuthorityCapabilityMask required_capabilities
            = typeAuthorityCapabilityBit(TypeAuthorityCapability::TransientResolution)
            | typeAuthorityCapabilityBit(TypeAuthorityCapability::Limits) | typeAuthorityCapabilityBit(TypeAuthorityCapability::Templates);
        authority.requireCapabilities(required_capabilities, "UDT type expression resolution");

        if (!resource_ledger)
            resource_ledger = std::make_shared<QueryResourceLedger>();

        authority_database_uuid = authority.getDatabaseUUID();
        if (authority_database_uuid == UUIDHelpers::Nil)
            fail(BinderError::Code::AuthorityMismatch, "the UDT authority has a nil database identity");

    }

    TemplateSpecializer::QueryMemo & getOrCreateSpecializationMemo(const IAuthorityAdapter & resolution_authority)
    {
        if (!specialization_memo)
        {
            specialization_memo
                = std::make_unique<TemplateSpecializer::QueryMemo>(resolution_authority, resolver_limits.specializer, *resource_budget);
        }
        return *specialization_memo;
    }

    BoundDeclaredTypeResult resolve(const ASTPtr & declared_type)
    {
        if (!declared_type)
            fail(BinderError::Code::InvalidInput, "an explicit UDT type expression is empty");

        /// Pin before deriving limits: the persisted database tuple and all
        /// catalog lookups must belong to one generation. No wider default
        /// tuple is allowed to authorize work against a subsequently pinned
        /// root.
        PinnedResolutionAuthorityAdapter pinned_authority(authority);
        if (pinned_authority.getDatabaseUUID() != authority_database_uuid)
            fail(BinderError::Code::AuthorityMismatch, "the UDT authority changed database identity while pinning a resolution root");
        const UInt64 generation = pinned_authority.getPinnedGeneration();
        if (authority_generation && *authority_generation != generation)
            fail(BinderError::Code::AuthorityMismatch, "the UDT authority generation changed during query analysis");
        authority_generation = generation;
        ++statistics.catalog_root_loads;

        const auto exact_query_limits = pinned_authority.getPinnedEffectiveResourceLimits()
            ? makeQueryEffectiveResourceLimits(*pinned_authority.getPinnedEffectiveResourceLimits(), authority.getCapabilities().limits)
            : makeDefaultQueryEffectiveResourceLimits(authority.getCapabilities().limits);
        if (!resource_budget)
            resource_budget.emplace(exact_query_limits, resource_ledger);
        requireAdmission(resource_budget->admitCurrentUsage(exact_query_limits));

        /// A sibling authority may have tightened the shared query minimum
        /// since the previous expression. Re-derive every component-local
        /// gate; a memo built under a wider specialization tuple is discarded
        /// before it can serve a hit (monotonic charges are not refunded).
        auto refreshed_resolver_limits = makeTypeResolverLimits(resource_budget->getLimits());
        if (specialization_memo && specialization_memo->getLimits() != refreshed_resolver_limits.specializer)
            specialization_memo.reset();
        resolver_limits = std::move(refreshed_resolver_limits);

        std::vector<const ASTUDTReference *> references;
        collectUDTReferences(declared_type, database_name, resolver_limits, *resource_budget, references);
        if (references.empty())
            fail(BinderError::Code::InvalidInput, "an explicit UDT type expression contains no UDT reference");

        std::map<UUID, Definition::Ptr> definitions_requiring_access;
        {
            auto session = pinned_authority.beginResolutionSession();
            for (const auto * reference : references)
            {
                auto name_it = definitions_by_name.find(reference->type_name);
                if (name_it == definitions_by_name.end())
                {
                    ResourceDelta delta;
                    delta.add(ResourceLimit::ExplicitNamesPerQuery, 1);
                    delta.add(ResourceLimit::SemanticScratchBytesPerQuery, resourceSize(reference->type_name.size()));
                    requireAdmission(resource_budget->charge(delta));
                    ++statistics.catalog_name_lookups;
                    auto definition = session.findByName(reference->type_name);
                    name_it = definitions_by_name.try_emplace(reference->type_name, std::move(definition)).first;
                }

                const auto & definition = name_it->second;
                if (!definition)
                    fail(BinderError::Code::UnknownDefinition, "a type expression names no current user-defined type");
                if (definition->getIdentity().database_uuid != authority_database_uuid)
                    fail(BinderError::Code::AuthorityMismatch, "a resolved UDT belongs to another authority identity");

                const auto [uuid_it, uuid_inserted] = definitions_by_uuid.try_emplace(definition->getIdentity().type_uuid, definition);
                if (!uuid_inserted
                    && (uuid_it->second->getIdentity() != definition->getIdentity()
                        || !uuid_it->second->hasSameCheckedSemantics(*definition)))
                {
                    fail(BinderError::Code::AuthorityMismatch, "one stable UDT identity resolved to conflicting checked definitions");
                }

                if (!authorized_type_uuids.contains(definition->getIdentity().type_uuid))
                    definitions_requiring_access.try_emplace(definition->getIdentity().type_uuid, definition);
            }
        }

        if (!definitions_requiring_access.empty())
        {
            std::vector<AccessTarget> access_targets;
            access_targets.reserve(definitions_requiring_access.size());
            for (const auto & [type_uuid, definition] : definitions_requiring_access)
            {
                access_targets.push_back({
                    .database_uuid = definition->getIdentity().database_uuid,
                    .type_uuid = type_uuid,
                });
            }
            checkUsageAccess(context, access_targets);
            for (const auto & [type_uuid, definition] : definitions_requiring_access)
            {
                static_cast<void>(definition);
                authorized_type_uuids.insert(type_uuid);
            }
        }

        /// Name resolution and USAGE checks deliberately precede the first
        /// memo access. A hit can skip expansion, never authorization.
        auto & query_memo = getOrCreateSpecializationMemo(pinned_authority);

        std::vector<LogicalReferenceBuildState> logical_references;
        chargeLogicalReferenceScratch(*resource_budget, resourceSize(references.size()), 1);
        logical_references.reserve(references.size());
        ASTPtr rewritten_type = rewriteLogicalReferences(declared_type->clone(), definitions_by_name, logical_references);
        if (logical_references.size() != references.size())
            fail(BinderError::Code::InvalidState, "the UDT binder changed the logical-reference occurrence count");

        LogicalReferenceInputBuilder input_builder(
            logical_references,
            pinned_authority,
            resolver_limits.specializer,
            resolver_limits.maximum_argument_validation_ast_depth,
            *resource_budget,
            std::addressof(query_memo));
        auto reference_inputs = input_builder.finish();
        if (reference_inputs.size() != references.size())
            fail(BinderError::Code::InvalidState, "the UDT binder produced an incomplete logical-reference side table");

        const std::span<const DeclaredTypeReferenceInput> inputs(reference_inputs);
        auto resolved = TypeResolver::resolve(
            rewritten_type,
            inputs,
            pinned_authority,
            resolver_limits,
            nullptr,
            std::addressof(*resource_budget),
            std::addressof(query_memo));
        if (!resolved.hasLogicalTree() || !resolved.getPhysicalType())
            fail(BinderError::Code::InvalidState, "an activated UDT type resolution produced no logical or physical result");
        return resolved;
    }

    String database_name;
    ContextPtr context;
    const IAuthorityAdapter & authority;
    UUID authority_database_uuid = UUIDHelpers::Nil;
    std::optional<UInt64> authority_generation;
    std::map<String, Definition::Ptr, std::less<>> definitions_by_name;
    std::map<UUID, Definition::Ptr> definitions_by_uuid;
    std::set<UUID> authorized_type_uuids;
    UDTTypeExpressionResolutionStatistics statistics;
    TypeResolverLimits resolver_limits;
    std::shared_ptr<QueryResourceLedger> resource_ledger;
    std::optional<ProspectiveResourceBudget> resource_budget;
    /// Destroy before its borrowed budget. It owns no authority session.
    std::unique_ptr<TemplateSpecializer::QueryMemo> specialization_memo;
};

UDTTypeExpressionResolutionScope::UDTTypeExpressionResolutionScope(
    String database_name, ContextPtr context, const IAuthorityAdapter & authority)
    : UDTTypeExpressionResolutionScope(std::move(database_name), std::move(context), authority, std::make_shared<QueryResourceLedger>())
{
}

UDTTypeExpressionResolutionScope::UDTTypeExpressionResolutionScope(
    String database_name,
    ContextPtr context,
    const IAuthorityAdapter & authority,
    std::shared_ptr<QueryResourceLedger> query_resource_ledger)
    : impl(std::make_unique<Impl>(std::move(database_name), std::move(context), authority, std::move(query_resource_ledger)))
{
}

UDTTypeExpressionResolutionScope::~UDTTypeExpressionResolutionScope() = default;
UDTTypeExpressionResolutionScope::UDTTypeExpressionResolutionScope(UDTTypeExpressionResolutionScope &&) noexcept = default;
UDTTypeExpressionResolutionScope & UDTTypeExpressionResolutionScope::operator=(UDTTypeExpressionResolutionScope &&) noexcept = default;

BoundDeclaredTypeResult UDTTypeExpressionResolutionScope::resolve(const ASTPtr & declared_type)
{
    if (!impl)
        fail(BinderError::Code::InvalidState, "a moved-from UDT type-resolution scope cannot resolve a type");
    return translateResolutionLimitErrors([&] { return impl->resolve(declared_type); });
}

const UDTTypeExpressionResolutionStatistics & UDTTypeExpressionResolutionScope::getStatistics() const noexcept
{
    static const UDTTypeExpressionResolutionStatistics empty;
    return impl ? impl->statistics : empty;
}

std::optional<EffectiveResourceLimits> UDTTypeExpressionResolutionScope::getEffectiveQueryResourceLimits() const noexcept
{
    if (!impl || !impl->authority_generation || !impl->resource_budget)
        return std::nullopt;
    return impl->resource_budget->getLimits();
}

BoundDeclaredTypeResult resolveUDTTypeExpression(
    const ASTPtr & declared_type,
    std::string_view database_name,
    const ContextPtr & context,
    const IAuthorityAdapter & authority,
    UDTTypeExpressionResolutionStatistics * statistics)
{
    UDTTypeExpressionResolutionScope scope(String(database_name), context, authority);
    try
    {
        auto resolved = scope.resolve(declared_type);
        if (statistics)
            *statistics = scope.getStatistics();
        return resolved;
    }
    catch (...)
    {
        if (statistics)
            *statistics = scope.getStatistics();
        throw;
    }
}

PreparedScalarAliasColumns PreparedScalarAliasColumns::prepareActivated(
    ASTCreateQuery * create_source,
    std::span<ASTColumnDeclaration * const> declarations,
    std::string_view database_name,
    const ContextPtr & context,
    const IAuthorityAdapter & authority)
{
    if (database_name.empty())
        fail(BinderError::Code::InvalidInput, "an activated UDT type-resolution batch has no database");
    if ((create_source == nullptr) == declarations.empty())
        fail(BinderError::Code::InvalidInput, "an activated UDT type-resolution batch must have exactly one column source");
    if (!context)
        fail(BinderError::Code::InvalidInput, "an activated UDT type-resolution batch has no query context");

    /// Reject foreign authority names under implementation-hard AST bounds
    /// before pinning or querying any authority state. Authorization then runs
    /// over the one pinned lookup batch before capability-gated specialization.
    preflightSyntacticReferenceDatabases(create_source, declarations, database_name);
    PinnedResolutionAuthorityAdapter pinned_authority(authority);
    const auto effective_limits = pinned_authority.getPinnedEffectiveResourceLimits()
        ? makeQueryEffectiveResourceLimits(*pinned_authority.getPinnedEffectiveResourceLimits(), authority.getCapabilities().limits)
        : makeDefaultQueryEffectiveResourceLimits(authority.getCapabilities().limits);
    const TypeResolverLimits resolver_limits = makeTypeResolverLimits(effective_limits);
    ProspectiveResourceBudget query_budget(effective_limits);
    requireAdmission(query_budget.admitCurrentUsage());

    std::vector<ASTColumnDeclaration *> create_declarations;
    if (create_source)
    {
        if (!create_source->columns_list || !create_source->columns_list->columns || create_source->columns_list->columns->children.empty())
        {
            fail(BinderError::Code::InvalidInput, "an activated CREATE TABLE has no explicit columns");
        }
        const auto & children = create_source->columns_list->columns->children;
        requireAdmission(query_budget.charge(
            ResourceLimit::SemanticScratchBytesPerQuery,
            checkedResourceProduct(resourceSize(children.size()), static_cast<UInt64>(sizeof(ASTColumnDeclaration *)))));
        create_declarations.reserve(children.size());
        for (const auto & child : children)
        {
            auto * declaration = child ? child->as<ASTColumnDeclaration>() : nullptr;
            if (!declaration)
            {
                fail(BinderError::Code::UnsupportedColumnShape, "an activated CREATE TABLE column declaration is malformed");
            }
            create_declarations.push_back(declaration);
        }
        declarations = std::span<ASTColumnDeclaration * const>(create_declarations.data(), create_declarations.size());
    }

    auto source_columns = collectAndValidateSourceColumns(declarations, database_name, resolver_limits, query_budget);
    auto resolved_types = translateResolutionLimitErrors(
        [&]
        {
            return resolveSourceColumnTypes(
                source_columns, database_name, context, pinned_authority, resolver_limits, query_budget, nullptr);
        });
    if (resolved_types.size() != source_columns.size())
        fail(BinderError::Code::InvalidState, "the UDT binder produced an invalid result count");

    auto result = std::make_unique<PreparedScalarAliasColumns::Impl>();
    result->table_binding_limits = makeTableColumnTypeBindingLimits(effective_limits);
    result->bindings.reserve(source_columns.size());
    result->replacements.reserve(source_columns.size());
    for (size_t column_index = 0; column_index < source_columns.size(); ++column_index)
    {
        auto & column = source_columns[column_index];
        auto resolved = std::move(resolved_types[column_index]);
        DataTypePtr physical_type = resolved.getPhysicalType();
        result->expected_physical_columns.emplace_back(column.declaration->name, physical_type);
        result->bindings.push_back({.column_name = column.declaration->name, .declared_type = std::move(resolved)});
        if (!column.references.empty())
        {
            result->replacements.push_back({
                .declaration = column.declaration,
                .original_type = std::move(column.declared_type),
                .physical_type = dataTypeToAST(physical_type),
            });
        }
    }

    return PreparedScalarAliasColumns(std::move(result));
}

std::optional<PreparedScalarAliasColumns> prepareScalarAliasColumns(
    ASTCreateQuery & create, std::string_view database_name, const ContextPtr & context, const IAuthorityAdapter & authority)
{
    if (!hasReferencesInCreateTableColumns(create))
        return std::nullopt;

    return PreparedScalarAliasColumns::prepareActivated(std::addressof(create), {}, database_name, context, authority);
}

std::optional<PreparedScalarAliasColumns> prepareScalarAliasAlterColumn(
    ASTColumnDeclaration & declaration, std::string_view database_name, const ContextPtr & context, const IAuthorityAdapter & authority)
{
    if (!containsUDTReference(declaration.getType()))
        return std::nullopt;

    std::array<ASTColumnDeclaration *, 1> declarations{&declaration};
    return PreparedScalarAliasColumns::prepareActivated(nullptr, declarations, database_name, context, authority);
}

std::optional<PreparedScalarAliasColumns> prepareScalarAliasAlterColumns(
    std::span<ASTColumnDeclaration * const> declarations,
    std::string_view database_name,
    const ContextPtr & context,
    const IAuthorityAdapter & authority)
{
    if (std::none_of(
            declarations.begin(),
            declarations.end(),
            [](const auto * declaration) { return declaration && containsUDTReference(declaration->getType()); }))
    {
        return std::nullopt;
    }

    return PreparedScalarAliasColumns::prepareActivated(nullptr, declarations, database_name, context, authority);
}

}
