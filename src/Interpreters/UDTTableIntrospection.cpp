#include <Interpreters/UDTTableIntrospection.h>

#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/UDT/BoundObjectTypeReferences.h>
#include <DataTypes/UDT/CanonicalTypeArguments.h>
#include <DataTypes/UDT/IAuthorityAdapter.h>
#include <DataTypes/UDT/SchemaObjectIdentity.h>
#include <DataTypes/UDT/TemplateSpecializer.h>
#include <DataTypes/dataTypeToAST.h>

#include <Databases/IDatabase.h>
#include <Databases/UDT/ILifecycleAdapter.h>

#include <Interpreters/StorageID.h>
#include <Interpreters/UDT/DictionaryAttributeTypeBindings.h>
#include <Interpreters/UDT/StoredObjectTypeBindingPreparation.h>
#include <Interpreters/UDT/ViewOutputTypeBindings.h>

#include <Parsers/ASTColumnDeclaration.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/ASTDataType.h>
#include <Parsers/ASTDictionaryAttributeDeclaration.h>
#include <Parsers/ASTEnumDataType.h>
#include <Parsers/ASTExpressionList.h>
#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTNameTypePair.h>
#include <Parsers/ASTObjectTypeArgument.h>
#include <Parsers/ASTTupleDataType.h>
#include <Parsers/ASTUDTReference.h>

#include <Storages/StorageInMemoryMetadata.h>

#include <Common/Exception.h>
#include <Common/typeid_cast.h>
#include <Core/Field.h>

#include <algorithm>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <set>
#include <span>
#include <string_view>
#include <type_traits>
#include <utility>

namespace DB::ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB::UDT
{
namespace
{

[[noreturn]] void introspectionError(std::string_view message)
{
    throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot render bound table UDTs: {}", message);
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

ASTExpressionList * getMutableTypeArguments(ASTPtr & ast)
{
    auto * data_type = ast ? ast->as<ASTDataType>() : nullptr;
    auto * tuple_type = ast ? ast->as<ASTTupleDataType>() : nullptr;
    auto * enum_type = ast ? ast->as<ASTEnumDataType>() : nullptr;
    ASTDataType * type = data_type ? data_type : (tuple_type ? static_cast<ASTDataType *>(tuple_type) : enum_type);
    if (!type || type->children.size() > 1)
        introspectionError("a physical type AST is malformed");
    if (type->children.empty())
        return nullptr;
    auto * arguments = type->children.front()->as<ASTExpressionList>();
    if (!arguments)
        introspectionError("a physical type AST has a malformed argument list");
    return arguments;
}

bool isExactDataTypeAST(const ASTPtr & ast) noexcept
{
    return ast && (ast->as<ASTDataType>() || ast->as<ASTTupleDataType>() || ast->as<ASTEnumDataType>());
}

const String & getExactDataTypeFamily(const ASTPtr & ast)
{
    const auto * data_type = ast ? ast->as<ASTDataType>() : nullptr;
    const auto * tuple_type = ast ? ast->as<ASTTupleDataType>() : nullptr;
    const auto * enum_type = ast ? ast->as<ASTEnumDataType>() : nullptr;
    const ASTDataType * type = data_type ? data_type : (tuple_type ? static_cast<const ASTDataType *>(tuple_type) : enum_type);
    if (!type)
        introspectionError("a physical type child is not an exact data-type AST");
    return type->name;
}

ASTPtr getMutableStableTypeChild(ASTPtr & ast, UInt32 ordinal)
{
    auto * arguments = getMutableTypeArguments(ast);
    if (!arguments)
        introspectionError("a logical occurrence path descends through a leaf physical type");

    const String & family = getExactDataTypeFamily(ast);
    const auto direct = [&](size_t index) -> ASTPtr
    {
        if (index >= arguments->children.size() || !isExactDataTypeAST(arguments->children[index]))
            introspectionError("a logical occurrence path contains an out-of-range physical child ordinal");
        return arguments->children[index];
    };

    if (family == "Array" || family == "Nullable" || family == "LowCardinality" || family == "QBit")
    {
        if (ordinal == 0)
            return direct(0);
        introspectionError("a logical occurrence path exceeds a unary physical type");
    }
    if (family == "Map" || family == "Variant")
    {
        if (family == "Map" && ordinal >= 2)
            introspectionError("a logical occurrence path exceeds a Map physical type");
        return direct(ordinal);
    }
    if (family == "Tuple" || family == "Nested")
    {
        if (ordinal >= arguments->children.size())
            introspectionError("a logical occurrence path exceeds a Tuple/Nested physical type");
        const auto & child = arguments->children[ordinal];
        if (auto * pair = child->as<ASTNameTypePair>())
        {
            if (!pair->type || pair->children.size() != 1 || pair->children.front().get() != pair->type.get())
                introspectionError("a named physical type child is malformed");
            return pair->type;
        }
        if (family == "Tuple" && isExactDataTypeAST(child))
            return child;
        introspectionError("a Tuple/Nested physical type child has an invalid wrapper");
    }
    if (family == "AggregateFunction" || family == "SimpleAggregateFunction")
    {
        size_t first_type = 1;
        if (family == "AggregateFunction" && !arguments->children.empty() && arguments->children.front()->as<ASTLiteral>())
            first_type = 2;
        if (ordinal > std::numeric_limits<size_t>::max() - first_type)
            introspectionError("an AggregateFunction child ordinal overflows size_t");
        return direct(first_type + ordinal);
    }
    if (family == "JSON")
    {
        std::vector<ASTObjectTypedPathArgument *> typed_paths;
        for (auto & child : arguments->children)
        {
            auto * object_argument = child->as<ASTObjectTypeArgument>();
            if (!object_argument || !object_argument->path_with_type)
                continue;
            auto * typed = object_argument->path_with_type->as<ASTObjectTypedPathArgument>();
            if (!typed || !typed->type || typed->children.size() != 1 || typed->children.front().get() != typed->type.get())
                introspectionError("a JSON typed physical path is malformed");
            typed_paths.push_back(typed);
        }
        std::sort(
            typed_paths.begin(),
            typed_paths.end(),
            [](const auto * lhs, const auto * rhs) { return binaryLess(lhs->path, rhs->path); });
        if (ordinal >= typed_paths.size())
            introspectionError("a logical occurrence path exceeds JSON typed paths");
        return typed_paths[ordinal]->type;
    }

    introspectionError("a logical occurrence path descends through an unsupported physical type family");
}

void setMutableStableTypeChild(ASTPtr & ast, UInt32 ordinal, ASTPtr child)
{
    auto * arguments = getMutableTypeArguments(ast);
    if (!arguments || !child)
        introspectionError("a physical type child replacement is malformed");

    const String & family = getExactDataTypeFamily(ast);
    const auto direct = [&](size_t index)
    {
        if (index >= arguments->children.size())
            introspectionError("a physical type child replacement ordinal is out of range");
        arguments->children[index] = std::move(child);
    };

    if (family == "Array" || family == "Nullable" || family == "LowCardinality" || family == "QBit")
    {
        if (ordinal != 0)
            introspectionError("a unary physical type replacement ordinal is out of range");
        direct(0);
        return;
    }
    if (family == "Map" || family == "Variant")
    {
        if (family == "Map" && ordinal >= 2)
            introspectionError("a Map physical type replacement ordinal is out of range");
        direct(ordinal);
        return;
    }
    if (family == "Tuple" || family == "Nested")
    {
        if (ordinal >= arguments->children.size())
            introspectionError("a Tuple/Nested physical type replacement ordinal is out of range");
        if (auto * pair = arguments->children[ordinal]->as<ASTNameTypePair>())
        {
            if (pair->children.size() != 1)
                introspectionError("a named physical type replacement wrapper is malformed");
            pair->type = std::move(child);
            pair->children.front() = pair->type;
            return;
        }
        if (family == "Tuple")
        {
            direct(ordinal);
            return;
        }
        introspectionError("a Nested physical type replacement lacks a named wrapper");
    }
    if (family == "AggregateFunction" || family == "SimpleAggregateFunction")
    {
        size_t first_type = 1;
        if (family == "AggregateFunction" && !arguments->children.empty() && arguments->children.front()->as<ASTLiteral>())
            first_type = 2;
        if (ordinal > std::numeric_limits<size_t>::max() - first_type)
            introspectionError("an AggregateFunction replacement ordinal overflows size_t");
        direct(first_type + ordinal);
        return;
    }
    if (family == "JSON")
    {
        std::vector<ASTObjectTypedPathArgument *> typed_paths;
        for (auto & argument : arguments->children)
        {
            auto * object_argument = argument->as<ASTObjectTypeArgument>();
            if (!object_argument || !object_argument->path_with_type)
                continue;
            auto * typed = object_argument->path_with_type->as<ASTObjectTypedPathArgument>();
            if (!typed || !typed->type || typed->children.size() != 1)
                introspectionError("a JSON typed physical replacement path is malformed");
            typed_paths.push_back(typed);
        }
        std::sort(
            typed_paths.begin(),
            typed_paths.end(),
            [](const auto * lhs, const auto * rhs) { return binaryLess(lhs->path, rhs->path); });
        if (ordinal >= typed_paths.size())
            introspectionError("a JSON physical type replacement ordinal is out of range");
        typed_paths[ordinal]->type = std::move(child);
        typed_paths[ordinal]->children.front() = typed_paths[ordinal]->type;
        return;
    }

    introspectionError("a logical occurrence path replaces a child of an unsupported physical type family");
}

using LogicalTypePath = std::vector<UInt32>;

LogicalTypePath normalizeSpecializationPath(
    const ASTPtr & canonical_physical_ast,
    const DataTypePtr & canonical_physical_type,
    const RelativePhysicalTypePath & source_path)
{
    if (!canonical_physical_ast || !canonical_physical_type)
        introspectionError("a checked specialization has no canonical physical root");

    ASTPtr current_ast = canonical_physical_ast;
    DataTypePtr current_type = canonical_physical_type;
    LogicalTypePath result;
    result.reserve(source_path.size());
    for (const auto & locator : source_path)
    {
        if (locator.kind == PhysicalTypeChildLocatorKind::StableOrdinal)
        {
            result.push_back(locator.source_ordinal);
            current_ast = getMutableStableTypeChild(current_ast, locator.source_ordinal);
            current_type = DataTypeFactory::instance().get(current_ast);
            continue;
        }
        if (locator.kind != PhysicalTypeChildLocatorKind::VariantNormalizedBranch)
            introspectionError("a checked specialization emitted an unknown physical-path locator");

        auto * arguments = getMutableTypeArguments(current_ast);
        const auto * variant = typeid_cast<const DataTypeVariant *>(current_type.get());
        if (!arguments || !variant || locator.source_ordinal >= arguments->children.size())
            introspectionError("a checked specialization emitted an invalid Variant branch locator");
        const ASTPtr & source_ast = arguments->children[locator.source_ordinal];
        if (!isExactDataTypeAST(source_ast))
            introspectionError("a checked specialization Variant branch is not a physical type AST");
        DataTypePtr source_type = DataTypeFactory::instance().get(source_ast);
        if (isNothing(source_type))
            introspectionError("a checked logical occurrence was erased from a Variant(Nothing) branch");

        size_t equal_source_branches = 0;
        for (const auto & branch_ast : arguments->children)
        {
            if (!isExactDataTypeAST(branch_ast))
                introspectionError("a checked specialization Variant contains a malformed branch");
            DataTypePtr branch_type = DataTypeFactory::instance().get(branch_ast);
            if (!isNothing(branch_type) && branch_type->getName() == source_type->getName())
                ++equal_source_branches;
        }
        if (equal_source_branches != 1)
            introspectionError("a checked logical occurrence was erased by collapsed equal Variant branches");

        const auto discriminator = variant->tryGetVariantDiscriminator(source_type->getName());
        if (!discriminator)
            introspectionError("Variant normalization lost its checked specialization branch");
        const size_t discriminator_index = static_cast<size_t>(*discriminator);
        if (discriminator_index >= variant->getVariants().size()
            || !variant->getVariants()[discriminator_index]->equals(*source_type)
            || discriminator_index > std::numeric_limits<UInt32>::max())
        {
            introspectionError("Variant normalization differs from its checked specialization path");
        }
        result.push_back(static_cast<UInt32>(discriminator_index));
        current_ast = source_ast;
        current_type = std::move(source_type);
    }
    return result;
}

ASTPtr canonicalArgumentToAST(const CanonicalTypeArgumentValue & argument)
{
    return std::visit(
        [](const auto & value) -> ASTPtr
        {
            using Value = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<Value, CanonicalTypeArgument>)
            {
                if (!value.getPhysicalType())
                    introspectionError("a canonical TYPE argument has no physical type");
                return dataTypeToAST(value.getPhysicalType());
            }
            else
                return make_intrusive<ASTLiteral>(Field(value));
        },
        argument.value);
}

ASTPtr makeCurrentReferenceAST(
    std::string_view database_name,
    const Definition & current_definition,
    const InstantiatedTypeDescriptor & descriptor,
    std::vector<ASTPtr> rendered_arguments)
{
    if (database_name.empty() || current_definition.getNormalizedLocalName().empty())
        introspectionError("a current type presentation has an empty database or local name");

    const auto & parameters = current_definition.getParameters();
    const auto & arguments = descriptor.getCanonicalArguments().values();
    if (parameters.size() != arguments.size() || arguments.size() != rendered_arguments.size())
        introspectionError("a descriptor argument count differs from its current definition");

    auto result = make_intrusive<ASTUDTReference>();
    result->database_name = String(database_name);
    result->type_name = current_definition.getNormalizedLocalName();
    if (arguments.empty())
        return result;

    auto argument_list = make_intrusive<ASTExpressionList>();
    argument_list->children.reserve(arguments.size());
    for (size_t index = 0; index < arguments.size(); ++index)
    {
        if (arguments[index].kind != parameters[index].kind || !rendered_arguments[index])
            introspectionError("a descriptor argument kind differs from its current definition");
        argument_list->children.push_back(std::move(rendered_arguments[index]));
    }
    result->children.push_back(std::move(argument_list));
    return result;
}

using DescriptorSequence = std::vector<UInt32>;
using LogicalOccurrenceMap = std::map<LogicalTypePath, DescriptorSequence>;

bool isPathPrefix(const LogicalTypePath & prefix, const LogicalTypePath & path) noexcept
{
    return prefix.size() <= path.size() && std::equal(prefix.begin(), prefix.end(), path.begin());
}

LogicalTypePath concatenatePaths(const LogicalTypePath & prefix, const LogicalTypePath & suffix)
{
    if (prefix.size() > 64 || suffix.size() > 64 - prefix.size())
        introspectionError("a replayed logical occurrence path exceeds the sidecar depth limit");
    LogicalTypePath result;
    result.reserve(prefix.size() + suffix.size());
    result.insert(result.end(), prefix.begin(), prefix.end());
    result.insert(result.end(), suffix.begin(), suffix.end());
    return result;
}

LogicalTypePath pathSuffix(const LogicalTypePath & path, size_t prefix_size)
{
    if (prefix_size > path.size())
        introspectionError("a replayed logical occurrence path has an invalid prefix");
    return LogicalTypePath(path.begin() + prefix_size, path.end());
}

bool isPathInsidePhysicalType(const DataTypePtr & physical_type, const LogicalTypePath & path)
{
    if (!physical_type)
        return false;
    ASTPtr current = dataTypeToAST(physical_type);
    try
    {
        for (const UInt32 ordinal : path)
            current = getMutableStableTypeChild(current, ordinal);
    }
    catch (const Exception & error)
    {
        if (error.code() == ErrorCodes::LOGICAL_ERROR)
            return false;
        throw;
    }
    return true;
}

struct CheckedReplayEvent
{
    LogicalTypePath path;
    RelativeLogicalTypeOccurrenceKind kind = RelativeLogicalTypeOccurrenceKind::Specialization;
    UInt32 source_ordinal = 0;
};

struct CheckedSpecializationReplay
{
    std::vector<CheckedReplayEvent> events;
};

struct ReplayVariable
{
    UInt32 formal = 0;
    LogicalTypePath path;

    bool operator==(const ReplayVariable &) const = default;
};

struct ReplayVariableLess
{
    bool operator()(const ReplayVariable & lhs, const ReplayVariable & rhs) const noexcept
    {
        if (lhs.formal != rhs.formal)
            return lhs.formal < rhs.formal;
        return lhs.path < rhs.path;
    }
};

struct ReplayWordPart
{
    bool is_constant = false;
    UInt32 constant = 0;
    ReplayVariable variable;
};

struct ReplayEquation
{
    /// At one normalized physical endpoint, the persisted descriptor stream
    /// must equal the ordered checked Specialization constants with each
    /// TypeArgument event replaced by that formal actual's endpoint stream.
    DescriptorSequence actual;
    std::vector<ReplayWordPart> parts;
    size_t distinct_variables = 0;
};

using ReplayAssignments = std::map<ReplayVariable, DescriptorSequence, ReplayVariableLess>;

class IntrospectionWorkBudget
{
public:
    explicit IntrospectionWorkBudget(UInt64 maximum_work_)
        : maximum_work(maximum_work_)
    {
    }

    void charge(UInt64 amount = 1)
    {
        if (amount > maximum_work - work)
            introspectionError("logical introspection exceeds its bounded work");
        work += amount;
    }

private:
    const UInt64 maximum_work;
    UInt64 work = 0;
};

constexpr UInt64 maximum_root_introspection_work = 16ULL << 20;
constexpr UInt64 maximum_occurrence_presentation_work = 64ULL << 20;

struct ReplayAssignmentFootprint
{
    UInt64 entries = 0;
    UInt64 path_components = 0;
    UInt64 descriptor_indices = 0;
};

constexpr UInt64 maximum_replay_candidate_states = 4'096;
/// One active state may coexist with the complete bounded pending/result set
/// while it is being expanded or transferred.
constexpr UInt64 maximum_replay_retained_states = maximum_replay_candidate_states + 1;
constexpr UInt64 maximum_replay_assignment_entries = 65'536;
constexpr UInt64 maximum_replay_assignment_path_components = 4ULL << 20;
constexpr UInt64 maximum_replay_assignment_descriptor_indices = 4ULL << 20;

UInt64 replaySize(size_t value) noexcept
{
    static_assert(sizeof(size_t) <= sizeof(UInt64));
    return static_cast<UInt64>(value);
}

void addReplayFootprintValue(UInt64 & destination, UInt64 addition, UInt64 maximum, std::string_view message)
{
    if (destination > maximum || addition > maximum - destination)
        introspectionError(message);
    destination += addition;
}

ReplayAssignmentFootprint extendReplayAssignmentFootprint(
    ReplayAssignmentFootprint footprint, const ReplayVariable & variable, size_t descriptor_indices)
{
    addReplayFootprintValue(
        footprint.entries, 1, maximum_replay_assignment_entries, "logical-lineage replay retains too many assignments");
    addReplayFootprintValue(
        footprint.path_components,
        replaySize(variable.path.size()),
        maximum_replay_assignment_path_components,
        "logical-lineage replay retains too many assignment-path components");
    addReplayFootprintValue(
        footprint.descriptor_indices,
        replaySize(descriptor_indices),
        maximum_replay_assignment_descriptor_indices,
        "logical-lineage replay retains too many assigned descriptor indices");
    return footprint;
}

void chargeReplayAssignmentsTraversal(const ReplayAssignments & assignments, IntrospectionWorkBudget & budget)
{
    for (const auto & [variable, value] : assignments)
    {
        budget.charge();
        budget.charge(replaySize(variable.path.size()));
        budget.charge(replaySize(value.size()));
    }
}

bool replayAssignmentsEqual(
    const ReplayAssignments & lhs, const ReplayAssignments & rhs, IntrospectionWorkBudget & budget)
{
    budget.charge();
    if (lhs.size() != rhs.size())
        return false;
    /// std::map equality can compare every retained path and descriptor stream.
    /// Charge both operands prospectively before invoking that deep comparison.
    chargeReplayAssignmentsTraversal(lhs, budget);
    chargeReplayAssignmentsTraversal(rhs, budget);
    return lhs == rhs;
}

bool containsReplayAssignments(
    const std::vector<ReplayAssignments> & assignments,
    const ReplayAssignments & candidate,
    IntrospectionWorkBudget & budget)
{
    for (const auto & existing : assignments)
        if (replayAssignmentsEqual(existing, candidate, budget))
            return true;
    return false;
}

class ReplayRetainedStateBudget
{
public:
    void checkCanRetain(const ReplayAssignmentFootprint & footprint) const
    {
        checkAddition(states, 1, maximum_replay_retained_states, "logical-lineage replay exceeds its retained-state limit");
        checkAddition(
            entries,
            footprint.entries,
            maximum_replay_assignment_entries,
            "logical-lineage replay frontier retains too many assignments");
        checkAddition(
            path_components,
            footprint.path_components,
            maximum_replay_assignment_path_components,
            "logical-lineage replay frontier retains too many assignment-path components");
        checkAddition(
            descriptor_indices,
            footprint.descriptor_indices,
            maximum_replay_assignment_descriptor_indices,
            "logical-lineage replay frontier retains too many assigned descriptor indices");
    }

    void retain(const ReplayAssignmentFootprint & footprint)
    {
        checkCanRetain(footprint);
        ++states;
        entries += footprint.entries;
        path_components += footprint.path_components;
        descriptor_indices += footprint.descriptor_indices;
    }

    void release(const ReplayAssignmentFootprint & footprint)
    {
        if (!states || footprint.entries > entries || footprint.path_components > path_components
            || footprint.descriptor_indices > descriptor_indices)
        {
            introspectionError("logical-lineage replay retained-state accounting is inconsistent");
        }
        --states;
        entries -= footprint.entries;
        path_components -= footprint.path_components;
        descriptor_indices -= footprint.descriptor_indices;
    }

    void checkCanReplace(const ReplayAssignmentFootprint & before, const ReplayAssignmentFootprint & after) const
    {
        if (!states || before.entries > entries || before.path_components > path_components
            || before.descriptor_indices > descriptor_indices)
        {
            introspectionError("logical-lineage replay retained-state replacement is inconsistent");
        }
        checkAddition(
            entries - before.entries,
            after.entries,
            maximum_replay_assignment_entries,
            "logical-lineage replay frontier retains too many assignments");
        checkAddition(
            path_components - before.path_components,
            after.path_components,
            maximum_replay_assignment_path_components,
            "logical-lineage replay frontier retains too many assignment-path components");
        checkAddition(
            descriptor_indices - before.descriptor_indices,
            after.descriptor_indices,
            maximum_replay_assignment_descriptor_indices,
            "logical-lineage replay frontier retains too many assigned descriptor indices");
    }

    void replace(const ReplayAssignmentFootprint & before, const ReplayAssignmentFootprint & after)
    {
        checkCanReplace(before, after);
        entries = entries - before.entries + after.entries;
        path_components = path_components - before.path_components + after.path_components;
        descriptor_indices = descriptor_indices - before.descriptor_indices + after.descriptor_indices;
    }

private:
    static void checkAddition(UInt64 current, UInt64 addition, UInt64 maximum, std::string_view message)
    {
        if (current > maximum || addition > maximum - current)
            introspectionError(message);
    }

    UInt64 states = 0;
    UInt64 entries = 0;
    UInt64 path_components = 0;
    UInt64 descriptor_indices = 0;
};

const DescriptorSequence * findReplayAssignment(
    const ReplayAssignments & fixed,
    const ReplayAssignments & local,
    const ReplayVariable & variable)
{
    if (const auto found = local.find(variable); found != local.end())
        return std::addressof(found->second);
    if (const auto found = fixed.find(variable); found != fixed.end())
        return std::addressof(found->second);
    return nullptr;
}

bool matchesSequence(const DescriptorSequence & actual, size_t offset, const DescriptorSequence & expected) noexcept
{
    return expected.size() <= actual.size() - std::min(offset, actual.size())
        && offset <= actual.size()
        && std::equal(expected.begin(), expected.end(), actual.begin() + offset);
}

size_t minimumReplaySuffixSize(
    const ReplayEquation & equation,
    size_t first_part,
    const ReplayAssignments & fixed,
    const ReplayAssignments & local,
    IntrospectionWorkBudget & budget)
{
    size_t result = 0;
    for (size_t index = first_part; index < equation.parts.size(); ++index)
    {
        budget.charge();
        const auto & part = equation.parts[index];
        size_t addition = part.is_constant ? 1 : 0;
        if (!part.is_constant)
        {
            if (const auto * assigned = findReplayAssignment(fixed, local, part.variable))
                addition = assigned->size();
        }
        if (addition > equation.actual.size() - std::min(result, equation.actual.size()))
            return equation.actual.size() + 1;
        result += addition;
    }
    return result;
}

std::vector<ReplayAssignments> enumerateEquationAssignments(
    const ReplayEquation & equation,
    const ReplayAssignments & fixed,
    IntrospectionWorkBudget & budget)
{
    struct State
    {
        size_t part = 0;
        size_t actual = 0;
        ReplayAssignments local;
        ReplayAssignmentFootprint footprint;
    };

    std::vector<State> pending;
    pending.reserve(maximum_replay_candidate_states);
    pending.push_back({});
    ReplayRetainedStateBudget retained_states;
    retained_states.retain({});
    std::vector<ReplayAssignments> result;
    result.reserve(maximum_replay_candidate_states);
    while (!pending.empty())
    {
        budget.charge();
        State state = std::move(pending.back());
        pending.pop_back();
        bool stopped = false;
        while (state.part < equation.parts.size())
        {
            budget.charge();
            const auto & part = equation.parts[state.part];
            if (part.is_constant)
            {
                if (state.actual >= equation.actual.size() || equation.actual[state.actual] != part.constant)
                {
                    stopped = true;
                    break;
                }
                ++state.part;
                ++state.actual;
                continue;
            }

            if (const auto * assigned = findReplayAssignment(fixed, state.local, part.variable))
            {
                if (!matchesSequence(equation.actual, state.actual, *assigned))
                {
                    stopped = true;
                    break;
                }
                ++state.part;
                state.actual += assigned->size();
                continue;
            }

            const size_t minimum_suffix
                = minimumReplaySuffixSize(equation, state.part + 1, fixed, state.local, budget);
            if (state.actual > equation.actual.size() || minimum_suffix > equation.actual.size() - state.actual)
            {
                stopped = true;
                break;
            }
            const size_t maximum_length = equation.actual.size() - state.actual - minimum_suffix;
            size_t fixed_suffix = 0;
            size_t same_variable_occurrences = 0;
            bool has_other_unassigned_variable = false;
            for (size_t index = state.part; index < equation.parts.size(); ++index)
            {
                budget.charge();
                const auto & suffix_part = equation.parts[index];
                if (suffix_part.is_constant)
                {
                    ++fixed_suffix;
                    continue;
                }
                if (suffix_part.variable == part.variable)
                {
                    ++same_variable_occurrences;
                    continue;
                }
                if (const auto * assigned = findReplayAssignment(fixed, state.local, suffix_part.variable))
                    fixed_suffix += assigned->size();
                else
                    has_other_unassigned_variable = true;
            }

            const auto push_candidate = [&](size_t candidate_length)
            {
                const auto candidate_footprint
                    = extendReplayAssignmentFootprint(state.footprint, part.variable, candidate_length);
                retained_states.checkCanRetain(candidate_footprint);
                /// Preflight both the retained frontier and the full deep-copy
                /// work before allocating a branch state.
                chargeReplayAssignmentsTraversal(state.local, budget);
                budget.charge();
                budget.charge(replaySize(part.variable.path.size()));
                budget.charge(replaySize(candidate_length));
                State candidate = state;
                DescriptorSequence value(
                    equation.actual.begin() + state.actual,
                    equation.actual.begin() + state.actual + candidate_length);
                if (!candidate.local.emplace(part.variable, std::move(value)).second)
                    introspectionError("logical-lineage replay branched an already assigned variable");
                candidate.footprint = candidate_footprint;
                ++candidate.part;
                candidate.actual += candidate_length;
                pending.push_back(std::move(candidate));
                retained_states.retain(candidate_footprint);
            };

            if (!has_other_unassigned_variable)
            {
                const size_t remaining = equation.actual.size() - state.actual;
                if (same_variable_occurrences && remaining >= fixed_suffix
                    && (remaining - fixed_suffix) % same_variable_occurrences == 0)
                {
                    push_candidate((remaining - fixed_suffix) / same_variable_occurrences);
                }
            }
            else
            {
                if (maximum_length + 1 > 4'096)
                    introspectionError("logical-lineage replay has an unbounded word split");
                for (size_t candidate_length = maximum_length + 1; candidate_length-- > 0;)
                {
                    budget.charge();
                    push_candidate(candidate_length);
                }
            }
            stopped = true;
            break;
        }

        bool retained_as_result = false;
        if (!stopped && state.actual == equation.actual.size())
        {
            if (!containsReplayAssignments(result, state.local, budget))
            {
                if (replaySize(result.size()) >= maximum_replay_candidate_states)
                    introspectionError("logical-lineage replay has too many candidate word splits");
                result.push_back(std::move(state.local));
                /// The active state's retained footprint is transferred to the
                /// result vector and remains covered by retained_states.
                retained_as_result = true;
            }
        }
        if (!retained_as_result)
            retained_states.release(state.footprint);
    }
    return result;
}

void mergeReplayAssignments(ReplayAssignments & destination, const ReplayAssignments & source)
{
    for (const auto & [variable, value] : source)
    {
        const auto [found, inserted] = destination.emplace(variable, value);
        if (!inserted && found->second != value)
            introspectionError("logical-lineage replay produced conflicting formal substitutions");
    }
}

ReplayAssignmentFootprint prospectiveMergedReplayAssignmentFootprint(
    const ReplayAssignments & destination,
    ReplayAssignmentFootprint footprint,
    const ReplayAssignments & source,
    IntrospectionWorkBudget & budget)
{
    for (const auto & [variable, value] : source)
    {
        budget.charge();
        budget.charge(replaySize(variable.path.size()));
        budget.charge(replaySize(value.size()));
        const auto found = destination.find(variable);
        if (found != destination.end())
        {
            if (found->second != value)
                introspectionError("logical-lineage replay produced conflicting formal substitutions");
            continue;
        }
        footprint = extendReplayAssignmentFootprint(std::move(footprint), variable, value.size());
    }
    return footprint;
}

ReplayAssignments solveReplayEquations(
    const std::vector<ReplayEquation> & equations,
    const std::set<ReplayVariable, ReplayVariableLess> & variables,
    IntrospectionWorkBudget & budget)
{
    struct State
    {
        ReplayAssignments assignments;
        ReplayAssignmentFootprint footprint;
    };

    std::vector<State> pending;
    pending.reserve(maximum_replay_candidate_states);
    pending.push_back({});
    ReplayRetainedStateBudget retained_states;
    retained_states.retain({});
    std::vector<ReplayAssignments> solutions;
    solutions.reserve(2);
    while (!pending.empty() && solutions.size() < 2)
    {
        budget.charge();
        State state = std::move(pending.back());
        pending.pop_back();
        auto & assignments = state.assignments;
        bool rejected = false;
        bool retained_as_solution = false;
        while (true)
        {
            std::optional<std::vector<ReplayAssignments>> branch;
            bool propagated = false;
            for (const auto & equation : equations)
            {
                auto candidates = enumerateEquationAssignments(equation, assignments, budget);
                if (candidates.empty())
                {
                    rejected = true;
                    break;
                }
                if (candidates.size() == 1)
                {
                    const size_t before = assignments.size();
                    const auto after_footprint = prospectiveMergedReplayAssignmentFootprint(
                        assignments, state.footprint, candidates.front(), budget);
                    retained_states.checkCanReplace(state.footprint, after_footprint);
                    mergeReplayAssignments(assignments, candidates.front());
                    retained_states.replace(state.footprint, after_footprint);
                    state.footprint = after_footprint;
                    if (assignments.size() != before)
                    {
                        propagated = true;
                        break;
                    }
                }
                else if (!branch || candidates.size() < branch->size())
                    branch = std::move(candidates);
            }
            if (rejected)
                break;
            if (propagated)
                continue;
            if (branch)
            {
                for (auto candidate = branch->rbegin(); candidate != branch->rend(); ++candidate)
                {
                    const auto next_footprint
                        = prospectiveMergedReplayAssignmentFootprint(assignments, state.footprint, *candidate, budget);
                    retained_states.checkCanRetain(next_footprint);
                    chargeReplayAssignmentsTraversal(assignments, budget);
                    ReplayAssignments next = assignments;
                    mergeReplayAssignments(next, *candidate);
                    pending.push_back({.assignments = std::move(next), .footprint = next_footprint});
                    retained_states.retain(next_footprint);
                }
                break;
            }
            if (assignments.size() != variables.size())
                introspectionError("logical-lineage replay left an unconstrained formal path");
            if (!containsReplayAssignments(solutions, assignments, budget))
            {
                solutions.push_back(std::move(assignments));
                /// The active state's footprint is transferred to solutions.
                retained_as_solution = true;
            }
            break;
        }
        if (!retained_as_solution)
            retained_states.release(state.footprint);
    }

    if (solutions.empty())
        introspectionError("the persisted occurrences do not match checked specialization lineage");
    if (solutions.size() != 1 || !pending.empty())
        introspectionError("the V1 sidecar does not uniquely identify nested TYPE-argument lineage");
    return std::move(solutions.front());
}

std::vector<LogicalOccurrenceMap> recoverTypeArgumentOccurrences(
    const CheckedSpecializationReplay & replay,
    const InstantiatedTypeDescriptor & descriptor,
    const LogicalOccurrenceMap & occurrences,
    IntrospectionWorkBudget & budget)
{
    const auto & parameters = descriptor.getDefinition()->getParameters();
    const auto & arguments = descriptor.getCanonicalArguments().values();
    if (parameters.size() != arguments.size())
        introspectionError("a checked descriptor has inconsistent formal arguments");

    std::set<LogicalTypePath> equation_paths;
    for (const auto & [path, sequence] : occurrences)
    {
        budget.charge(1 + path.size() + sequence.size());
        if (sequence.empty())
            introspectionError("a logical occurrence map contains an empty endpoint");
        equation_paths.insert(path);
    }

    std::set<ReplayVariable, ReplayVariableLess> variables;
    std::vector<std::vector<const CheckedReplayEvent *>> substitutions_by_formal(arguments.size());
    std::map<LogicalTypePath, std::vector<size_t>> specialization_events_by_path;
    std::map<LogicalTypePath, std::vector<size_t>> substitution_events_by_path;
    for (size_t event_index = 0; event_index < replay.events.size(); ++event_index)
    {
        const auto & event = replay.events[event_index];
        budget.charge(1 + event.path.size());
        if (event.kind == RelativeLogicalTypeOccurrenceKind::Specialization)
        {
            equation_paths.insert(event.path);
            specialization_events_by_path[event.path].push_back(event_index);
            continue;
        }
        if (event.kind != RelativeLogicalTypeOccurrenceKind::TypeArgument || event.source_ordinal >= arguments.size()
            || arguments[event.source_ordinal].kind != ParameterKind::Type)
        {
            introspectionError("a checked specialization emitted an invalid TYPE-argument event");
        }
        substitutions_by_formal[event.source_ordinal].push_back(std::addressof(event));
        substitution_events_by_path[event.path].push_back(event_index);
        /// Lexicographic ordering makes the descendants of one physical path
        /// a contiguous range. Avoid scanning unrelated occurrence endpoints.
        for (auto occurrence = occurrences.lower_bound(event.path);
             occurrence != occurrences.end() && isPathPrefix(event.path, occurrence->first);
             ++occurrence)
        {
            const auto & path = occurrence->first;
            budget.charge(1 + path.size());
            ReplayVariable variable{event.source_ordinal, pathSuffix(path, event.path.size())};
            if (!variables.contains(variable))
            {
                const auto & canonical = std::get<CanonicalTypeArgument>(arguments[event.source_ordinal].value);
                if (isPathInsidePhysicalType(canonical.getPhysicalType(), variable.path))
                    variables.insert(std::move(variable));
            }
        }
    }

    for (const auto & variable : variables)
    {
        if (variable.formal >= substitutions_by_formal.size())
            introspectionError("a replay variable exceeds the formal substitution index");
        for (const auto * substitution : substitutions_by_formal[variable.formal])
        {
            budget.charge(1 + variable.path.size() + substitution->path.size());
            equation_paths.insert(concatenatePaths(substitution->path, variable.path));
        }
    }

    std::vector<ReplayEquation> equations;
    equations.reserve(equation_paths.size());
    for (const auto & path : equation_paths)
    {
        budget.charge(1 + path.size());
        ReplayEquation equation;
        if (const auto found = occurrences.find(path); found != occurrences.end())
            equation.actual = found->second;

        std::vector<size_t> relevant_event_indices;
        const auto append_event_indices = [&](const std::vector<size_t> & indices)
        {
            budget.charge(replaySize(indices.size()));
            if (indices.size() > relevant_event_indices.max_size() - relevant_event_indices.size())
                introspectionError("a replay equation event index exceeds the host size domain");
            relevant_event_indices.insert(relevant_event_indices.end(), indices.begin(), indices.end());
        };
        if (const auto found = specialization_events_by_path.find(path); found != specialization_events_by_path.end())
            append_event_indices(found->second);

        LogicalTypePath prefix;
        prefix.reserve(path.size());
        for (size_t prefix_size = 0;; ++prefix_size)
        {
            budget.charge();
            if (const auto found = substitution_events_by_path.find(prefix); found != substitution_events_by_path.end())
                append_event_indices(found->second);
            if (prefix_size == path.size())
                break;
            prefix.push_back(path[prefix_size]);
        }

        /// Buckets retain their local order, but events from different path
        /// prefixes must be merged back into the checked global outer-to-inner
        /// lineage stream.
        for (size_t sort_level = relevant_event_indices.size(); sort_level > 1;
             sort_level = sort_level / 2 + sort_level % 2)
        {
            budget.charge(replaySize(relevant_event_indices.size()));
        }
        std::sort(relevant_event_indices.begin(), relevant_event_indices.end());
        if (std::adjacent_find(relevant_event_indices.begin(), relevant_event_indices.end()) != relevant_event_indices.end())
            introspectionError("a replay equation indexed one checked event more than once");

        for (const size_t event_index : relevant_event_indices)
        {
            if (event_index >= replay.events.size())
                introspectionError("a replay equation contains an out-of-range event index");
            const auto & event = replay.events[event_index];
            budget.charge(1 + event.path.size());
            if (event.kind == RelativeLogicalTypeOccurrenceKind::Specialization)
            {
                if (event.path != path)
                    introspectionError("a replay equation indexed a specialization at another physical path");
                equation.parts.push_back({.is_constant = true, .constant = event.source_ordinal, .variable = {}});
            }
            else if (event.kind == RelativeLogicalTypeOccurrenceKind::TypeArgument && isPathPrefix(event.path, path))
            {
                ReplayVariable variable{event.source_ordinal, pathSuffix(path, event.path.size())};
                if (variables.contains(variable))
                    equation.parts.push_back({.is_constant = false, .constant = 0, .variable = std::move(variable)});
            }
            else
                introspectionError("a replay equation indexed an invalid TYPE-argument event");
        }
        std::set<ReplayVariable, ReplayVariableLess> equation_variables;
        for (const auto & part : equation.parts)
            if (!part.is_constant)
                equation_variables.insert(part.variable);
        equation.distinct_variables = equation_variables.size();
        equations.push_back(std::move(equation));
    }
    std::stable_sort(
        equations.begin(),
        equations.end(),
        [](const ReplayEquation & lhs, const ReplayEquation & rhs)
        {
            if (lhs.distinct_variables != rhs.distinct_variables)
                return lhs.distinct_variables < rhs.distinct_variables;
            return lhs.parts.size() < rhs.parts.size();
        });

    ReplayAssignments assignments = solveReplayEquations(equations, variables, budget);
    std::vector<LogicalOccurrenceMap> result(arguments.size());
    for (auto & [variable, sequence] : assignments)
    {
        budget.charge(1 + variable.path.size() + sequence.size());
        if (variable.formal >= result.size())
            introspectionError("a solved TYPE-argument variable exceeds the formal domain");
        if (!sequence.empty())
            result[variable.formal].emplace(variable.path, std::move(sequence));
    }
    return result;
}

struct IntrospectionReplayContext
{
    std::string_view database_name;
    std::span<const InstantiatedTypeDescriptor::Ptr> descriptors;
    const std::vector<Definition::Ptr> & current_definitions;
    const std::vector<CheckedSpecializationReplay> & replays;
    IntrospectionWorkBudget & budget;
};

struct LogicalReplacementNode
{
    ASTPtr replacement;
    std::map<UInt32, LogicalReplacementNode> children;
};

ASTPtr applyLogicalReplacements(ASTPtr physical_ast, const LogicalReplacementNode & path)
{
    if (path.replacement)
    {
        if (!path.children.empty())
            introspectionError("logical surface replacements contain a prefix collision");
        return path.replacement;
    }
    for (const auto & [ordinal, child_path] : path.children)
    {
        ASTPtr child = getMutableStableTypeChild(physical_ast, ordinal);
        child = applyLogicalReplacements(std::move(child), child_path);
        setMutableStableTypeChild(physical_ast, ordinal, std::move(child));
    }
    return physical_ast;
}

ASTPtr renderLogicalSurface(
    ASTPtr physical_ast,
    const LogicalOccurrenceMap & occurrences,
    const IntrospectionReplayContext & context,
    UInt64 lineage_depth);

ASTPtr renderLogicalApplication(
    UInt32 descriptor_index,
    const LogicalOccurrenceMap & occurrences,
    const IntrospectionReplayContext & context,
    UInt64 lineage_depth)
{
    if (lineage_depth > 64 || descriptor_index >= context.descriptors.size()
        || descriptor_index >= context.current_definitions.size() || descriptor_index >= context.replays.size()
        || !context.descriptors[descriptor_index] || !context.current_definitions[descriptor_index])
    {
        introspectionError("nested logical TYPE-argument lineage exceeds its checked presentation domain");
    }
    const auto root = occurrences.find({});
    if (root == occurrences.end() || root->second.empty() || root->second.front() != descriptor_index)
        introspectionError("a logical application does not begin with its checked outer descriptor");

    const auto & descriptor = *context.descriptors[descriptor_index];
    context.budget.charge(1 + occurrences.size());
    auto argument_occurrences
        = recoverTypeArgumentOccurrences(context.replays[descriptor_index], descriptor, occurrences, context.budget);
    const auto & arguments = descriptor.getCanonicalArguments().values();
    const auto & parameters = context.current_definitions[descriptor_index]->getParameters();
    if (arguments.size() != parameters.size() || argument_occurrences.size() != arguments.size())
        introspectionError("a replayed logical application has an inconsistent argument vector");

    std::vector<ASTPtr> rendered_arguments;
    rendered_arguments.reserve(arguments.size());
    for (size_t index = 0; index < arguments.size(); ++index)
    {
        if (arguments[index].kind != parameters[index].kind)
            introspectionError("a replayed logical application has an inconsistent argument kind");
        if (arguments[index].kind != ParameterKind::Type)
        {
            if (!argument_occurrences[index].empty())
                introspectionError("a value argument retained logical TYPE occurrences");
            rendered_arguments.push_back(canonicalArgumentToAST(arguments[index]));
            continue;
        }

        const auto & canonical = std::get<CanonicalTypeArgument>(arguments[index].value);
        if (!canonical.getPhysicalType())
            introspectionError("a replayed canonical TYPE argument has no physical type");
        ASTPtr rendered = dataTypeToAST(canonical.getPhysicalType());
        if (!argument_occurrences[index].empty())
        {
            rendered = renderLogicalSurface(
                std::move(rendered), argument_occurrences[index], context, lineage_depth + 1);
        }
        rendered_arguments.push_back(std::move(rendered));
    }
    return makeCurrentReferenceAST(
        context.database_name,
        *context.current_definitions[descriptor_index],
        descriptor,
        std::move(rendered_arguments));
}

ASTPtr renderLogicalSurface(
    ASTPtr physical_ast,
    const LogicalOccurrenceMap & occurrences,
    const IntrospectionReplayContext & context,
    UInt64 lineage_depth)
{
    if (!physical_ast || occurrences.empty())
        return physical_ast;

    std::vector<LogicalTypePath> roots;
    for (const auto & [path, sequence] : occurrences)
    {
        context.budget.charge(1 + path.size() + sequence.size());
        if (sequence.empty())
            introspectionError("a logical surface contains an empty occurrence endpoint");
        /// std::map's lexicographic ordering makes every prefix subtree a
        /// contiguous range. Only the latest root can contain this path.
        if (roots.empty() || !isPathPrefix(roots.back(), path))
            roots.push_back(path);
    }

    LogicalReplacementNode replacements;
    for (const auto & root : roots)
    {
        LogicalOccurrenceMap application_occurrences;
        for (auto occurrence = occurrences.lower_bound(root);
             occurrence != occurrences.end() && isPathPrefix(root, occurrence->first);
             ++occurrence)
        {
            const auto & [path, sequence] = *occurrence;
            context.budget.charge(1 + path.size() - root.size() + sequence.size());
            application_occurrences.emplace(pathSuffix(path, root.size()), sequence);
        }
        const auto application_root = application_occurrences.find({});
        if (application_root == application_occurrences.end() || application_root->second.empty())
            introspectionError("a logical surface root has no outer occurrence");
        ASTPtr replacement = renderLogicalApplication(
            application_root->second.front(), application_occurrences, context, lineage_depth);

        LogicalReplacementNode * destination = std::addressof(replacements);
        for (const UInt32 ordinal : root)
        {
            if (destination->replacement)
                introspectionError("logical surface replacements contain an ancestor collision");
            destination = std::addressof(destination->children[ordinal]);
        }
        if (destination->replacement || !destination->children.empty())
            introspectionError("logical surface replacements contain a duplicate path");
        destination->replacement = std::move(replacement);
    }
    return applyLogicalReplacements(std::move(physical_ast), replacements);
}

void validateCurrentDefinition(
    const Definition::Ptr & retained,
    const Definition::Ptr & current,
    const UUID & database_uuid)
{
    if (!retained || !current || retained->getIdentity() != current->getIdentity()
        || current->getIdentity().database_uuid != database_uuid
        || current->getIdentity().type_uuid == UUIDHelpers::Nil || !current->getIdentity().revision
        || retained->getDefinitionHash() != current->getDefinitionHash()
        || !retained->hasSameCheckedSemantics(*current) || current->getNormalizedLocalName().empty())
    {
        introspectionError("a retained definition is inconsistent with the current authority snapshot");
    }
}

std::vector<CheckedSpecializationReplay> prepareCheckedSpecializationReplays(
    IDatabase & database,
    const ILifecycleSnapshot & lifecycle_snapshot,
    std::span<const InstantiatedTypeDescriptor::Ptr> descriptors,
    std::span<const UInt32> selected_descriptor_indices)
{
    const auto * authority = lifecycle_snapshot.getResolutionAuthorityAdapter();
    if (!authority || authority->getDatabaseUUID() != database.getUUID())
        introspectionError("the lifecycle snapshot has no matching resolution authority");
    if (descriptors.size() > std::numeric_limits<UInt32>::max())
        introspectionError("the descriptor dictionary exceeds the replay index domain");

    std::vector<TemplateSpecializationID> requested;
    requested.reserve(selected_descriptor_indices.size());
    FinishedTemplateSpecializations finished;
    try
    {
        auto attempt = TemplateSpecializer::Attempt::begin(*authority);
        for (const UInt32 descriptor_index : selected_descriptor_indices)
        {
            if (descriptor_index >= descriptors.size())
                introspectionError("a selected replay descriptor index is out of range");
            const auto & descriptor = descriptors[descriptor_index];
            if (!descriptor || !descriptor->getDefinition())
                introspectionError("the descriptor dictionary contains an empty replay input");
            requested.push_back(attempt.specialize(
                descriptor->getDefinition()->getIdentity(), descriptor->getCanonicalArguments()));
        }
        finished = attempt.finish();
    }
    catch (const TemplateSpecializerError &)
    {
        introspectionError("the current authority cannot replay a retained checked specialization");
    }

    if (finished.specializations.size() != selected_descriptor_indices.size())
        introspectionError("checked re-specialization produced a different selected descriptor closure");
    constexpr UInt32 invalid_descriptor = std::numeric_limits<UInt32>::max();
    std::vector<UInt32> descriptor_by_specialization(finished.specializations.size(), invalid_descriptor);
    for (size_t selected_index = 0; selected_index < requested.size(); ++selected_index)
    {
        const auto specialization_id = requested[selected_index];
        if (specialization_id >= descriptor_by_specialization.size()
            || descriptor_by_specialization[specialization_id] != invalid_descriptor)
        {
            introspectionError("checked re-specialization does not map one-to-one to persisted descriptors");
        }
        descriptor_by_specialization[specialization_id] = selected_descriptor_indices[selected_index];
    }

    for (const auto & retained : finished.definition_handles)
    {
        if (!retained)
            introspectionError("checked re-specialization retained an empty definition handle");
        auto current = lifecycle_snapshot.findCheckedDefinitionByIdentity(retained->getIdentity());
        validateCurrentDefinition(retained, current, database.getUUID());
    }

    std::vector<CheckedSpecializationReplay> result(descriptors.size());
    for (size_t specialization_id = 0; specialization_id < finished.specializations.size(); ++specialization_id)
    {
        const UInt32 descriptor_index = descriptor_by_specialization[specialization_id];
        if (descriptor_index == invalid_descriptor || descriptor_index >= descriptors.size())
            introspectionError("checked re-specialization left an unmapped specialization");
        const auto & specialization = finished.specializations[specialization_id];
        const auto & descriptor = descriptors[descriptor_index];
        if (!specialization.canonical_physical_ast
            || specialization.definition_identity != descriptor->getDefinition()->getIdentity()
            || specialization.canonical_arguments != descriptor->getCanonicalArguments())
        {
            introspectionError("checked re-specialization differs from a persisted descriptor identity or arguments");
        }
        DataTypePtr replayed_physical_type = DataTypeFactory::instance().get(specialization.canonical_physical_ast);
        if (!replayed_physical_type || !replayed_physical_type->equals(*descriptor->getPhysicalType())
            || replayed_physical_type->getName() != descriptor->getPhysicalType()->getName())
        {
            introspectionError("checked re-specialization differs from a persisted descriptor physical type");
        }

        auto & replay = result[descriptor_index];
        replay.events.reserve(specialization.relative_occurrences.size());
        std::vector<size_t> type_argument_event_indices;
        type_argument_event_indices.reserve(specialization.relative_occurrences.size());
        for (const auto & occurrence : specialization.relative_occurrences)
        {
            CheckedReplayEvent event;
            event.path = normalizeSpecializationPath(
                specialization.canonical_physical_ast, replayed_physical_type, occurrence.path);
            event.kind = occurrence.kind;
            if (occurrence.kind == RelativeLogicalTypeOccurrenceKind::Specialization)
            {
                if (occurrence.source_ordinal >= descriptor_by_specialization.size()
                    || descriptor_by_specialization[occurrence.source_ordinal] == invalid_descriptor)
                {
                    introspectionError("checked replay references an absent specialization descriptor");
                }
                event.source_ordinal = descriptor_by_specialization[occurrence.source_ordinal];
            }
            else if (occurrence.kind == RelativeLogicalTypeOccurrenceKind::TypeArgument)
            {
                event.source_ordinal = occurrence.source_ordinal;
                type_argument_event_indices.push_back(replay.events.size());
            }
            else
                introspectionError("checked replay contains an unknown logical-lineage event");
            replay.events.push_back(std::move(event));
        }
        std::sort(
            type_argument_event_indices.begin(),
            type_argument_event_indices.end(),
            [&replay](size_t lhs, size_t rhs) { return replay.events[lhs].path < replay.events[rhs].path; });
        for (size_t index = 1; index < type_argument_event_indices.size(); ++index)
        {
            const auto & previous = replay.events[type_argument_event_indices[index - 1]].path;
            const auto & current = replay.events[type_argument_event_indices[index]].path;
            if (isPathPrefix(previous, current))
                introspectionError("checked replay TYPE-argument paths are not prefix-free");
        }
        if (replay.events.empty() || replay.events.front().kind != RelativeLogicalTypeOccurrenceKind::Specialization
            || !replay.events.front().path.empty() || replay.events.front().source_ordinal != descriptor_index)
        {
            introspectionError("checked replay lacks its outer specialization event");
        }
    }
    return result;
}

}

namespace
{

struct StoredObjectEndpointKey
{
    PersistedTypePathSection section{};
    PersistedTypeOccurrenceSite site{};
    UInt64 object_ordinal = 0;

    friend bool operator<(const StoredObjectEndpointKey & lhs, const StoredObjectEndpointKey & rhs) noexcept
    {
        if (lhs.section != rhs.section)
            return static_cast<UInt8>(lhs.section) < static_cast<UInt8>(rhs.section);
        if (lhs.site != rhs.site)
            return static_cast<UInt8>(lhs.site) < static_cast<UInt8>(rhs.site);
        return lhs.object_ordinal < rhs.object_ordinal;
    }
};

struct CurrentStoredObjectEndpoint
{
    StoredObjectEndpointKey key;
    String runtime_owner_key;
    DataTypePtr physical_type;
    ASTPtr declared_type{};
    bool has_logical_references = false;
    LogicalOccurrenceMap logical_occurrences{};
    std::map<std::vector<UInt64>, UInt64> next_occurrence_ordinals{};
};

struct StoredObjectEndpointInventory
{
    std::vector<CurrentStoredObjectEndpoint> endpoints;
    std::vector<ViewAuxiliaryPhysicalTypeBindingInput> view_auxiliary;
    Digest physical_schema_fingerprint{};
};

StoredObjectEndpointInventory collectStoredObjectPhysicalEndpoints(
    const ASTCreateQuery & create, const StorageInMemoryMetadata & metadata, const BoundObjectTypeReferences & bound)
{
    StoredObjectEndpointInventory result;
    const auto & object = bound.getObject();
    if (object.kind == SchemaObjectKind::View)
    {
        if (!create.isView() || !create.columns_list || !create.columns_list->columns)
            introspectionError("mapped View CREATE metadata has no normalized output declarations");
        NamesAndTypesList outputs;
        UInt64 ordinal = 0;
        for (const auto & child : create.columns_list->columns->children)
        {
            const auto * declaration = child ? child->as<ASTColumnDeclaration>() : nullptr;
            if (!declaration || declaration->name.empty() || !declaration->getType())
                introspectionError("mapped View CREATE metadata contains a malformed output declaration");
            auto physical_type = DataTypeFactory::instance().get(declaration->getType());
            outputs.emplace_back(declaration->name, physical_type);
            result.endpoints.push_back({
                .key = {
                    .section = PersistedTypePathSection::ViewExpression,
                    .site = PersistedTypeOccurrenceSite::Declaration,
                    .object_ordinal = ordinal++,
                },
                .runtime_owner_key = declaration->name,
                .physical_type = std::move(physical_type),
            });
        }
        if (outputs != metadata.getColumns().getAllPhysical())
            introspectionError("mapped View CREATE outputs differ from its runtime physical metadata");
        try
        {
            result.view_auxiliary = collectViewAuxiliaryPhysicalTypeBindings(create);
            for (const auto & endpoint : result.view_auxiliary)
            {
                result.endpoints.push_back({
                    .key = {
                        .section = PersistedTypePathSection::ViewExpression,
                        .site = endpoint.site,
                        .object_ordinal = endpoint.object_ordinal,
                    },
                    .runtime_owner_key = endpoint.runtime_owner_key,
                    .physical_type = endpoint.physical_type,
                });
            }
            result.physical_schema_fingerprint = bound.getFormatVersion() == persisted_type_references_format_version_v2
                ? computeViewMixedPhysicalSchemaFingerprint(outputs, result.view_auxiliary)
                : computeViewOutputPhysicalSchemaFingerprint(outputs);
        }
        catch (const StoredObjectTypeBindingPreparationError &)
        {
            introspectionError("mapped View auxiliary endpoint metadata is not canonical");
        }
        catch (const ViewOutputTypeBindingError &)
        {
            introspectionError("mapped View physical endpoint fingerprint is invalid");
        }
    }
    else if (object.kind == SchemaObjectKind::Dictionary)
    {
        if (!create.is_dictionary || !create.dictionary_attributes_list)
            introspectionError("mapped Dictionary CREATE metadata has no attribute declarations");
        NamesAndTypesList attributes;
        UInt64 ordinal = 0;
        for (const auto & child : create.dictionary_attributes_list->children)
        {
            const auto * declaration = child ? child->as<ASTDictionaryAttributeDeclaration>() : nullptr;
            if (!declaration || declaration->name.empty() || !declaration->type)
                introspectionError("mapped Dictionary CREATE metadata contains a malformed attribute declaration");
            auto physical_type = DataTypeFactory::instance().get(declaration->type);
            attributes.emplace_back(declaration->name, physical_type);
            result.endpoints.push_back({
                .key = {
                    .section = PersistedTypePathSection::DictionaryAttribute,
                    .site = PersistedTypeOccurrenceSite::Declaration,
                    .object_ordinal = ordinal++,
                },
                .runtime_owner_key = declaration->name,
                .physical_type = std::move(physical_type),
            });
        }
        if (attributes != metadata.getColumns().getAllPhysical())
            introspectionError("mapped Dictionary CREATE attributes differ from its runtime physical metadata");
        try
        {
            result.physical_schema_fingerprint = computeDictionaryAttributePhysicalSchemaFingerprint(attributes);
        }
        catch (const DictionaryAttributeTypeBindingError &)
        {
            introspectionError("mapped Dictionary physical attribute fingerprint is invalid");
        }
    }
    else
        introspectionError("kind-aware stored-object rendering received a non-View/Dictionary binding");

    return result;
}

std::vector<CurrentStoredObjectEndpoint> projectCurrentStoredObjectEndpoints(
    const StorageID & object_id, const StorageInMemoryMetadata & metadata, IDatabase & database, const ASTCreateQuery & create)
{
    metadata.validateBoundUDTReferences();
    const auto & bound = metadata.getBoundUDTReferences();
    const auto & retained_expectation = metadata.getBoundUDTExpectation();
    if (!bound || !retained_expectation)
        introspectionError("mapped stored-object metadata has no complete bound package");
    const auto & object = bound->getObject();
    if (!object_id.hasUUID() || object_id.database_name != database.getDatabaseName() || object_id.table_name.empty()
        || object_id.uuid != object.object_uuid || database.getUUID() == UUIDHelpers::Nil || object.database_uuid != database.getUUID()
        || !bound->getObjectSchemaRevision() || (object.kind != SchemaObjectKind::View && object.kind != SchemaObjectKind::Dictionary))
        introspectionError("stored-object, database, and bound identities differ");

    auto inventory = collectStoredObjectPhysicalEndpoints(create, metadata, *bound);
    if (inventory.physical_schema_fingerprint != bound->getPhysicalSchemaFingerprint()
        || inventory.physical_schema_fingerprint != retained_expectation->physical_schema_fingerprint)
        introspectionError("stored-object physical endpoint fingerprint differs from its bound package");

    std::map<StoredObjectEndpointKey, size_t> endpoint_by_key;
    for (size_t index = 0; index < inventory.endpoints.size(); ++index)
    {
        if (!inventory.endpoints[index].physical_type || !endpoint_by_key.emplace(inventory.endpoints[index].key, index).second)
            introspectionError("stored-object physical endpoint inventory is invalid or duplicated");
    }

    const auto descriptors = bound->getDescriptors();
    if (descriptors.empty() || bound->getUses().empty())
        introspectionError("mapped stored-object metadata has no descriptor occurrences");
    std::set<UInt32> selected_descriptor_indices;
    IntrospectionWorkBudget budget(maximum_occurrence_presentation_work);
    for (const auto & use : bound->getUses())
    {
        const auto & path = use.getPath();
        budget.charge(1 + path.type_child_ordinals.size());
        const StoredObjectEndpointKey key{path.section, path.site, path.object_ordinal};
        const auto endpoint = endpoint_by_key.find(key);
        if (endpoint == endpoint_by_key.end() || use.getRuntimeOwnerKey() != inventory.endpoints[endpoint->second].runtime_owner_key
            || use.getDescriptorIndex() >= descriptors.size() || !descriptors[use.getDescriptorIndex()] || !use.getPhysicalType())
            introspectionError("a bound stored-object occurrence has no exact physical endpoint");

        auto & current = inventory.endpoints[endpoint->second];
        auto & next_ordinal = current.next_occurrence_ordinals[path.type_child_ordinals];
        if (path.occurrence_ordinal != next_ordinal)
            introspectionError("stored-object occurrence ordinals are not contiguous at one endpoint path");
        ++next_ordinal;
        LogicalTypePath normalized_path;
        normalized_path.reserve(path.type_child_ordinals.size());
        for (const UInt64 child_ordinal : path.type_child_ordinals)
        {
            if (!std::in_range<UInt32>(child_ordinal))
                introspectionError("a stored-object type-child ordinal exceeds UInt32");
            normalized_path.push_back(static_cast<UInt32>(child_ordinal));
        }
        auto & sequence = current.logical_occurrences[std::move(normalized_path)];
        if (path.occurrence_ordinal != sequence.size())
            introspectionError("stored-object occurrence ordering differs from its endpoint sequence");
        sequence.push_back(use.getDescriptorIndex());
        selected_descriptor_indices.insert(use.getDescriptorIndex());
    }

    const auto & lifecycle = database.getUDTLifecycleAdapter();
    if (lifecycle.getDatabaseUUID() != database.getUUID())
        introspectionError("the stored-object lifecycle adapter belongs to another database");
    auto snapshot = lifecycle.acquireSnapshot();
    if (!snapshot || snapshot->getDatabaseUUID() != database.getUUID())
        introspectionError("the stored-object lifecycle snapshot belongs to another database");
    const auto * current_expectation = snapshot->findSidecarExpectation(object);
    if (!current_expectation || *current_expectation != *retained_expectation)
        introspectionError("stored-object metadata and its authority snapshot have different sidecar expectations");

    std::vector<Definition::Ptr> current_definitions(descriptors.size());
    for (const UInt32 descriptor_index : selected_descriptor_indices)
    {
        const auto & descriptor = descriptors[descriptor_index];
        const auto & retained = descriptor->getDefinition();
        if (!retained || descriptor->getPersistedDescriptor().getDefinitionIdentity() != retained->getIdentity()
            || descriptor->getPersistedDescriptor().getDefinitionHash() != retained->getDefinitionHash())
            introspectionError("a stored-object descriptor differs from its retained definition");
        auto current = snapshot->findCheckedDefinitionByIdentity(retained->getIdentity());
        validateCurrentDefinition(retained, current, database.getUUID());
        current_definitions[descriptor_index] = std::move(current);
    }
    const std::vector<UInt32> selected_indices(selected_descriptor_indices.begin(), selected_descriptor_indices.end());
    const auto replays = prepareCheckedSpecializationReplays(database, *snapshot, descriptors, std::span<const UInt32>(selected_indices));
    const IntrospectionReplayContext replay_context{
        .database_name = object_id.database_name,
        .descriptors = descriptors,
        .current_definitions = current_definitions,
        .replays = replays,
        .budget = budget,
    };
    for (auto & endpoint : inventory.endpoints)
    {
        endpoint.declared_type = dataTypeToAST(endpoint.physical_type);
        endpoint.has_logical_references = !endpoint.logical_occurrences.empty();
        if (endpoint.has_logical_references)
            endpoint.declared_type
                = renderLogicalSurface(std::move(endpoint.declared_type), endpoint.logical_occurrences, replay_context, 0);
    }
    return std::move(inventory.endpoints);
}

std::vector<CurrentDeclaredTableColumnType> projectCurrentDeclaredTableColumnTypesImpl(
    const StorageID & table_id,
    const StorageInMemoryMetadata & metadata,
    IDatabase & database,
    const std::span<const UInt8> * included_physical_columns,
    bool validate_complete_metadata,
    bool include_occurrence_presentations)
{
    if (validate_complete_metadata)
        metadata.validateBoundUDTReferences();

    const NamesAndTypesList physical_columns = metadata.getColumns().getAllPhysical();
    if (included_physical_columns && included_physical_columns->size() != physical_columns.size())
        introspectionError("a physical-column visibility mask has the wrong size");
    const auto physical_only_projection = [&]
    {
        std::vector<CurrentDeclaredTableColumnType> result;
        result.reserve(physical_columns.size());
        for (const auto & column : physical_columns)
        {
            result.push_back({
                .column_name = column.name,
                .physical_type = column.type,
                .declared_type = {},
                .has_logical_references = false,
                .logical_occurrences = {},
            });
        }
        return result;
    };

    const auto & bound = metadata.getBoundUDTReferences();
    if (!bound)
        return {};

    const auto & retained_expectation = metadata.getBoundUDTExpectation();
    if (!retained_expectation)
    {
        if (!validate_complete_metadata)
            return physical_only_projection();
        introspectionError("mapped metadata has no retained sidecar expectation");
    }

    const auto & object = bound->getObject();
    if (table_id.database_name.empty() || table_id.table_name.empty() || !table_id.hasUUID()
        || database.getUUID() == UUIDHelpers::Nil || table_id.database_name != database.getDatabaseName()
        || object.kind != SchemaObjectKind::Table || object.database_uuid != database.getUUID()
        || object.object_uuid != table_id.uuid || !bound->getObjectSchemaRevision())
    {
        if (!validate_complete_metadata)
            return physical_only_projection();
        introspectionError("the storage, database, and bound object identities differ");
    }

    const auto descriptors = bound->getDescriptors();
    if (descriptors.empty() || bound->getUses().empty())
    {
        if (!validate_complete_metadata)
            return physical_only_projection();
        introspectionError("mapped metadata has no descriptor occurrences");
    }
    if (physical_columns.empty())
    {
        if (!validate_complete_metadata)
            return physical_only_projection();
        introspectionError("a mapped table has no physical columns");
    }

    std::vector<LogicalOccurrenceMap> logical_occurrences(physical_columns.size());
    std::vector<std::map<std::vector<UInt64>, UInt64>> next_occurrence_ordinals(physical_columns.size());
    std::set<UInt32> selected_descriptor_indices;
    IntrospectionWorkBudget introspection_budget(
        include_occurrence_presentations ? maximum_occurrence_presentation_work : maximum_root_introspection_work);
    if (validate_complete_metadata)
    {
        for (size_t descriptor_index = 0; descriptor_index < descriptors.size(); ++descriptor_index)
        {
            if (!std::in_range<UInt32>(descriptor_index))
                introspectionError("the descriptor dictionary exceeds the replay index domain");
            selected_descriptor_indices.insert(static_cast<UInt32>(descriptor_index));
        }
    }
    for (const auto & use : bound->getUses())
    {
        const auto & path = use.getPath();
        introspection_budget.charge(1 + path.type_child_ordinals.size());
        if (path.section != PersistedTypePathSection::ColumnType || path.site != PersistedTypeOccurrenceSite::Declaration
            || path.object_ordinal >= physical_columns.size())
        {
            if (!validate_complete_metadata
                && (path.object_ordinal >= physical_columns.size()
                    || !(*included_physical_columns)[static_cast<size_t>(path.object_ordinal)]))
            {
                continue;
            }
            introspectionError("a bound occurrence has an invalid physical-column ordinal");
        }

        const size_t column = static_cast<size_t>(path.object_ordinal);
        if (included_physical_columns && !(*included_physical_columns)[column])
            continue;
        if (use.getDescriptorIndex() >= descriptors.size() || !descriptors[use.getDescriptorIndex()] || !use.getPhysicalType())
            introspectionError("a bound occurrence has an invalid descriptor or physical type");
        selected_descriptor_indices.insert(use.getDescriptorIndex());

        auto & next_ordinal = next_occurrence_ordinals[column][path.type_child_ordinals];
        if (path.occurrence_ordinal != next_ordinal)
            introspectionError("bound occurrence ordinals are not contiguous at one physical path");
        ++next_ordinal;

        LogicalTypePath normalized_path;
        normalized_path.reserve(path.type_child_ordinals.size());
        for (const UInt64 child_ordinal : path.type_child_ordinals)
        {
            if (!std::in_range<UInt32>(child_ordinal))
                introspectionError("a bound physical child ordinal exceeds UInt32");
            normalized_path.push_back(static_cast<UInt32>(child_ordinal));
        }
        auto & sequence = logical_occurrences[column][std::move(normalized_path)];
        if (path.occurrence_ordinal != sequence.size())
            introspectionError("bound occurrence ordering differs from its endpoint sequence");
        sequence.push_back(use.getDescriptorIndex());
    }

    std::vector<Definition::Ptr> current_definitions(descriptors.size());
    std::vector<CheckedSpecializationReplay> replays(descriptors.size());
    if (!selected_descriptor_indices.empty())
    {
        const auto & lifecycle = database.getUDTLifecycleAdapter();
        if (lifecycle.getDatabaseUUID() != database.getUUID())
            introspectionError("the lifecycle adapter belongs to another database identity");
        std::unique_ptr<const ILifecycleSnapshot> snapshot;
        snapshot = lifecycle.acquireSnapshot();
        if (!snapshot || snapshot->getDatabaseUUID() != database.getUUID())
            introspectionError("the lifecycle snapshot belongs to another database identity");

        const auto * current_expectation = snapshot->findSidecarExpectation(object);
        if (!current_expectation || *current_expectation != *retained_expectation)
        {
            /// Selective system.columns must not turn hidden-column/global
            /// integrity state into an oracle. Full SHOW/DESCRIBE callers hold
            /// the backend's table-introspection lease, so a mismatch is a
            /// genuine invariant failure rather than a publication race.
            if (!validate_complete_metadata)
                return physical_only_projection();
            introspectionError("mapped metadata and its pinned authority snapshot have different sidecar expectations");
        }

        for (const UInt32 descriptor_index : selected_descriptor_indices)
        {
            if (descriptor_index >= descriptors.size() || !descriptors[descriptor_index]
                || !descriptors[descriptor_index]->getDefinition())
            {
                introspectionError("the bound descriptor dictionary contains an empty selected entry");
            }
            const auto & descriptor = descriptors[descriptor_index];
            const auto & retained = descriptor->getDefinition();
            const auto & persisted = descriptor->getPersistedDescriptor();
            if (persisted.getDefinitionIdentity() != retained->getIdentity()
                || persisted.getDefinitionHash() != retained->getDefinitionHash())
            {
                introspectionError("a bound descriptor differs from its retained definition handle");
            }
            auto current = snapshot->findCheckedDefinitionByIdentity(persisted.getDefinitionIdentity());
            validateCurrentDefinition(retained, current, database.getUUID());
            current_definitions[descriptor_index] = std::move(current);
        }

        if (validate_complete_metadata)
        {
            for (const auto & retained : bound->getDefinitionHandles())
            {
                if (!retained)
                    introspectionError("the bound definition-handle set contains an empty entry");
                auto current = snapshot->findCheckedDefinitionByIdentity(retained->getIdentity());
                validateCurrentDefinition(retained, current, database.getUUID());
            }
        }

        const std::vector<UInt32> selected_indices(selected_descriptor_indices.begin(), selected_descriptor_indices.end());
        replays = prepareCheckedSpecializationReplays(
            database, *snapshot, descriptors, std::span<const UInt32>(selected_indices));
    }

    const IntrospectionReplayContext replay_context{
        .database_name = table_id.database_name,
        .descriptors = descriptors,
        .current_definitions = current_definitions,
        .replays = replays,
        .budget = introspection_budget,
    };
    std::vector<CurrentDeclaredTableColumnType> result;
    result.reserve(physical_columns.size());
    size_t column_index = 0;
    for (const auto & column : physical_columns)
    {
        if (!column.type)
            introspectionError("a mapped physical column has no data type");
        ASTPtr declared_type = dataTypeToAST(column.type);
        const bool has_logical_references = !logical_occurrences[column_index].empty();
        if (has_logical_references)
        {
            declared_type = renderLogicalSurface(
                std::move(declared_type), logical_occurrences[column_index], replay_context, 0);
        }

        std::vector<CurrentDeclaredTableTypeOccurrence> occurrence_presentations;
        if (include_occurrence_presentations && has_logical_references)
        {
            for (const auto & [root_path, sequence] : logical_occurrences[column_index])
            {
                replay_context.budget.charge(1 + root_path.size() + sequence.size());
                LogicalOccurrenceMap application_occurrences;
                for (auto occurrence = logical_occurrences[column_index].lower_bound(root_path);
                     occurrence != logical_occurrences[column_index].end()
                     && isPathPrefix(root_path, occurrence->first);
                     ++occurrence)
                {
                    const auto & [path, path_sequence] = *occurrence;
                    replay_context.budget.charge(
                        1 + path.size() - root_path.size() + path_sequence.size());
                    application_occurrences.emplace(
                        pathSuffix(path, root_path.size()), path_sequence);
                }
                auto application_root = application_occurrences.find({});
                if (application_root == application_occurrences.end())
                    introspectionError("an occurrence presentation has no application root");

                for (size_t occurrence_ordinal = 0; occurrence_ordinal < sequence.size(); ++occurrence_ordinal)
                {
                    replay_context.budget.charge(1 + sequence.size() - occurrence_ordinal);
                    application_root->second.assign(
                        sequence.begin() + occurrence_ordinal, sequence.end());

                    ASTPtr occurrence_type = renderLogicalApplication(
                        sequence[occurrence_ordinal], application_occurrences, replay_context, 0);
                    std::vector<UInt64> persisted_path;
                    persisted_path.reserve(root_path.size());
                    for (const UInt32 child_ordinal : root_path)
                        persisted_path.push_back(child_ordinal);
                    occurrence_presentations.push_back({
                        .type_child_ordinals = std::move(persisted_path),
                        .occurrence_ordinal = static_cast<UInt64>(occurrence_ordinal),
                        .descriptor_index = sequence[occurrence_ordinal],
                        .declared_type = std::move(occurrence_type),
                    });
                }
            }
        }
        result.push_back({
            .column_name = column.name,
            .physical_type = column.type,
            .declared_type = std::move(declared_type),
            .has_logical_references = has_logical_references,
            .logical_occurrences = std::move(occurrence_presentations),
        });
        ++column_index;
    }
    return result;
}

}

std::vector<CurrentDeclaredTableColumnType> projectCurrentDeclaredTableColumnTypes(
    const StorageID & table_id,
    const StorageInMemoryMetadata & metadata,
    IDatabase & database)
{
    return projectCurrentDeclaredTableColumnTypesImpl(
        table_id, metadata, database, nullptr, true, false);
}

std::vector<CurrentDeclaredTableColumnType> projectCurrentDeclaredTableColumnTypes(
    const StorageID & table_id,
    const StorageInMemoryMetadata & metadata,
    IDatabase & database,
    std::span<const UInt8> included_physical_columns,
    bool include_occurrence_presentations)
{
    return projectCurrentDeclaredTableColumnTypesImpl(
        table_id,
        metadata,
        database,
        std::addressof(included_physical_columns),
        false,
        include_occurrence_presentations);
}

void applyCurrentDeclaredTableColumnTypes(
    ASTCreateQuery & create,
    const StorageID & table_id,
    const std::vector<CurrentDeclaredTableColumnType> & columns)
{
    if (columns.empty())
        return;
    if (!table_id.hasUUID() || create.uuid != table_id.uuid || create.getDatabase() != table_id.database_name
        || create.getTable() != table_id.table_name)
    {
        introspectionError("a fetched CREATE TABLE AST belongs to another storage identity");
    }
    if (!create.columns_list || !create.columns_list->columns)
        introspectionError("a mapped CREATE TABLE AST has no explicit columns");

    size_t physical_ordinal = 0;
    for (const auto & child : create.columns_list->columns->children)
    {
        auto * declaration = child ? child->as<ASTColumnDeclaration>() : nullptr;
        if (!declaration || declaration->name.empty() || !declaration->getType())
            introspectionError("a mapped CREATE TABLE AST contains a malformed column declaration");
        if (declaration->default_specifier == ColumnDefaultSpecifier::Alias
            || declaration->default_specifier == ColumnDefaultSpecifier::Ephemeral)
        {
            continue;
        }
        if (physical_ordinal >= columns.size())
            introspectionError("a mapped CREATE TABLE AST has extra physical columns");

        const auto & projected = columns[physical_ordinal++];
        if (declaration->name != projected.column_name || !projected.physical_type || !projected.declared_type)
            introspectionError("a mapped CREATE TABLE AST differs from its bound column order");
        DataTypePtr ast_physical_type = DataTypeFactory::instance().get(declaration->getType());
        if (!ast_physical_type || !ast_physical_type->equals(*projected.physical_type)
            || ast_physical_type->getName() != projected.physical_type->getName())
        {
            introspectionError("a mapped CREATE TABLE AST differs from its bound physical schema");
        }
        if (projected.has_logical_references)
            declaration->setType(projected.declared_type->clone());
    }
    if (physical_ordinal != columns.size())
        introspectionError("a mapped CREATE TABLE AST omits bound physical columns");
}

void applyCurrentDeclaredStoredObjectTypes(
    ASTCreateQuery & create, const StorageID & object_id, const StorageInMemoryMetadata & metadata, IDatabase & database)
{
    if (!object_id.hasUUID() || create.uuid != object_id.uuid || create.getDatabase() != object_id.database_name
        || create.getTable() != object_id.table_name)
        introspectionError("a fetched stored-object CREATE AST belongs to another identity");

    auto endpoints = projectCurrentStoredObjectEndpoints(object_id, metadata, database, create);
    std::map<StoredObjectEndpointKey, CurrentStoredObjectEndpoint *> endpoint_by_key;
    for (auto & endpoint : endpoints)
        if (!endpoint_by_key.emplace(endpoint.key, std::addressof(endpoint)).second)
            introspectionError("a projected stored-object endpoint is duplicated");

    size_t declaration_count = 0;
    if (create.isView())
    {
        UInt64 ordinal = 0;
        for (auto & child : create.columns_list->columns->children)
        {
            auto * declaration = child ? child->as<ASTColumnDeclaration>() : nullptr;
            const StoredObjectEndpointKey key{
                PersistedTypePathSection::ViewExpression,
                PersistedTypeOccurrenceSite::Declaration,
                ordinal++,
            };
            const auto found = endpoint_by_key.find(key);
            if (!declaration || found == endpoint_by_key.end() || declaration->name != found->second->runtime_owner_key
                || !found->second->physical_type || !found->second->declared_type)
                introspectionError("a mapped View output declaration differs from its bound endpoint");
            auto ast_physical_type = DataTypeFactory::instance().get(declaration->getType());
            if (!ast_physical_type->equals(*found->second->physical_type)
                || ast_physical_type->getName() != found->second->physical_type->getName())
                introspectionError("a mapped View output declaration differs from its physical endpoint");
            if (found->second->has_logical_references)
                declaration->setType(found->second->declared_type->clone());
            ++declaration_count;
        }

        std::vector<ViewAuxiliaryTypePresentation> presentations;
        for (const auto & endpoint : endpoints)
        {
            if (endpoint.key.site == PersistedTypeOccurrenceSite::Declaration)
                continue;
            presentations.push_back({
                .site = endpoint.key.site,
                .object_ordinal = endpoint.key.object_ordinal,
                .runtime_owner_key = endpoint.runtime_owner_key,
                .physical_type = endpoint.physical_type,
                .declared_type = endpoint.declared_type,
                .has_logical_references = endpoint.has_logical_references,
            });
        }
        try
        {
            applyViewAuxiliaryTypePresentations(create, presentations);
        }
        catch (const StoredObjectTypeBindingPreparationError &)
        {
            introspectionError("mapped View auxiliary endpoints changed before current-name rendering");
        }
    }
    else if (create.is_dictionary)
    {
        UInt64 ordinal = 0;
        for (auto & child : create.dictionary_attributes_list->children)
        {
            auto * declaration = child ? child->as<ASTDictionaryAttributeDeclaration>() : nullptr;
            const StoredObjectEndpointKey key{
                PersistedTypePathSection::DictionaryAttribute,
                PersistedTypeOccurrenceSite::Declaration,
                ordinal++,
            };
            const auto found = endpoint_by_key.find(key);
            if (!declaration || found == endpoint_by_key.end() || declaration->name != found->second->runtime_owner_key
                || !declaration->type || !found->second->physical_type || !found->second->declared_type)
                introspectionError("a mapped Dictionary attribute differs from its bound endpoint");
            auto ast_physical_type = DataTypeFactory::instance().get(declaration->type);
            if (!ast_physical_type->equals(*found->second->physical_type)
                || ast_physical_type->getName() != found->second->physical_type->getName())
                introspectionError("a mapped Dictionary attribute differs from its physical endpoint");
            if (found->second->has_logical_references)
            {
                const auto type_child = std::find_if(
                    declaration->children.begin(),
                    declaration->children.end(),
                    [&](const ASTPtr & candidate) { return candidate.get() == declaration->type.get(); });
                if (type_child == declaration->children.end())
                    introspectionError("a mapped Dictionary attribute does not own its type AST");
                declaration->type = found->second->declared_type->clone();
                *type_child = declaration->type;
            }
            ++declaration_count;
        }
    }
    else
        introspectionError("kind-aware stored-object rendering received a non-View/Dictionary CREATE AST");

    const size_t expected_declarations = std::count_if(
        endpoints.begin(),
        endpoints.end(),
        [](const auto & endpoint) { return endpoint.key.site == PersistedTypeOccurrenceSite::Declaration; });
    if (declaration_count != expected_declarations)
        introspectionError("stored-object CREATE omits bound declaration endpoints");
}
}
