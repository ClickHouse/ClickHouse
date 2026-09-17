#include <DataTypes/DataTypeFactory.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/UDT/IAuthorityAdapter.h>
#include <DataTypes/UDT/TemplateChecker.h>
#include <DataTypes/UDT/TemplateSpecializer.h>
#include <DataTypes/UDT/TypeResolver.h>
#include <DataTypes/UDT/Catalog.h>

#include <Common/Exception.h>

#include <Parsers/ASTDataType.h>
#include <Parsers/ASTExpressionList.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <exception>
#include <memory>
#include <numeric>
#include <span>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, std::size_t size);

namespace DB::ErrorCodes
{
extern const int BAD_ARGUMENTS;
extern const int LOGICAL_ERROR;
}

namespace DB::UDT
{
namespace
{

constexpr std::size_t maximum_input_size = 64;
constexpr UInt64 catalog_generation = 7;

const std::array<std::string_view, 12> scalar_families{
    "UInt8",
    "UInt16",
    "UInt32",
    "UInt64",
    "Int8",
    "Int64",
    "Float32",
    "Float64",
    "String",
    "Date",
    "UUID",
    "IPv4",
};

enum class InputMode : UInt8
{
    Valid,
    CheckerInvalid,
    ResolverInvalid,
};

enum class DefinitionTopology : UInt8
{
    Scalar,
    Array,
    Tuple,
    Dependency,
};

class InputCursor final
{
public:
    explicit InputCursor(std::span<const uint8_t> input_)
        : input(input_)
    {
    }

    UInt8 next() noexcept
    {
        if (position < input.size())
            return input[position++];

        const UInt8 fallback = static_cast<UInt8>(0x9dU + 17U * position);
        ++position;
        return fallback;
    }

private:
    std::span<const uint8_t> input;
    std::size_t position = 0;
};

[[noreturn]] void propertyViolation(std::string_view message) noexcept
{
    std::fputs("UDT property violation: ", stderr);
    std::fwrite(message.data(), 1, message.size(), stderr);
    std::fputc('\n', stderr);
    std::abort();
}

void requireProperty(bool condition, std::string_view message)
{
    if (!condition)
        propertyViolation(message);
}

UUID testUUID(UInt64 high, UInt64 low)
{
    UUID result{};
    UUIDHelpers::getHighBytes(result) = high;
    UUIDHelpers::getLowBytes(result) = low;
    return result;
}

const UUID database_uuid = testUUID(0x7564742d66757a7aULL, 0x2d617574686f7231ULL);

DefinitionIdentity identity(std::size_t ordinal)
{
    return {
        .database_uuid = database_uuid,
        .type_uuid = testUUID(0x7564742d66757a7aULL, static_cast<UInt64>(ordinal) + 1),
        .revision = 1,
    };
}

String localName(std::size_t ordinal)
{
    return "FuzzType" + std::to_string(ordinal);
}

TemplateNode builtIn(String family)
{
    TemplateNode node;
    node.kind = TemplateNodeKind::BuiltIn;
    node.atom = std::move(family);
    return node;
}

String scalarFamily(InputCursor & cursor)
{
    return String(scalar_families[cursor.next() % scalar_families.size()]);
}

struct GeneratedDefinition
{
    DefinitionInput input;
    String expected_physical_name;
};

GeneratedDefinition generateDefinition(InputCursor & cursor, std::size_t ordinal, const std::vector<GeneratedDefinition> & preceding)
{
    GeneratedDefinition generated;
    generated.input.identity = identity(ordinal);
    generated.input.normalized_local_name = localName(ordinal);
    generated.input.normalized_name = "udt_fuzz." + generated.input.normalized_local_name;

    DefinitionTopology topology = static_cast<DefinitionTopology>(cursor.next() % 4);
    if (topology == DefinitionTopology::Dependency && ordinal == 0)
        topology = DefinitionTopology::Scalar;

    switch (topology)
    {
        case DefinitionTopology::Scalar: {
            generated.expected_physical_name = scalarFamily(cursor);
            generated.input.nodes.push_back(builtIn(generated.expected_physical_name));
            break;
        }
        case DefinitionTopology::Array: {
            const String nested = scalarFamily(cursor);
            auto root = builtIn("Array");
            root.children.push_back({.reference = 1, .label = {}});
            generated.input.nodes = {std::move(root), builtIn(nested)};
            generated.expected_physical_name = "Array(" + nested + ")";
            break;
        }
        case DefinitionTopology::Tuple: {
            const String first = scalarFamily(cursor);
            const String second = scalarFamily(cursor);
            auto root = builtIn("Tuple");
            root.children = {{.reference = 1, .label = {}}, {.reference = 2, .label = {}}};
            generated.input.nodes = {std::move(root), builtIn(first), builtIn(second)};
            generated.expected_physical_name = "Tuple(" + first + ", " + second + ")";
            break;
        }
        case DefinitionTopology::Dependency: {
            const std::size_t target = cursor.next() % ordinal;
            generated.input.dependencies.push_back(
                {.type_uuid = preceding[target].input.identity.type_uuid,
                 .revision = preceding[target].input.identity.revision,
                 .target_definition_hash = {}});
            TemplateNode call;
            call.kind = TemplateNodeKind::DefinitionCall;
            call.dependency_ordinal = 0;
            generated.input.nodes.push_back(std::move(call));
            generated.expected_physical_name = preceding[target].expected_physical_name;
            break;
        }
    }

    return generated;
}

std::vector<GeneratedDefinition> generateDefinitions(InputCursor & cursor)
{
    const std::size_t count = 1 + cursor.next() % 4;
    std::vector<GeneratedDefinition> generated;
    generated.reserve(count);
    for (std::size_t ordinal = 0; ordinal < count; ++ordinal)
        generated.push_back(generateDefinition(cursor, ordinal, generated));
    return generated;
}

TemplateCheckerLimits checkerLimits()
{
    TemplateCheckerLimits limits;
    limits.maximum_definitions = 4;
    limits.maximum_formals = 1;
    limits.maximum_definition_input_bytes = 4ULL << 10;
    limits.maximum_catalog_input_bytes = 16ULL << 10;
    limits.maximum_template_nodes = 4;
    limits.maximum_logical_node_occurrences = 16;
    limits.maximum_template_edges = 8;
    limits.maximum_template_depth = 4;
    limits.maximum_catalog_nodes = 16;
    limits.maximum_catalog_edges = 32;
    limits.maximum_direct_dependencies = 1;
    limits.maximum_transitive_dependencies = 4;
    limits.maximum_checker_work = 4ULL << 10;
    limits.maximum_catalog_checker_work = 16ULL << 10;
    limits.maximum_canonical_definition_bytes = 8ULL << 10;
    limits.maximum_canonical_catalog_bytes = 32ULL << 10;
    limits.maximum_formal_name_bytes = 64;
    limits.maximum_ir_atom_bytes = 256;
    limits.maximum_ir_literal_bytes = 256;
    limits.maximum_ir_identifier_bytes = 64;
    limits.maximum_ir_enum_entries = 4;
    limits.maximum_scratch_bytes = 1ULL << 20;
    return limits;
}

TypeCatalogBuildLimits catalogBuildLimits()
{
    TypeCatalogBuildLimits limits;
    limits.shard_count = 4;
    limits.maximum_definitions = 4;
    limits.maximum_normalized_name_length = 64;
    limits.maximum_normalized_name_bytes = 256;
    limits.maximum_root_accounted_bytes = 1ULL << 20;
    limits.maximum_identity_shard_entries = 4;
    limits.maximum_name_shard_entries = 4;
    limits.maximum_shard_accounted_bytes = 256ULL << 10;
    return limits;
}

TypeCatalogPublicationLimits catalogPublicationLimits()
{
    TypeCatalogPublicationLimits limits;
    limits.hazard_slot_count = 4;
    limits.maximum_retired_root_count = 1;
    limits.maximum_retired_root_bytes = 1ULL << 20;
    return limits;
}

TypeAuthorityCapabilities authorityCapabilities()
{
    TypeAuthorityCapabilities capabilities;
    capabilities.mask = typeAuthorityCapabilityBit(TypeAuthorityCapability::TransientResolution)
        | typeAuthorityCapabilityBit(TypeAuthorityCapability::Limits) | typeAuthorityCapabilityBit(TypeAuthorityCapability::Templates);
    capabilities.limits = {
        .maximum_definitions = 4,
        .maximum_definition_bytes = 64ULL << 10,
        .maximum_template_nodes = 4,
        .maximum_direct_dependencies = 1,
        .maximum_transitive_dependencies = 4,
        .maximum_checker_work = 4ULL << 10,
    };
    return capabilities;
}

TemplateSpecializerLimits specializerLimits()
{
    TemplateSpecializerLimits limits;
    limits.maximum_distinct_specializations = 4;
    limits.maximum_definition_handles = 4;
    limits.maximum_definition_lookups = 16;
    limits.maximum_specialization_depth = 4;
    limits.maximum_canonical_argument_bytes = 256;
    limits.maximum_canonical_argument_item_bytes = 256;
    limits.maximum_memo_key_bytes = 4ULL << 10;
    limits.maximum_template_node_occurrences = 64;
    limits.maximum_constructed_ast_nodes = 64;
    limits.maximum_constructed_ast_edges = 64;
    limits.maximum_ast_depth = 8;
    limits.maximum_field_depth = 8;
    limits.maximum_owned_ast_string_bytes = 4ULL << 10;
    limits.maximum_enum_entries = 4;
    limits.maximum_retained_occurrences = 64;
    limits.maximum_retained_path_components = 128;
    limits.maximum_emitted_ast_node_occurrences = 64;
    limits.maximum_emitted_ast_edges = 64;
    limits.maximum_emitted_occurrences = 64;
    limits.maximum_emitted_path_components = 128;
    limits.maximum_work = 4ULL << 10;
    return limits;
}

TypeDescriptorLimits descriptorLimits()
{
    TypeDescriptorLimits limits;
    limits.maximum_canonical_arguments_bytes = 256;
    limits.maximum_canonical_physical_type_bytes = 4ULL << 10;
    limits.maximum_qualified_name_bytes = 64;
    limits.maximum_nodes = 64;
    limits.maximum_edges = 64;
    limits.maximum_path_depth = 8;
    limits.maximum_descriptors = 16;
    limits.maximum_occurrences = 32;
    return limits;
}

TypeResolverLimits resolverLimits()
{
    TypeResolverLimits limits;
    limits.maximum_input_references = 4;
    limits.maximum_argument_lineage_entries = 4;
    limits.maximum_argument_validation_factory_calls = 4;
    limits.maximum_argument_validation_ast_nodes = 32;
    limits.maximum_argument_validation_ast_edges = 32;
    limits.maximum_argument_validation_ast_depth = 8;
    limits.maximum_argument_validation_syntax_bytes = 4ULL << 10;
    limits.maximum_argument_validation_binary_bytes = 4ULL << 10;
    limits.maximum_declaration_ast_nodes = 64;
    limits.maximum_declaration_ast_edges = 64;
    limits.maximum_declaration_ast_depth = 8;
    limits.maximum_declaration_ast_syntax_bytes = 4ULL << 10;
    limits.maximum_physical_ast_nodes = 64;
    limits.maximum_physical_ast_edges = 64;
    limits.maximum_physical_ast_depth = 8;
    limits.maximum_physical_ast_syntax_bytes = 4ULL << 10;
    limits.maximum_literal_field_nodes = 4;
    limits.maximum_literal_field_edges = 4;
    limits.maximum_literal_field_depth = 4;
    limits.maximum_path_components = 128;
    limits.maximum_logical_occurrences = 32;
    limits.maximum_variant_branch_factory_calls = 4;
    limits.specializer = specializerLimits();
    limits.descriptors = descriptorLimits();
    return limits;
}

CanonicalTypeArguments noArguments()
{
    return CanonicalTypeArguments::validate({}, {});
}

boost::intrusive_ptr<ASTDataType> marker(String name)
{
    auto result = make_intrusive<ASTDataType>();
    result->name = std::move(name);
    return result;
}

bool checkerStatisticsEqual(const TemplateCheckerStatistics & lhs, const TemplateCheckerStatistics & rhs) noexcept
{
    return lhs.accepted_input_bytes == rhs.accepted_input_bytes && lhs.maximum_definition_input_bytes == rhs.maximum_definition_input_bytes
        && lhs.checked_definitions == rhs.checked_definitions && lhs.graph_edges == rhs.graph_edges && lhs.charged_work == rhs.charged_work
        && lhs.canonical_bytes == rhs.canonical_bytes && lhs.scratch_peak_bytes == rhs.scratch_peak_bytes;
}

TemplateCheckerStatistics checkerStatisticsSentinel()
{
    return {
        .accepted_input_bytes = 101,
        .maximum_definition_input_bytes = 102,
        .checked_definitions = 103,
        .graph_edges = 104,
        .charged_work = 105,
        .canonical_bytes = 106,
        .scratch_peak_bytes = 107,
    };
}

std::vector<DefinitionInput> takeInputs(std::vector<GeneratedDefinition> & generated)
{
    std::vector<DefinitionInput> inputs;
    inputs.reserve(generated.size());
    for (auto & definition : generated)
        inputs.push_back(std::move(definition.input));
    return inputs;
}

void runCheckerInvalid(std::vector<GeneratedDefinition> generated, InputCursor & cursor)
{
    const std::size_t invalid_ordinal = cursor.next() % generated.size();
    generated[invalid_ordinal].input.dependencies.clear();
    generated[invalid_ordinal].input.nodes = {builtIn("DefinitelyNotARegisteredDataType")};

    TemplateCheckerStatistics statistics = checkerStatisticsSentinel();
    const TemplateCheckerStatistics sentinel = statistics;
    try
    {
        static_cast<void>(TemplateChecker::checkAll(takeInputs(generated), checkerLimits(), &statistics));
    }
    catch (const Exception & error)
    {
        if (error.code() == ErrorCodes::LOGICAL_ERROR)
            propertyViolation("production checker reported LOGICAL_ERROR");
        requireProperty(error.code() == ErrorCodes::BAD_ARGUMENTS, "checker-invalid mode returned the wrong DB error");
        requireProperty(checkerStatisticsEqual(statistics, sentinel), "failed checker transaction published statistics");
        return;
    }
    propertyViolation("checker-invalid mode was accepted");
}

struct CheckedDefinitions
{
    std::vector<Definition::Ptr> definitions;
    std::vector<String> expected_physical_names;
};

CheckedDefinitions checkValidDefinitions(std::vector<GeneratedDefinition> generated)
{
    CheckedDefinitions checked;
    checked.expected_physical_names.reserve(generated.size());
    UInt64 expected_dependency_edges = 0;
    for (const auto & definition : generated)
    {
        checked.expected_physical_names.push_back(definition.expected_physical_name);
        expected_dependency_edges += definition.input.dependencies.size();
    }

    TemplateCheckerStatistics statistics;
    checked.definitions = TemplateChecker::checkAll(takeInputs(generated), checkerLimits(), &statistics);
    requireProperty(checked.definitions.size() == checked.expected_physical_names.size(), "checker changed the definition count");
    requireProperty(statistics.checked_definitions == checked.definitions.size(), "checker statistics lost definitions");
    requireProperty(statistics.graph_edges == expected_dependency_edges, "checker statistics lost dependency edges");
    requireProperty(statistics.accepted_input_bytes > 0, "checker accepted no input bytes");
    requireProperty(statistics.charged_work > 0, "checker charged no work");
    requireProperty(statistics.canonical_bytes > 0, "checker emitted no canonical bytes");

    for (std::size_t index = 0; index < checked.definitions.size(); ++index)
    {
        const auto & definition = checked.definitions[index];
        requireProperty(static_cast<bool>(definition), "checker returned a null definition");
        requireProperty(definition->getIdentity() == identity(index), "checker changed a definition identity");
        requireProperty(definition->getNormalizedLocalName() == localName(index), "checker changed a local name");
    }
    return checked;
}

void verifyCatalogIndexes(
    const TypeCatalogRoot & root, const std::vector<Definition::Ptr> & definitions, UInt64 expected_generation)
{
    requireProperty(root.getDatabaseUUID() == database_uuid, "catalog changed the database authority");
    requireProperty(root.getGeneration() == expected_generation, "catalog changed the generation");
    requireProperty(root.getDefinitionCount() == definitions.size(), "catalog changed the definition count");
    for (const auto & definition : definitions)
    {
        requireProperty(root.findByIdentity(definition->getIdentity()) == definition, "catalog identity index returned the wrong handle");
        requireProperty(
            root.findByName(definition->getNormalizedLocalName()) == definition, "catalog name index returned the wrong handle");
    }
    requireProperty(!root.findByName("MissingFuzzType"), "catalog name index found an absent definition");
}

void runResolverInvalid(const std::vector<Definition::Ptr> & definitions)
{
    auto authority = makeTransientAuthorityAdapter(database_uuid, authorityCapabilities(), definitions);
    ASTPtr absent = marker("udt_fuzz.Absent");
    ASTPtr declaration = makeASTDataType("Tuple", makeASTDataType("UInt8"), makeASTDataType("String"));
    const std::array references{
        DeclaredTypeReferenceInput{
            .reference_node = absent.get(),
            .definition_identity = definitions.front()->getIdentity(),
            .canonical_arguments = noArguments(),
            .type_argument_lineage = {},
        },
    };
    TypeResolverStatistics statistics;
    statistics.logical_occurrences = 701;
    statistics.physical_factory_calls = 702;
    statistics.specializer.charged_work = 703;
    const TypeResolverStatistics sentinel = statistics;

    try
    {
        static_cast<void>(TypeResolver::resolve(declaration, references, *authority, resolverLimits(), &statistics));
    }
    catch (const TypeResolverError & error)
    {
        requireProperty(error.code == TypeResolverError::Code::UnreachableReference, "resolver-invalid mode returned the wrong error");
        requireProperty(statistics == sentinel, "failed resolver transaction published statistics");
        return;
    }
    propertyViolation("resolver-invalid mode was accepted");
}

std::vector<std::size_t> specializationOrder(std::size_t count, InputCursor & cursor)
{
    std::vector<std::size_t> order(count);
    std::iota(order.begin(), order.end(), 0);
    if ((cursor.next() & 1U) != 0)
        std::reverse(order.begin(), order.end());
    return order;
}

void verifyExplicitSpecialization(
    const IAuthorityAdapter & authority,
    const std::vector<Definition::Ptr> & definitions,
    const std::vector<String> & expected_physical_names,
    InputCursor & cursor)
{
    auto attempt = TemplateSpecializer::Attempt::begin(authority, specializerLimits());
    std::vector<TemplateSpecializationID> ids(definitions.size(), invalid_template_specialization_id);
    for (const std::size_t index : specializationOrder(definitions.size(), cursor))
    {
        ids[index] = attempt.specialize(definitions[index]->getIdentity(), noArguments());
        const ASTPtr & physical_ast = attempt.getCanonicalPhysicalAST(ids[index]);
        requireProperty(static_cast<bool>(physical_ast), "specializer returned a null physical AST");
        const auto physical_type = DataTypeFactory::instance().get(physical_ast);
        requireProperty(physical_type->getName() == expected_physical_names[index], "specializer changed the expected physical type");
    }

    const auto finished = attempt.finish();
    requireProperty(finished.specializations.size() == definitions.size(), "specializer changed the distinct specialization count");
    requireProperty(finished.definition_handles.size() == definitions.size(), "specializer retained the wrong definition count");
    requireProperty(finished.statistics.resolution_sessions == 1, "specializer opened the wrong number of authority sessions");
    requireProperty(
        finished.statistics.distinct_specializations == definitions.size(), "specializer statistics lost distinct specializations");
    for (const TemplateSpecializationID id : ids)
        requireProperty(id < finished.specializations.size(), "specializer returned an out-of-range ID");
}

struct Declaration
{
    ASTPtr root;
    std::vector<ASTPtr> markers;
    String expected_physical_name;
};

Declaration
makeDeclaration(const std::vector<Definition::Ptr> & definitions, const std::vector<String> & expected_physical_names)
{
    Declaration declaration;
    declaration.markers.reserve(definitions.size());
    for (const auto & definition : definitions)
        declaration.markers.push_back(marker(definition->getNormalizedName()));

    if (declaration.markers.size() == 1)
    {
        declaration.root = declaration.markers.front();
        declaration.expected_physical_name = expected_physical_names.front();
        return declaration;
    }

    auto arguments = make_intrusive<ASTExpressionList>();
    arguments->children.assign(declaration.markers.begin(), declaration.markers.end());
    auto tuple = make_intrusive<ASTDataType>();
    tuple->name = "Tuple";
    tuple->children.push_back(arguments);
    declaration.root = tuple;

    declaration.expected_physical_name = "Tuple(";
    for (std::size_t index = 0; index < expected_physical_names.size(); ++index)
    {
        if (index != 0)
            declaration.expected_physical_name += ", ";
        declaration.expected_physical_name += expected_physical_names[index];
    }
    declaration.expected_physical_name += ")";
    return declaration;
}

std::vector<DeclaredTypeReferenceInput>
makeReferences(const Declaration & declaration, const std::vector<Definition::Ptr> & definitions, InputCursor & cursor)
{
    std::vector<DeclaredTypeReferenceInput> references;
    references.reserve(definitions.size());
    for (std::size_t index = 0; index < definitions.size(); ++index)
    {
        references.push_back(
            {.reference_node = declaration.markers[index].get(),
             .definition_identity = definitions[index]->getIdentity(),
             .canonical_arguments = noArguments(),
             .type_argument_lineage = {}});
    }
    if ((cursor.next() & 1U) != 0)
        std::reverse(references.begin(), references.end());
    return references;
}

void verifyBoundTree(
    const BoundDeclaredTypeTree & tree,
    const std::vector<Definition::Ptr> & definitions,
    const std::vector<String> & expected_physical_names)
{
    requireProperty(tree.getDefinitionHandles().size() == definitions.size(), "bound tree retained the wrong definition count");
    requireProperty(tree.getDescriptors().size() == definitions.size(), "bound tree retained the wrong descriptor count");
    requireProperty(tree.getOccurrenceCount() >= definitions.size(), "bound tree lost logical occurrences");
    requireProperty(tree.getNodeCount() > 0, "bound tree contains no physical nodes");

    for (std::size_t index = 0; index < definitions.size(); ++index)
    {
        const auto handle = std::find_if(
            tree.getDefinitionHandles().begin(),
            tree.getDefinitionHandles().end(),
            [&](const auto & candidate) { return candidate->getIdentity() == definitions[index]->getIdentity(); });
        requireProperty(handle != tree.getDefinitionHandles().end(), "bound tree lost a definition handle");

        const auto descriptor = std::find_if(
            tree.getDescriptors().begin(),
            tree.getDescriptors().end(),
            [&](const auto & candidate) { return candidate->getDefinition()->getIdentity() == definitions[index]->getIdentity(); });
        requireProperty(descriptor != tree.getDescriptors().end(), "bound tree lost a logical descriptor");
        requireProperty(
            (*descriptor)->getPersistedDescriptor().getCanonicalPhysicalType() == expected_physical_names[index],
            "bound descriptor changed the expected physical type");
    }
}

void verifyRetainedTreeAfterRelease(const BoundDeclaredTypeTree & tree, const std::vector<String> & expected_physical_names)
{
    requireProperty(tree.getDescriptors().size() == expected_physical_names.size(), "retained tree changed its descriptor count");
    requireProperty(
        tree.getDefinitionHandles().size() == expected_physical_names.size(), "retained tree changed its definition-handle count");
    for (std::size_t index = 0; index < expected_physical_names.size(); ++index)
    {
        const auto descriptor = std::find_if(
            tree.getDescriptors().begin(),
            tree.getDescriptors().end(),
            [&](const auto & candidate) { return candidate->getDefinition()->getIdentity() == identity(index); });
        requireProperty(descriptor != tree.getDescriptors().end(), "retained tree lost a dereferenceable descriptor");
        requireProperty(
            (*descriptor)->getDefinition()->getNormalizedLocalName() == localName(index),
            "retained descriptor changed its definition after authority release");
        requireProperty(
            (*descriptor)->getPersistedDescriptor().getCanonicalPhysicalType() == expected_physical_names[index],
            "retained descriptor changed its physical type after authority release");
    }
}

void verifyFailedGenerationPublication(TypeCatalogRoot::Ptr initial_root, const std::vector<Definition::Ptr> & definitions)
{
    Catalog catalog(std::move(initial_root), catalogPublicationLimits());
    const TypeCatalogRetirementState before = catalog.getRetirementState();
    auto rejected = TypeCatalogBuilder::build(
        database_uuid, catalog_generation, std::span<const Definition::Ptr>{}, catalogBuildLimits());
    try
    {
        catalog.publish(std::move(rejected));
    }
    catch (const CatalogError & error)
    {
        requireProperty(
            error.code == CatalogError::Code::GenerationMismatch, "same-generation publication returned the wrong error");
        requireProperty(catalog.currentGeneration() == catalog_generation, "failed publication changed the live generation");
        requireProperty(catalog.getRetirementState() == before, "failed publication changed retirement state");
        for (const auto & definition : definitions)
        {
            requireProperty(
                catalog.findByIdentity(definition->getIdentity()) == definition, "failed publication changed the identity index");
            requireProperty(
                catalog.findByName(definition->getNormalizedLocalName()) == definition, "failed publication changed the name index");
        }
        return;
    }
    propertyViolation("same-generation publication succeeded");
}

void runValid(CheckedDefinitions checked, TypeCatalogRoot::Ptr root, InputCursor & cursor)
{
    std::vector<std::weak_ptr<const Definition>> weak_definitions;
    weak_definitions.reserve(checked.definitions.size());
    for (const auto & definition : checked.definitions)
        weak_definitions.push_back(definition);

    BoundDeclaredTypeTree::Ptr retained_tree;
    DataTypePtr retained_physical_type;
    {
        auto authority = makeTransientAuthorityAdapter(database_uuid, authorityCapabilities(), checked.definitions);
        verifyExplicitSpecialization(*authority, checked.definitions, checked.expected_physical_names, cursor);

        const Declaration declaration = makeDeclaration(checked.definitions, checked.expected_physical_names);
        auto references = makeReferences(declaration, checked.definitions, cursor);
        TypeResolverStatistics statistics;
        const auto bound = TypeResolver::resolve(declaration.root, references, *authority, resolverLimits(), &statistics);
        requireProperty(bound.hasLogicalTree(), "resolver omitted the logical tree");
        requireProperty(
            bound.getPhysicalType()->getName() == declaration.expected_physical_name, "resolver changed the declaration physical type");
        requireProperty(statistics.input_references == checked.definitions.size(), "resolver statistics lost input references");
        requireProperty(statistics.physical_factory_calls == 1, "resolver did not use exactly one declaration factory call");
        requireProperty(statistics.specializer.resolution_sessions == 1, "resolver opened the wrong number of authority sessions");
        requireProperty(statistics.logical_occurrences >= checked.definitions.size(), "resolver statistics lost logical occurrences");

        verifyBoundTree(*bound.getLogicalTree(), checked.definitions, checked.expected_physical_names);
        retained_tree = bound.getLogicalTree();
        retained_physical_type = bound.getPhysicalType();
    }

    verifyFailedGenerationPublication(std::move(root), checked.definitions);
    checked.definitions.clear();
    checked.definitions.shrink_to_fit();

    requireProperty(static_cast<bool>(retained_tree), "retained bound tree is null");
    requireProperty(static_cast<bool>(retained_physical_type), "retained physical type is null");
    for (const auto & weak : weak_definitions)
        requireProperty(!weak.expired(), "bound tree did not retain a definition descriptor");

    const UInt64 retained_node_count = retained_tree->getNodeCount();
    const UInt64 retained_occurrence_count = retained_tree->getOccurrenceCount();
    requireProperty(retained_node_count > 0 && retained_occurrence_count > 0, "retained bound tree became empty");
    verifyRetainedTreeAfterRelease(*retained_tree, checked.expected_physical_names);
    retained_tree.reset();
    for (const auto & weak : weak_definitions)
        requireProperty(weak.expired(), "destroyed bound tree leaked a definition handle");
}

InputMode inputMode(UInt8 byte) noexcept
{
    switch (byte % 8)
    {
        case 6: return InputMode::CheckerInvalid;
        case 7: return InputMode::ResolverInvalid;
        default: return InputMode::Valid;
    }
}

void runOneInput(std::span<const uint8_t> input)
{
    InputCursor cursor(input);
    const InputMode mode = inputMode(cursor.next());
    auto generated = generateDefinitions(cursor);
    if (mode == InputMode::CheckerInvalid)
    {
        runCheckerInvalid(std::move(generated), cursor);
        return;
    }

    auto checked = checkValidDefinitions(std::move(generated));
    auto root = TypeCatalogBuilder::build(catalog_generation, checked.definitions, catalogBuildLimits());
    verifyCatalogIndexes(*root, checked.definitions, catalog_generation);
    if (mode == InputMode::ResolverInvalid)
    {
        runResolverInvalid(checked.definitions);
        return;
    }

    runValid(std::move(checked), std::move(root), cursor);
}

}
}

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, std::size_t size)
{
    if (size > DB::UDT::maximum_input_size)
        return 0;

    try
    {
        const std::span<const uint8_t> input = size == 0 ? std::span<const uint8_t>{} : std::span<const uint8_t>(data, size);
        DB::UDT::runOneInput(input);
    }
    catch (const DB::Exception & error)
    {
        if (error.code() == DB::ErrorCodes::LOGICAL_ERROR)
            DB::UDT::propertyViolation("production code reported LOGICAL_ERROR");
        DB::UDT::propertyViolation("unexpected DB exception escaped the bounded input model");
    }
    catch (const std::exception &)
    {
        DB::UDT::propertyViolation("unexpected standard exception escaped the bounded input model");
    }
    catch (...)
    {
        DB::UDT::propertyViolation("unexpected non-standard exception escaped the bounded input model");
    }

    return 0;
}
