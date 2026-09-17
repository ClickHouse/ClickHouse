#pragma once

#include <DataTypes/UDT/InstantiatedTypeDescriptor.h>
#include <DataTypes/UDT/TemplateSpecializer.h>

#include <Parsers/IAST_fwd.h>

#include <span>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

class TypeResolverError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidRoot,
        InvalidReference,
        DuplicateReference,
        InvalidArgumentLineage,
        DuplicateArgumentLineage,
        ArgumentLineageCycle,
        UnreachableArgumentLineage,
        CanonicalArgumentMismatch,
        UnreachableReference,
        UnsubstitutedReference,
        InvalidASTShape,
        PhysicalTopologyMismatch,
        VariantBranchDropped,
        VariantBranchCollapsed,
        LimitExceeded,
    };

    TypeResolverError(Code code_, std::string_view message);

    Code code;
};

/// One direct logical application nested in a caller TYPE actual. `parameter`
/// is the caller formal ordinal; `path` is relative to that actual and uses
/// the same pre-factory locator contract as TemplateSpecializer. The endpoint
/// must be `reference_node`, which must identify another entry in the same
/// side table. Records are grouped by parameter and are in strict physical
/// preorder; transitive applications belong to the referenced entry, never to
/// its parent. This is binder-produced provenance, not syntax reparsing.
struct DeclaredTypeArgumentLineageInput
{
    UInt16 parameter = 0;
    RelativePhysicalTypePath path;
    const IAST * reference_node = nullptr;

    bool operator==(const DeclaredTypeArgumentLineageInput &) const = default;
};

/// Synthetic binder side-table entry. reference_node is a borrowed identity in
/// declaration_ast; neither this pointer nor the caller AST is retained by a
/// successful result. The resolver independently proves every syntax actual
/// equal to its canonical value; TYPE actuals are physicalized through this
/// direct lineage before their bounded auxiliary factory validation.
struct DeclaredTypeReferenceInput
{
    const IAST * reference_node = nullptr;
    DefinitionIdentity definition_identity;
    CanonicalTypeArguments canonical_arguments;
    std::vector<DeclaredTypeArgumentLineageInput> type_argument_lineage;
};

struct TypeResolverLimits
{
    UInt64 maximum_input_references = 65'536;
    UInt64 maximum_argument_lineage_entries = 65'536;
    UInt64 maximum_argument_validation_factory_calls = 65'536;
    UInt64 maximum_argument_validation_ast_nodes = 1ULL << 20;
    UInt64 maximum_argument_validation_ast_edges = 4ULL << 20;
    UInt64 maximum_argument_validation_ast_depth = 64;
    UInt64 maximum_argument_validation_syntax_bytes = 64ULL << 20;
    UInt64 maximum_argument_validation_binary_bytes = 64ULL << 20;
    UInt64 maximum_declaration_ast_nodes = 1ULL << 20;
    UInt64 maximum_declaration_ast_edges = 4ULL << 20;
    UInt64 maximum_declaration_ast_depth = 64;
    UInt64 maximum_declaration_ast_syntax_bytes = 64ULL << 20;
    UInt64 maximum_physical_ast_nodes = 1ULL << 20;
    UInt64 maximum_physical_ast_edges = 4ULL << 20;
    UInt64 maximum_physical_ast_depth = 64;
    UInt64 maximum_physical_ast_syntax_bytes = 64ULL << 20;
    UInt64 maximum_literal_field_nodes = 1ULL << 20;
    UInt64 maximum_literal_field_edges = 4ULL << 20;
    UInt64 maximum_literal_field_depth = 256;
    UInt64 maximum_path_components = 4ULL << 20;
    UInt64 maximum_logical_occurrences = 65'536;
    UInt64 maximum_variant_branch_factory_calls = 65'536;
    TemplateSpecializerLimits specializer;
    TypeDescriptorLimits descriptors;
};

struct TypeResolverStatistics
{
    UInt64 declaration_ast_nodes = 0;
    UInt64 declaration_ast_edges = 0;
    UInt64 maximum_declaration_ast_depth = 0;
    UInt64 declaration_ast_syntax_bytes = 0;
    UInt64 input_references = 0;
    UInt64 argument_lineage_entries = 0;
    UInt64 argument_validation_factory_calls = 0;
    UInt64 argument_validation_ast_nodes = 0;
    UInt64 argument_validation_ast_edges = 0;
    UInt64 maximum_argument_validation_ast_depth = 0;
    UInt64 argument_validation_syntax_bytes = 0;
    UInt64 argument_validation_physical_ast_nodes = 0;
    UInt64 argument_validation_physical_ast_edges = 0;
    UInt64 maximum_argument_validation_physical_ast_depth = 0;
    UInt64 argument_validation_physical_syntax_bytes = 0;
    UInt64 argument_validation_binary_bytes = 0;
    UInt64 physical_ast_nodes = 0;
    UInt64 physical_ast_edges = 0;
    UInt64 maximum_physical_ast_depth = 0;
    UInt64 physical_ast_syntax_bytes = 0;
    UInt64 literal_field_nodes = 0;
    UInt64 literal_field_edges = 0;
    UInt64 maximum_literal_field_depth = 0;
    UInt64 specialization_requests = 0;
    UInt64 physical_factory_calls = 0;
    UInt64 variant_branch_factory_calls = 0;
    UInt64 logical_occurrences = 0;
    UInt64 bound_nodes = 0;
    TemplateSpecializerStatistics specializer;

    bool operator==(const TypeResolverStatistics &) const = default;
};

/// Schema/type binder. The empty-side-table branch is intentionally a direct
/// DataTypeFactory call and cannot touch an authority adapter or allocate UDT
/// resolver state. The activated branch uses either one snapshot-consistent
/// specialization attempt or a caller-owned exact query memo, then publishes
/// only physical IDataType plus compact, independent logical descriptors. A
/// nonnull query_budget is borrowed only for this synchronous call and makes
/// work/descriptor accounting cumulative across multiple resolver calls. A
/// query_memo must use the same authority generation, limits, and budget; it
/// is never consulted on the physical-only branch.
class TypeResolver final
{
public:
    [[nodiscard]] static BoundDeclaredTypeResult resolve(
        const ASTPtr & declaration_ast,
        std::span<const DeclaredTypeReferenceInput> references,
        const IAuthorityAdapter & authority,
        const TypeResolverLimits & limits = {},
        TypeResolverStatistics * statistics = nullptr,
        ProspectiveResourceBudget * query_budget = nullptr,
        TemplateSpecializer::QueryMemo * query_memo = nullptr);

private:
    TypeResolver() = delete;
};

}
