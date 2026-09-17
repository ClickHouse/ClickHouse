#pragma once

#include <DataTypes/UDT/Definition.h>

#include <Core/Types.h>

#include <string_view>
#include <vector>

namespace DB::UDT
{

/// All limits are independent and prospective: exceeding one fails before the
/// allocation or work unit it protects. Raising a caller limit cannot exceed
/// a newer implementation maximum without a checker-ABI revision.
struct TemplateCheckerLimits
{
    UInt64 maximum_definitions = 4'096;
    UInt64 maximum_formals = 64;
    UInt64 maximum_definition_input_bytes = 1ULL << 20;
    UInt64 maximum_catalog_input_bytes = 64ULL << 20;
    UInt64 maximum_template_nodes = 4'096;
    UInt64 maximum_logical_node_occurrences = 4'096;
    UInt64 maximum_template_edges = 65'536;
    UInt64 maximum_template_depth = 32;
    UInt64 maximum_catalog_nodes = 256ULL << 10;
    /// Aggregate direct-dependency graph edges. Template child edges remain
    /// independently bounded per definition by `maximum_template_edges` and
    /// globally by accepted/canonical catalog bytes.
    UInt64 maximum_catalog_edges = 1ULL << 20;
    UInt64 maximum_direct_dependencies = 256;
    UInt64 maximum_transitive_dependencies = 1'024;
    UInt64 maximum_checker_work = 65'536;
    UInt64 maximum_catalog_checker_work = 8'000'000;
    UInt64 maximum_canonical_definition_bytes = 256ULL << 10;
    UInt64 maximum_canonical_catalog_bytes = 16ULL << 20;
    UInt64 maximum_formal_name_bytes = 1ULL << 10;
    UInt64 maximum_ir_atom_bytes = 64ULL << 10;
    UInt64 maximum_ir_literal_bytes = 256ULL << 10;
    UInt64 maximum_ir_identifier_bytes = 1ULL << 10;
    UInt64 maximum_ir_enum_entries = 1ULL << 16;
    UInt64 maximum_scratch_bytes = 64ULL << 20;
};

struct TemplateCheckerStatistics
{
    UInt64 accepted_input_bytes = 0;
    UInt64 maximum_definition_input_bytes = 0;
    UInt64 checked_definitions = 0;
    UInt64 graph_edges = 0;
    UInt64 charged_work = 0;
    UInt64 canonical_bytes = 0;
    UInt64 scratch_peak_bytes = 0;
};

/// Context-free, deterministic production checker. It consumes a complete
/// same-database definition set, validates dependency hashes in one
/// dependency-first pass, and returns immutable definitions in input order.
/// Nothing is published on failure.
class TemplateChecker final
{
public:
    using BuiltInFamilyPredicateForTest = bool (*)(std::string_view) noexcept;

    /// Test-only injection seam. Production callers use checkAll(), which
    /// constructs the two deliberately different predicates internally.
    struct BuiltInFamilyAuthorityForTest
    {
        BuiltInFamilyPredicateForTest is_registered_family = nullptr;
        BuiltInFamilyPredicateForTest collides_with_registered_family_or_alias = nullptr;
    };

    /// Production entry point: built-in atoms follow DataTypeFactory's real
    /// case policy, while UDT names reserve the ASCII-folded spelling of every
    /// registered family and alias. Callers cannot substitute either rule.
    [[nodiscard]] static std::vector<Definition::Ptr> checkAll(
        std::vector<DefinitionInput> inputs, const TemplateCheckerLimits & limits = {}, TemplateCheckerStatistics * statistics = nullptr);

    [[nodiscard]] static std::vector<Definition::Ptr> checkAllWithBuiltInFamilyAuthorityForTest(
        std::vector<DefinitionInput> inputs,
        const BuiltInFamilyAuthorityForTest & built_in_authority,
        const TemplateCheckerLimits & limits = {},
        TemplateCheckerStatistics * statistics = nullptr);

private:
    [[nodiscard]] static std::vector<Definition::Ptr> checkAllImpl(
        std::vector<DefinitionInput> inputs,
        BuiltInFamilyPredicateForTest is_registered_family,
        BuiltInFamilyPredicateForTest collides_with_registered_family_or_alias,
        const TemplateCheckerLimits & limits,
        TemplateCheckerStatistics * statistics);

    TemplateChecker() = delete;
};

}
