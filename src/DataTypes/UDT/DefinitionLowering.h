#pragma once

#include <DataTypes/UDT/Definition.h>

#include <Core/Types.h>

#include <cstddef>
#include <span>
#include <string_view>
#include <vector>

namespace DB
{
class ASTCreateTypeQuery;
}

namespace DB::UDT
{

/// Identifier components are supplied by the caller's normalization boundary.
/// Keeping all three fields structured avoids recovering a local identifier by
/// splitting formatted SQL, which is ambiguous for quoted names containing dots.
struct StructuredDefinitionName
{
    String normalized_database_name;
    String normalized_qualified_name;
    String normalized_local_name;

    bool operator==(const StructuredDefinitionName &) const = default;
};

/// One same-database name binding visible while lowering a complete authority
/// snapshot. The span may include the definition being lowered, allowing one
/// shared complete snapshot at startup. Definition hashes may be zero for a
/// fresh batch; TemplateChecker derives them after dependency ordering, while
/// startup supplies stored hashes.
struct AvailableDefinitionBinding
{
    StructuredDefinitionName name;
    DefinitionIdentity identity;
    Digest definition_hash{};
    std::vector<ParameterKind> parameter_kinds;

    bool operator==(const AvailableDefinitionBinding &) const = default;
};

struct DefinitionLoweringLimits
{
    /// Total definitions in the Atomic authority. The binding span may
    /// include the definition being lowered, so recovery can reuse one shared
    /// complete-authority span for every definition without filtering it.
    UInt64 maximum_definitions = 10'000;
    UInt64 maximum_formals = 64;
    UInt64 maximum_ast_nodes = 8'192;
    UInt64 maximum_ast_edges = 65'536;
    UInt64 maximum_ast_depth = 128;
    UInt64 maximum_output_nodes = 4'096;
    UInt64 maximum_output_edges = 65'536;
    UInt64 maximum_enum_entries = 65'536;
    UInt64 maximum_string_bytes = 256ULL << 10;
    /// Aggregate bytes retained by the one-per-authority prepared binding
    /// index. This is separate from the per-definition AST/output budget.
    UInt64 maximum_catalog_string_bytes = 64ULL << 20;
    UInt64 maximum_total_string_bytes = 1ULL << 20;
    UInt64 maximum_dependencies = 256;
    CanonicalFieldValueLimits field_values{
        .maximum_nodes = 4'096,
        .maximum_edges = 65'536,
        .maximum_entries = 65'536,
        .maximum_depth = 64,
        .maximum_literal_bytes = 256ULL << 10,
    };
};

struct DefinitionLoweringRequest
{
    DefinitionIdentity identity;
    StructuredDefinitionName name;
    std::span<const AvailableDefinitionBinding> available_bindings;
};

struct DefinitionLoweringBindingPreparationStatistics
{
    UInt64 validated_bindings = 0;
    UInt64 catalog_string_bytes = 0;
    UInt64 name_index_entries = 0;
    UInt64 identity_index_entries = 0;

    bool operator==(const DefinitionLoweringBindingPreparationStatistics &) const = default;
};

/// One immutable, owning name/identity index for a complete database
/// authority. Startup prepares it once and reuses it for every definition;
/// no caller-owned strings or spans survive this boundary.
class PreparedDefinitionLoweringBindings final
{
public:
    PreparedDefinitionLoweringBindings(const PreparedDefinitionLoweringBindings &) = delete;
    PreparedDefinitionLoweringBindings & operator=(const PreparedDefinitionLoweringBindings &) = delete;
    PreparedDefinitionLoweringBindings(PreparedDefinitionLoweringBindings &&) noexcept = default;
    PreparedDefinitionLoweringBindings & operator=(PreparedDefinitionLoweringBindings &&) noexcept = default;
    ~PreparedDefinitionLoweringBindings() = default;

    const UUID & getDatabaseUUID() const noexcept { return database_uuid; }
    std::string_view getNormalizedDatabaseName() const noexcept { return normalized_database_name; }
    const DefinitionLoweringLimits & getLimits() const noexcept { return limits; }
    const DefinitionLoweringBindingPreparationStatistics & getStatistics() const noexcept { return statistics; }

    const AvailableDefinitionBinding * findByLocalName(std::string_view normalized_local_name) const noexcept;
    const AvailableDefinitionBinding * findByIdentity(const DefinitionIdentity & identity) const noexcept;

private:
    PreparedDefinitionLoweringBindings(
        UUID database_uuid_,
        String normalized_database_name_,
        DefinitionLoweringLimits limits_,
        std::vector<AvailableDefinitionBinding> bindings_,
        std::vector<std::size_t> name_order_,
        std::vector<std::size_t> identity_order_,
        DefinitionLoweringBindingPreparationStatistics statistics_);

    friend PreparedDefinitionLoweringBindings
    prepareDefinitionLoweringBindings(UUID, String, std::vector<AvailableDefinitionBinding>, const DefinitionLoweringLimits &);

    UUID database_uuid;
    String normalized_database_name;
    DefinitionLoweringLimits limits;
    std::vector<AvailableDefinitionBinding> bindings;
    std::vector<std::size_t> name_order;
    std::vector<std::size_t> identity_order;
    DefinitionLoweringBindingPreparationStatistics statistics;
};

/// Validates and owns a complete same-database binding set and builds both
/// lookup indexes once. All work is prospective under the supplied limits.
PreparedDefinitionLoweringBindings prepareDefinitionLoweringBindings(
    UUID database_uuid,
    String normalized_database_name,
    std::vector<AvailableDefinitionBinding> bindings,
    const DefinitionLoweringLimits & limits = {});

/// Converts only the parser-owned CREATE/ATTACH TYPE surface into the semantic
/// checker input. It performs no Context, catalog, registry, or SQL callbacks.
DefinitionInput lowerCreateTypeQueryToDefinitionInput(
    const ASTCreateTypeQuery & query, const DefinitionLoweringRequest & request, const DefinitionLoweringLimits & limits = {});

/// Batch/startup entry point. The prepared object freezes both limits and the
/// complete binding catalog, so each definition pays only for its own AST and
/// dependency lookups.
DefinitionInput lowerCreateTypeQueryToDefinitionInput(
    const ASTCreateTypeQuery & query,
    DefinitionIdentity identity,
    const StructuredDefinitionName & name,
    const PreparedDefinitionLoweringBindings & prepared_bindings);

/// Exact semantic copy used when a caller needs to include already checked
/// definitions in the complete same-database batch passed to checkAll().
DefinitionInput definitionInputFromCheckedDefinition(const Definition & definition);

}
