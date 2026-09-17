#pragma once

#include <DataTypes/UDT/CanonicalTypeArguments.h>
#include <DataTypes/UDT/IAuthorityAdapter.h>

#include <Core/Types.h>

#include <Parsers/IAST_fwd.h>

#include <limits>
#include <memory>
#include <stdexcept>
#include <string_view>
#include <vector>

namespace DB::UDT
{

class ProspectiveResourceBudget;

using TemplateSpecializationID = UInt32;
inline constexpr TemplateSpecializationID invalid_template_specialization_id = std::numeric_limits<TemplateSpecializationID>::max();

class TemplateSpecializerError final : public std::runtime_error
{
public:
    enum class Code : UInt8
    {
        InvalidAttemptState,
        MissingCapability,
        AuthorityFailure,
        InvalidIdentity,
        DefinitionNotFound,
        DependencyMismatch,
        InvalidArguments,
        InvalidTemplate,
        ActiveCycle,
        NonDecreasingRecursion,
        LimitExceeded,
    };

    TemplateSpecializerError(Code code_, std::string_view message);

    Code code;
};

/// Paths follow the direct child order committed by the complete
/// DataTypesBinaryEncoding. Variant is the sole V1 normalization boundary:
/// its factory sorts by final type name and squashes equal/Nothing branches.
/// The resolver uses the source ordinal together with the current canonical
/// physical AST to build one bounded normalization map after factory creation.
enum class PhysicalTypeChildLocatorKind : UInt8
{
    StableOrdinal,
    VariantNormalizedBranch,
};

struct PhysicalTypeChildLocator
{
    PhysicalTypeChildLocatorKind kind = PhysicalTypeChildLocatorKind::StableOrdinal;
    UInt32 source_ordinal = 0;

    bool operator==(const PhysicalTypeChildLocator &) const = default;
};

using RelativePhysicalTypePath = std::vector<PhysicalTypeChildLocator>;

/// The one ordered lineage stream contains both instantiated definitions and
/// formal TYPE-argument substitution points. Keeping them in one stream is
/// necessary when both occur at the same physical path: a resolver can replay
/// the caller's nested argument lineage at the exact point, without adding it
/// to the physical specialization memo key. source_ordinal is a
/// TemplateSpecializationID for Specialization and a formal parameter ordinal
/// for TypeArgument. Multiple records may name the same path and must never be
/// deduplicated; their order is logical outer-to-inner order.
enum class RelativeLogicalTypeOccurrenceKind : UInt8
{
    Specialization,
    TypeArgument,
};

struct RelativeLogicalTypeOccurrence
{
    RelativePhysicalTypePath path;
    RelativeLogicalTypeOccurrenceKind kind = RelativeLogicalTypeOccurrenceKind::Specialization;
    UInt32 source_ordinal = invalid_template_specialization_id;

    bool operator==(const RelativeLogicalTypeOccurrence &) const = default;
};

/// Independent prospective limits. A caller may lower these values. Raising
/// one above its implementation maximum is rejected rather than silently
/// widening an already-reviewed resource contract.
struct TemplateSpecializerLimits
{
    UInt64 maximum_distinct_specializations = 4'096;
    UInt64 maximum_definition_handles = 1'024;
    UInt64 maximum_definition_lookups = 4'096;
    UInt64 maximum_specialization_depth = 64;
    UInt64 maximum_canonical_argument_bytes = 64ULL << 10;
    UInt64 maximum_canonical_argument_item_bytes = 16ULL << 10;
    UInt64 maximum_memo_key_bytes = 64ULL << 20;
    UInt64 maximum_template_node_occurrences = 1ULL << 20;
    UInt64 maximum_constructed_ast_nodes = 1ULL << 20;
    UInt64 maximum_constructed_ast_edges = 4ULL << 20;
    UInt64 maximum_ast_depth = 64;
    UInt64 maximum_field_depth = 256;
    UInt64 maximum_owned_ast_string_bytes = 64ULL << 20;
    UInt64 maximum_enum_entries = 1ULL << 20;
    UInt64 maximum_retained_occurrences = 1ULL << 20;
    UInt64 maximum_retained_path_components = 4ULL << 20;
    UInt64 maximum_emitted_ast_node_occurrences = 1ULL << 20;
    UInt64 maximum_emitted_ast_edges = 4ULL << 20;
    UInt64 maximum_emitted_occurrences = 1ULL << 20;
    UInt64 maximum_emitted_path_components = 4ULL << 20;
    UInt64 maximum_work = 8ULL << 20;

    bool operator==(const TemplateSpecializerLimits &) const = default;
};

/// Validates the complete limit tuple without opening an authority session or
/// allocating specialization state. Composite callers use this to reject an
/// invalid lowered domain before any activated-path work begins.
void validateTemplateSpecializerLimits(const TemplateSpecializerLimits & limits);

struct TemplateSpecializerStatistics
{
    UInt64 resolution_sessions = 0;
    UInt64 specialization_requests = 0;
    UInt64 distinct_specializations = 0;
    UInt64 specialization_memo_hits = 0;
    UInt64 definition_lookups = 0;
    UInt64 maximum_specialization_depth = 0;
    UInt64 distinct_definition_handles = 0;
    UInt64 memo_key_bytes = 0;
    UInt64 template_node_occurrences = 0;
    UInt64 constructed_ast_nodes = 0;
    UInt64 constructed_ast_edges = 0;
    UInt64 maximum_ast_depth = 0;
    UInt64 owned_ast_string_bytes = 0;
    UInt64 enum_entries = 0;
    UInt64 retained_occurrences = 0;
    UInt64 retained_path_components = 0;
    UInt64 emitted_ast_node_occurrences = 0;
    UInt64 emitted_ast_edges = 0;
    UInt64 emitted_occurrences = 0;
    UInt64 emitted_path_components = 0;
    UInt64 charged_work = 0;

    bool operator==(const TemplateSpecializerStatistics &) const = default;
};

/// One distinct memo entry. definition_handle_index addresses the batch's
/// sorted/deduplicated O(D) handle vector; no per-specialization shared_ptr is
/// retained. IDs are attempt-local encounter-order indexes and are not a wire
/// or persistence identity.
struct TemplateSpecialization
{
    DefinitionIdentity definition_identity;
    UInt32 definition_handle_index = 0;
    CanonicalTypeArguments canonical_arguments;
    ASTPtr canonical_physical_ast;
    std::vector<RelativeLogicalTypeOccurrence> relative_occurrences;
};

struct FinishedTemplateSpecializations
{
    std::vector<TemplateSpecialization> specializations;
    std::vector<Definition::Ptr> definition_handles;
    TemplateSpecializerStatistics statistics;
};

/// Borrowed immutable view into one completed query-memo specialization. The
/// owner outlives the synchronous resolver call; no AST, session, or view may
/// be retained by the published declared-type result.
struct TemplateSpecializationView
{
    const DefinitionIdentity & definition_identity;
    const CanonicalTypeArguments & canonical_arguments;
    const ASTPtr & canonical_physical_ast;
    std::span<const RelativeLogicalTypeOccurrence> relative_occurrences;
    /// Keep an independent handle: the query memo may append another
    /// definition after this view is returned and reallocate its handle vector.
    Definition::Ptr definition_handle;
};

/// This class has deliberately no ordinary physical-type path. It exists only
/// after a caller has identified at least one UDT reference, so built-in-only
/// query execution cannot acquire a session, allocate a memo, or pay a branch.
class TemplateSpecializer final
{
public:
    class QueryMemo;

    class Attempt final
    {
    public:
        Attempt(const Attempt &) = delete;
        Attempt & operator=(const Attempt &) = delete;
        Attempt(Attempt &&) noexcept;
        Attempt & operator=(Attempt &&) = delete;
        ~Attempt();

        /// `query_budget`, when supplied, accumulates work and exact descriptor
        /// identities across attempts and must outlive this Attempt.
        [[nodiscard]] static Attempt begin(
            const IAuthorityAdapter & authority,
            const TemplateSpecializerLimits & limits = {},
            ProspectiveResourceBudget * query_budget = nullptr);

        /// On success, the ID and its borrowed AST remain attempt-local until
        /// finish() commits the final batch. Any exception poisons the attempt
        /// and releases its pinned authority session immediately.
        [[nodiscard]] TemplateSpecializationID specialize(const DefinitionIdentity & identity, const CanonicalTypeArguments & arguments);

        /// Recovery/binding entry point for the frozen canonical-argument
        /// envelope. The definition is resolved before decoding its formal
        /// kinds, and both operations use this attempt's one pinned authority
        /// generation.
        [[nodiscard]] TemplateSpecializationID specializeEncoded(
            const DefinitionIdentity & identity,
            std::string_view canonical_arguments,
            const CanonicalTypeArgumentLimits & type_argument_limits = {});

        /// Attempt-scoped view used only to assemble a larger transaction-local
        /// physical AST. Such aliases must not be published or escape unless
        /// finish() succeeds; finish() is the sole ownership-commit boundary.
        [[nodiscard]] const ASTPtr & getCanonicalPhysicalAST(TemplateSpecializationID id);

        /// Mandatory, single-use commit. It releases the pinned session before
        /// returning and moves out only canonical ASTs, canonical arguments,
        /// compact occurrence routes, and independent definition handles.
        [[nodiscard]] FinishedTemplateSpecializations finish();

    private:
        friend class QueryMemo;

        class State;

        explicit Attempt(std::unique_ptr<State> state_);
        [[nodiscard]] static Attempt beginImpl(
            const IAuthorityAdapter & authority,
            const TemplateSpecializerLimits & limits,
            ProspectiveResourceBudget * query_budget,
            bool retain_query_memo);
        [[noreturn]] void invalidState(std::string_view operation) const;
        void poison() noexcept;
        void closeQueryMemo() noexcept;
        void releaseQueryMemoSession() noexcept;
        [[nodiscard]] TemplateSpecializationID
        specializeFromQueryMemo(
            const IAuthorityAdapter & authority, const DefinitionIdentity & identity, const CanonicalTypeArguments & arguments);
        [[nodiscard]] TemplateSpecializationView getSpecialization(TemplateSpecializationID id) const;
        [[nodiscard]] const TemplateSpecializerStatistics & getStatistics() const;
        [[nodiscard]] const TemplateSpecializerLimits & getLimits() const;
        [[nodiscard]] UUID getAuthorityDatabaseUUID() const;
        [[nodiscard]] UInt64 getAuthorityGeneration() const;

        std::unique_ptr<State> state;
        bool finished = false;
        bool poisoned = false;
    };

    /// Query-local exact specialization memo. It retains only independent
    /// definition/AST handles, never an authority session or catalog root.
    /// Every specialize() call attaches one short-lived session and requires
    /// the memo's original immutable generation before any lookup or expansion.
    /// Exact equality is `(DefinitionIdentity, canonical argument bytes)`; the
    /// digest is only a hash-table accelerator. A caller must still resolve the
    /// name and authorize its stable identity before every lookup; a hit is
    /// never an access decision.
    class QueryMemo final
    {
    public:
        QueryMemo(const IAuthorityAdapter & authority, const TemplateSpecializerLimits & limits, ProspectiveResourceBudget & query_budget);
        ~QueryMemo();

        QueryMemo(const QueryMemo &) = delete;
        QueryMemo & operator=(const QueryMemo &) = delete;
        QueryMemo(QueryMemo &&) = delete;
        QueryMemo & operator=(QueryMemo &&) = delete;

        [[nodiscard]] TemplateSpecializationID specialize(
            const IAuthorityAdapter & authority, const DefinitionIdentity & identity, const CanonicalTypeArguments & arguments);
        [[nodiscard]] const ASTPtr & getCanonicalPhysicalAST(TemplateSpecializationID id);
        [[nodiscard]] TemplateSpecializationView getSpecialization(TemplateSpecializationID id) const;
        [[nodiscard]] const TemplateSpecializerStatistics & getStatistics() const;
        [[nodiscard]] const TemplateSpecializerLimits & getLimits() const;
        [[nodiscard]] UUID getAuthorityDatabaseUUID() const;
        [[nodiscard]] UInt64 getAuthorityGeneration() const;
        [[nodiscard]] bool usesResourceBudget(const ProspectiveResourceBudget & query_budget_) const noexcept;

    private:
        ProspectiveResourceBudget * resource_budget;
        Attempt attempt;
    };

private:
    TemplateSpecializer() = delete;
};
}
