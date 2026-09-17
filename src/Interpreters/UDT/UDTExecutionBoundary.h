#pragma once

#include <Interpreters/UDT/QueryResultCacheStorageDependencies.h>
#include <Parsers/IAST_fwd.h>

#include <cstddef>
#include <cstdint>
#include <memory>
#include <mutex>

namespace DB
{

class IAST;
class InterpreterFactory;
class ASTAlterQuery;
class ASTCreateQuery;

namespace UDT
{

struct UDTExecutionBoundaryOptions
{
    bool allow_experimental_analyzer = false;
    bool allow_experimental_user_defined_types = false;
    bool inspect_query_result_cache_candidates = false;

    bool operator==(const UDTExecutionBoundaryOptions &) const = default;
};

/// One-shot capability for the only non-SELECT roots whose exact SELECT child
/// is owned by an inventoried semantic-analysis/persistence path. The
/// capability retains the original root and child identity: an interpreter
/// must consume it before cloning or rewriting that child, then run its own
/// analyzer/binder over the resulting generation. It authorizes no other AST
/// field and carries no catalog, access, or semantic result.
class UDTStoredObjectDDLSelectBoundaryHandoff final
{
public:
    using Ptr = std::shared_ptr<UDTStoredObjectDDLSelectBoundaryHandoff>;

    UDTStoredObjectDDLSelectBoundaryHandoff(const UDTStoredObjectDDLSelectBoundaryHandoff &) = delete;
    UDTStoredObjectDDLSelectBoundaryHandoff & operator=(const UDTStoredObjectDDLSelectBoundaryHandoff &) = delete;

    void consumeForCreate(const ASTCreateQuery & root_, const IAST & select_);
    void consumeForAlter(const ASTAlterQuery & root_, const IAST & select_);

private:
    enum class Owner : uint8_t
    {
        Create,
        AlterModifyQuery,
    };

    UDTStoredObjectDDLSelectBoundaryHandoff(ASTPtr root_, const IAST * select_, Owner owner_);
    void consume(const IAST & root_, const IAST & select_, Owner owner_);

    friend class UDTExecutionBoundaryProof;

    std::mutex mutex;
    ASTPtr root;
    const IAST * select = nullptr;
    Owner owner;
    bool consumed = false;
};

/// One-shot opaque evidence that one exact `AST` root crossed the pre-side-effect UDT
/// boundary under the retained settings. The proof owns the root, so pointer
/// reuse cannot substitute another `AST`. It is intentionally not constructible
/// by dispatch callers and is consumed by `InterpreterFactory`.
///
/// The `AST` may undergo only the audited routing-only mutations between proof
/// creation and interpreter dispatch (for example, `INSERT` tail/table identity
/// setup). Any expression or type-tree rewrite invalidates this contract and
/// must be followed by a new validation.
class UDTExecutionBoundaryProof final
{
public:
    UDTExecutionBoundaryProof(const UDTExecutionBoundaryProof &) = delete;
    UDTExecutionBoundaryProof & operator=(const UDTExecutionBoundaryProof &) = delete;
    UDTExecutionBoundaryProof(UDTExecutionBoundaryProof &&) noexcept = default;
    UDTExecutionBoundaryProof & operator=(UDTExecutionBoundaryProof &&) = delete;

    /// Non-consuming cache-safety predicate collected during the mandatory
    /// execution/AST-size walk. It is intentionally conservative: a candidate
    /// must reach QueryAnalyzer for catalog/access checks before any cached
    /// result can be read or written.
    bool hasPotentialUDTSemanticSinkCandidate() const noexcept { return has_potential_udt_semantic_sink_candidate; }

    /// Closed contextual syntax classes which may become semantic only after
    /// the analyzer selects an exact mapped column/use.
    QueryResultCacheContextualSinkCandidateMask getPotentialQueryResultCacheContextualSinkCandidates() const noexcept
    {
        return potential_query_result_cache_contextual_sink_candidates;
    }

    /// Collected by the mandatory AST-size walk, or by the equivalent
    /// candidate-only walk when the element limit is unlimited.
    bool hasPotentialStorageReference() const noexcept { return has_potential_storage_reference; }

    /// True only when the mandatory size walk observed a concrete table
    /// expression.
    bool hasObservedStorageReference() const noexcept { return has_observed_storage_reference; }

    /// An explicit SELECT/subquery `use_query_cache` setting observed during
    /// the same walk.  The top-level Context setting is checked separately.
    bool hasPotentialQueryResultCacheUse() const noexcept { return has_potential_query_result_cache_use; }

private:
    UDTExecutionBoundaryProof(
        ASTPtr root_,
        UDTExecutionBoundaryOptions options_,
        bool has_potential_udt_semantic_sink_candidate_ = false,
        bool has_potential_storage_reference_ = false,
        bool has_observed_storage_reference_ = false,
        bool has_potential_query_result_cache_use_ = false,
        QueryResultCacheContextualSinkCandidateMask potential_query_result_cache_contextual_sink_candidates_ = 0,
        const IAST * stored_object_ddl_select_ = nullptr,
        bool stored_object_ddl_select_is_alter_ = false);

    /// Constant-time one-shot check used at the final dispatch boundary.
    UDTStoredObjectDDLSelectBoundaryHandoff::Ptr
    consumeForDispatch(const IAST & root_, const UDTExecutionBoundaryOptions & current_options);

    friend class DB::InterpreterFactory;
    friend UDTExecutionBoundaryProof validateUDTExecutionBoundary(const ASTPtr &, const UDTExecutionBoundaryOptions &);
    friend UDTExecutionBoundaryProof validateUDTExecutionBoundaryAndSize(const ASTPtr &, size_t, const UDTExecutionBoundaryOptions &);

    ASTPtr root;
    UDTExecutionBoundaryOptions options;
    bool has_potential_udt_semantic_sink_candidate = false;
    bool has_potential_storage_reference = false;
    bool has_observed_storage_reference = false;
    bool has_potential_query_result_cache_use = false;
    QueryResultCacheContextualSinkCandidateMask potential_query_result_cache_contextual_sink_candidates = 0;
    UDTStoredObjectDDLSelectBoundaryHandoff::Ptr stored_object_ddl_select_handoff;
};

/// `SELECT`-family roots reach an analyzer before execution. Every other root
/// must cross this boundary before it can be dispatched or persisted.
[[nodiscard]] bool requiresUDTExecutionBoundaryValidation(const IAST & root) noexcept;

/// Validate the whole execution-relevant AST, including the expression fields
/// which some `AST` nodes keep outside `IAST::children`. Returns the exact proof
/// required by `InterpreterFactory`; validation and dispatch are therefore never
/// duplicated for the ordinary `executeQuery` path.
[[nodiscard]] UDTExecutionBoundaryProof validateUDTExecutionBoundary(const ASTPtr & root, const UDTExecutionBoundaryOptions & options);

/// Preserve the usual `AST`-limit error precedence while collecting the UDT
/// result during one execution-aware size walk. AST fields outside `children`
/// consume the same budget without being counted twice when a deserializer also
/// exposes one as a regular child.
[[nodiscard]] UDTExecutionBoundaryProof
validateUDTExecutionBoundaryAndSize(const ASTPtr & root, size_t max_ast_elements, const UDTExecutionBoundaryOptions & options);

}
}
