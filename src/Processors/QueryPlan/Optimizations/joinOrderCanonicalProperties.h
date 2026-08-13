#pragma once

#include <Interpreters/JoinExpressionActions.h>
#include <Processors/QueryPlan/Optimizations/joinOrderDataPropertyCatalog.h>

#include <base/types.h>

#include <expected>
#include <memory>
#include <optional>
#include <span>
#include <variant>
#include <vector>

namespace DB
{

enum class JoinOrderPropertyUnsupportedReason : UInt8
{
    MissingCatalog,
    NonInnerAllRegion,
    CrossOrCommaRegion,
    NonDeterministicPredicate,
    NullSafeEquality,
    AmbiguousEqualityBinding,
    UnsupportedEqualityType,
    NullableEqualityColumn,
    AmbiguousOutputContract,
    InvalidSubset,
    InvalidColumnId,
    ColumnOutsideGroup,
    NullableDemandColumn,
    NullableKeyColumn,
    EmptyDemand,
    InvalidGroup,
    InvalidCut,
    ProviderMismatch,
    UnrepresentableObligation,
};

/// Provider-scoped strong identifier: `value` is provider-local and `provider` pins the
/// issuing `JoinOrderCanonicalProperties` instance. One distinct type per id space so ids
/// cannot be mixed up, without repeating the `{UInt32, UInt64}` struct per space.
template <typename Tag>
struct JoinOrderProviderId
{
    UInt32 value = 0;
    UInt64 provider = 0;
    bool operator==(const JoinOrderProviderId &) const = default;
};

struct JoinOrderSemanticRegionIdTag;
struct JoinOrderPredicateClosureIdTag;
struct JoinOrderOutputContractIdTag;
struct JoinOrderLogicalGroupIdTag;
struct JoinOrderColumnSetIdTag;
struct JoinOrderEqualityCutIdTag;
struct JoinOrderPropertyProofIdTag;

using JoinOrderSemanticRegionId = JoinOrderProviderId<JoinOrderSemanticRegionIdTag>;
using JoinOrderPredicateClosureId = JoinOrderProviderId<JoinOrderPredicateClosureIdTag>;
using JoinOrderOutputContractId = JoinOrderProviderId<JoinOrderOutputContractIdTag>;
using JoinOrderLogicalGroupId = JoinOrderProviderId<JoinOrderLogicalGroupIdTag>;
using JoinOrderColumnSetId = JoinOrderProviderId<JoinOrderColumnSetIdTag>;
using JoinOrderEqualityCutId = JoinOrderProviderId<JoinOrderEqualityCutIdTag>;

/// Provider-local opaque attestation handle. Its value is stable for the provider lifetime,
/// but it does not identify a stored, reconstructable proof DAG record.
using JoinOrderPropertyProofId = JoinOrderProviderId<JoinOrderPropertyProofIdTag>;

using JoinOrderGroupLookupResult = std::expected<JoinOrderLogicalGroupId, JoinOrderPropertyUnsupportedReason>;
using JoinOrderColumnSetLookupResult = std::expected<JoinOrderColumnSetId, JoinOrderPropertyUnsupportedReason>;

struct JoinOrderNoEqualityCut
{
    bool operator==(const JoinOrderNoEqualityCut &) const = default;
};

/// Exactly one state is representable (the flat-variant idiom shared by every tri-state
/// answer of the provider): no cut exists, a cut id, or a typed unsupported condition.
/// Default construction yields the ordinary negative answer.
using JoinOrderEqualityCutResult = std::variant<JoinOrderNoEqualityCut, JoinOrderEqualityCutId, JoinOrderPropertyUnsupportedReason>;

struct JoinOrderUniquenessNotProven
{
    bool operator==(const JoinOrderUniquenessNotProven &) const = default;
};

struct JoinOrderUniquenessProof
{
    JoinOrderPropertyProofId proof;
    /// Equality classes whose intra-group links this proof relied on although no predicate
    /// chain inside the group justifies them (one bit per provider class index). Such a link
    /// holds only if the selected plan synthesizes the class's predicates at or below the
    /// join that produced the group. The ledger is always exact: a proof that would need an
    /// obligation on a class with index >= 64 fails closed with `UnrepresentableObligation`.
    UInt64 obligation_classes = 0;
    bool operator==(const JoinOrderUniquenessProof &) const = default;
};

/// Exactly one state is representable: an ordinary not-proven answer, proven uniqueness
/// with its obligations, or a typed unsupported condition (fails closed).
using JoinOrderUniquenessResult = std::variant<JoinOrderUniquenessNotProven, JoinOrderUniquenessProof, JoinOrderPropertyUnsupportedReason>;

enum class JoinOrderNoCardinalityCapReason : UInt8
{
    Disabled,
    MissingInputRows,
    NoEqualityCut,
    NotProven,
};

struct JoinOrderCardinalityCapProof
{
    UInt64 upper_bound = 0;
    JoinOrderPropertyProofId proof;
    UInt64 obligation_classes = 0;
    bool operator==(const JoinOrderCardinalityCapProof &) const = default;
};

/// Exactly one state is representable: a proven cap, an ordinary reason why no cap exists,
/// or a typed unsupported condition. Unsupported conditions always fail closed.
using JoinOrderCardinalityCap
    = std::variant<JoinOrderNoCardinalityCapReason, JoinOrderCardinalityCapProof, JoinOrderPropertyUnsupportedReason>;

struct JoinOrderResidualPredicateBinding
{
    bool operator==(const JoinOrderResidualPredicateBinding &) const = default;
};

struct JoinOrderOrdinaryEqualityBinding
{
    JoinOrderColumnId lhs;
    JoinOrderColumnId rhs;
    /// The equality class contains a member pair that cannot be physically equated.
    /// A proof that would need one of those links synthesized must fail closed.
    bool members_incomparable = false;
    bool operator==(const JoinOrderOrdinaryEqualityBinding &) const = default;
};

/// Exactly one predicate classification is representable: a residual predicate, a bound
/// ordinary equality that always carries both source-qualified catalog columns, or the
/// typed reason (e.g. `NullSafeEquality`, `AmbiguousEqualityBinding`,
/// `UnsupportedEqualityType`) why the predicate rejects the whole canonical region.
using JoinOrderPredicatePropertyBinding
    = std::variant<JoinOrderResidualPredicateBinding, JoinOrderOrdinaryEqualityBinding, JoinOrderPropertyUnsupportedReason>;

/// Immutable predicate input snapshotted before enumeration mutates/applies graph edges.
struct JoinOrderCanonicalPredicate
{
    UInt32 stable_id = 0;
    BitSet applicability;
    bool deterministic = true;
    JoinOrderPredicatePropertyBinding binding;
};

struct JoinOrderCanonicalMetrics
{
    UInt64 groups = 0;
    UInt64 retained_subset_payload_members = 0;
    UInt64 retained_subset_payload_bytes = 0;
    UInt64 generic_subset_scratch_capacity_changes = 0;
    UInt64 generic_subset_scratch_uses = 0;
    UInt64 retained_expanded_predicate_closure_members = 0;
    UInt64 retained_expanded_output_contract_members = 0;
    UInt64 demands = 0;
    UInt64 cuts = 0;
    UInt64 cut_cache_hits = 0;
    UInt64 cut_cache_misses = 0;
    UInt64 negative_cut_cache_hits = 0;
    UInt64 cut_scratch_initializations = 0;
    UInt64 cut_scratch_capacity_changes = 0;
    UInt64 cut_scratch_uses = 0;
    UInt64 cache_hits = 0;
    UInt64 cache_misses = 0;
    UInt64 uniqueness_scratch_initializations = 0;
    UInt64 uniqueness_scratch_capacity_changes = 0;
    UInt64 uniqueness_scratch_uses = 0;
    UInt64 equality_members_visited = 0;
    UInt64 key_checks = 0;
    UInt64 key_firings = 0;
    UInt64 maximum_closure_size = 0;
    UInt64 proofs = 0;
};

struct JoinOrderLogicalGroupDescription
{
    JoinOrderSemanticRegionId region;
    BitSet subset;
    JoinOrderPredicateClosureId predicate_closure;
    JoinOrderOutputContractId output_contract;
};

/// One query-local provider. Catalog facts, predicate inputs, semantic eligibility,
/// and output contract are immutable; exact group, demand, and answer caches are logically
/// mutable and cannot influence an answer. Opaque proof handles are provider-local
/// attestations, not reconstructable proof DAG records. The provider owns reusable mutable
/// scratch and unsynchronized caches, so it is intentionally not thread-safe.
class JoinOrderCanonicalProperties
{
public:
    JoinOrderCanonicalProperties(
        std::shared_ptr<const JoinOrderDataPropertyCatalog> catalog,
        size_t relation_count,
        std::vector<JoinOrderCanonicalPredicate> predicates,
        std::optional<JoinOrderPropertyUnsupportedReason> region_rejection = {});
    ~JoinOrderCanonicalProperties();

    JoinOrderCanonicalProperties(JoinOrderCanonicalProperties &&) noexcept;
    JoinOrderCanonicalProperties & operator=(JoinOrderCanonicalProperties &&) noexcept;
    JoinOrderCanonicalProperties(const JoinOrderCanonicalProperties &) = delete;
    JoinOrderCanonicalProperties & operator=(const JoinOrderCanonicalProperties &) = delete;

    JoinOrderGroupLookupResult getGroup(const BitSet & subset) const;
    JoinOrderGroupLookupResult getGroup(UInt32 native_subset) const;

    JoinOrderColumnSetLookupResult internColumnSet(std::span<const JoinOrderColumnId> columns) const;

    JoinOrderEqualityCutResult getEqualityCut(JoinOrderLogicalGroupId left, JoinOrderLogicalGroupId right) const;

    JoinOrderUniquenessResult isUniqueOn(JoinOrderLogicalGroupId group, JoinOrderColumnSetId columns) const;
    JoinOrderCardinalityCap inferCardinalityCapForCut(
        JoinOrderLogicalGroupId left, JoinOrderLogicalGroupId right, JoinOrderEqualityCutId cut, UInt64 left_rows, UInt64 right_rows) const;

    /// Members of the equality class with the given index (indices match the
    /// `obligation_classes` bits of proofs and caps); empty when out of range.
    std::span<const JoinOrderColumnId> equalityClassMembers(size_t class_index) const;
    size_t equalityClassCount() const;

    /// Resolve both groups and their equality cut, then infer the INNER ALL join cap.
    /// This is the call-site API; lower-level group/cut operations remain available for
    /// diagnostics and focused tests.
    JoinOrderCardinalityCap inferInnerAllCardinalityCap(
        const BitSet & left_subset, const BitSet & right_subset, std::optional<UInt64> left_rows, std::optional<UInt64> right_rows) const;
    JoinOrderCardinalityCap inferInnerAllCardinalityCap(
        UInt32 left_subset, UInt32 right_subset, std::optional<UInt64> left_rows, std::optional<UInt64> right_rows) const;

    std::optional<JoinOrderPropertyUnsupportedReason> regionUnsupportedReason() const;
    JoinOrderLogicalGroupDescription describeGroup(JoinOrderLogicalGroupId group) const;
    JoinOrderCanonicalMetrics getMetrics() const;
    String dumpGroup(JoinOrderLogicalGroupId group) const;
    String dumpMetrics() const;

private:
    JoinOrderCardinalityCap inferInnerAllCardinalityCap(
        JoinOrderGroupLookupResult left, JoinOrderGroupLookupResult right, UInt64 left_rows, UInt64 right_rows) const;
    struct Impl;
    std::unique_ptr<Impl> impl;
};

String joinOrderPropertyUnsupportedReasonToString(JoinOrderPropertyUnsupportedReason reason);

/// Uniform accessors for the flat-variant answers above. The pointer-returning accessors
/// alias the argument, which therefore must outlive the returned pointer.
inline const JoinOrderCardinalityCapProof * getProvenCap(const JoinOrderCardinalityCap & cap)
{
    return std::get_if<JoinOrderCardinalityCapProof>(&cap);
}

inline const JoinOrderUniquenessProof * getUniquenessProof(const JoinOrderUniquenessResult & result)
{
    return std::get_if<JoinOrderUniquenessProof>(&result);
}

inline std::optional<JoinOrderEqualityCutId> getEqualityCutId(const JoinOrderEqualityCutResult & result)
{
    if (const auto * cut = std::get_if<JoinOrderEqualityCutId>(&result))
        return *cut;
    return std::nullopt;
}

template <typename... Alternatives>
const JoinOrderPropertyUnsupportedReason * getUnsupportedReason(const std::variant<Alternatives...> & result)
{
    return std::get_if<JoinOrderPropertyUnsupportedReason>(&result);
}

}
