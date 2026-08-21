#pragma once

#include <base/types.h>

#include <cstddef>
#include <optional>
#include <span>
#include <vector>

namespace DB::QueryPlanOptimizations
{

/// Identifies a column in the output header of the current plan step.
/// `position` is the identity used by derivation. `name` is retained only for
/// metadata matching, validation, and diagnostics and does not affect identity.
///
/// Equality and hashing are therefore position-only and meaningful only within one step's
/// implicit output-header scope: schema-changing steps reconstruct and renumber references
/// (or drop the properties entirely), so references from unrelated plan nodes must never be
/// compared or combined directly. If cross-scope references ever become necessary, an
/// explicit scope identifier is required; including `name` in identity would not fix that.
struct PlanColumnRef
{
    size_t position = 0;
    String name;

    bool operator==(const PlanColumnRef & other) const { return position == other.position; }
};

struct PlanColumnRefHash
{
    size_t operator()(const PlanColumnRef & column) const;
};

/// Identifies a column consumed from one of the current step's children.
/// Identity is `(child_index, position)`; `name` is validation/display metadata.
struct InputColumnRef
{
    size_t child_index = 0;
    size_t position = 0;
    String name;

    bool operator==(const InputColumnRef & other) const { return child_index == other.child_index && position == other.position; }
};

using ColumnSet = std::vector<PlanColumnRef>;

/// Sort a set by output position and coalesce exact identities.
/// Returns false when one position is associated with different names.
bool normalizeColumnSet(ColumnSet & columns);

/// Resolve metadata names to current output positions. A missing or duplicated
/// output name makes the mapping ambiguous and causes the whole set to be dropped.
std::optional<ColumnSet> resolveColumnSetByName(std::span<const String> output_names, std::span<const String> column_names);

enum class DataPropertyOrigin : UInt8
{
    Unknown,
    StorageDeclaration,
    AggregationGrouping,
};

enum class DataPropertyConfidence : UInt8
{
    Unknown,
    DiagnosticOnly,
    Proven,
};

enum class DataPropertyTransformationKind : UInt8
{
    Identity = 1,
    ValuePreservingExpression,
    NDVBoundExpression,
    FilterSubset,
    JoinPreservation,
};

/// Transformations which preserve exact facts such as unique keys and functional
/// dependencies. Keeping this as a strict subset prevents an NDV-only mapping from
/// accidentally retaining an exact fact with Proven confidence.
enum class DataPropertyPreservingTransformationKind : UInt8
{
    Identity = static_cast<UInt8>(DataPropertyTransformationKind::Identity),
    ValuePreservingExpression = static_cast<UInt8>(DataPropertyTransformationKind::ValuePreservingExpression),
    FilterSubset = static_cast<UInt8>(DataPropertyTransformationKind::FilterSubset),
    JoinPreservation = static_cast<UInt8>(DataPropertyTransformationKind::JoinPreservation),
};

constexpr DataPropertyTransformationKind toDataPropertyTransformationKind(DataPropertyPreservingTransformationKind kind)
{
    return static_cast<DataPropertyTransformationKind>(kind);
}

enum class DataPropertyEqualityMode : UInt8
{
    NonNullOrdinaryEquality,
    NullsEqual,
    Unsupported,
};

enum class DataPropertyDependencyKind : UInt8
{
    Exact,
    Statistical,
};

/// A compact transformation sequence. Four bits encode the length and each
/// following nibble encodes one transformation.
struct DataPropertyTransformationHistory
{
    UInt64 value = 0;
    auto operator<=>(const DataPropertyTransformationHistory &) const = default;
};

/// Evidence for a fact. Equality/null and dependency semantics live on the fact
/// itself rather than being mixed into provenance.
struct DataPropertyProvenance
{
    DataPropertyOrigin origin = DataPropertyOrigin::Unknown;
    DataPropertyConfidence confidence = DataPropertyConfidence::Unknown;
    DataPropertyTransformationHistory history;

    static DataPropertyProvenance storageDeclaration();
    static DataPropertyProvenance aggregationGrouping();
    static DataPropertyProvenance transformation(DataPropertyTransformationKind kind);

    DataPropertyProvenance transformed(DataPropertyTransformationKind kind) const;

    bool operator==(const DataPropertyProvenance &) const = default;
};

enum class ColumnLineageKind : UInt8
{
    Identity,
    ValuePreserving,
    NDVBound,
    Computed,
    Unknown,
};

struct UniqueKeyFact
{
    ColumnSet columns;
    DataPropertyProvenance provenance;
    DataPropertyEqualityMode equality_mode = DataPropertyEqualityMode::Unsupported;

    static UniqueKeyFact fromStorageDeclaration(ColumnSet columns);
    static UniqueKeyFact fromAggregationGrouping(ColumnSet columns);

    /// Preserve this fact while remapping its columns through a transformation
    /// whose type guarantees that exact facts remain valid.
    UniqueKeyFact remap(ColumnSet mapped_columns, DataPropertyPreservingTransformationKind transformation) const;

    bool operator==(const UniqueKeyFact &) const = default;
};

struct FunctionalDependencyFact
{
    ColumnSet determinant;
    ColumnSet dependents;
    DataPropertyDependencyKind kind = DataPropertyDependencyKind::Exact;
    DataPropertyProvenance provenance;

    /// Preserve this fact while remapping both sides through a transformation
    /// whose type guarantees that exact facts remain valid.
    FunctionalDependencyFact
    remap(ColumnSet mapped_determinant, ColumnSet mapped_dependents, DataPropertyPreservingTransformationKind transformation) const;

    bool operator==(const FunctionalDependencyFact &) const = default;
};

struct ColumnLineageFact
{
    PlanColumnRef output;
    InputColumnRef input;
    ColumnLineageKind kind = ColumnLineageKind::Unknown;
    DataPropertyProvenance provenance;

    bool operator==(const ColumnLineageFact &) const = default;
};

using UniqueKeyFacts = std::vector<UniqueKeyFact>;
using FunctionalDependencyFacts = std::vector<FunctionalDependencyFact>;
using ColumnLineageFacts = std::vector<ColumnLineageFact>;

/// Normalized optimizer interchange summary. Fact collections are private so
/// callers cannot invalidate the sorted/deduplicated representation required by
/// the insertion methods.
class DataPropertySet
{
public:
    bool empty() const;

    bool addUniqueKey(UniqueKeyFact fact);
    bool addFunctionalDependency(FunctionalDependencyFact fact);
    bool addNonNullColumn(PlanColumnRef column);
    bool addLineage(ColumnLineageFact fact);

    const UniqueKeyFacts & uniqueKeys() const { return unique_keys; }
    const FunctionalDependencyFacts & functionalDependencies() const { return functional_dependencies; }
    const ColumnSet & nonNullColumns() const { return non_null_columns; }
    const ColumnLineageFacts & columnLineage() const { return lineage; }

    String dump() const;

    bool operator==(const DataPropertySet &) const = default;

private:
    UniqueKeyFacts unique_keys;
    FunctionalDependencyFacts functional_dependencies;
    ColumnSet non_null_columns;
    ColumnLineageFacts lineage;
};

bool isProvenStrongBagKey(const DataPropertyProvenance & provenance, DataPropertyEqualityMode equality_mode);
inline bool isProvenStrongBagKey(const UniqueKeyFact & fact)
{
    return isProvenStrongBagKey(fact.provenance, fact.equality_mode);
}

String dataPropertyOriginToString(DataPropertyOrigin origin);
String dataPropertyConfidenceToString(DataPropertyConfidence confidence);
String dataPropertyEqualityModeToString(DataPropertyEqualityMode mode);
String dataPropertyProvenanceToString(const DataPropertyProvenance & provenance);
String columnLineageKindToString(ColumnLineageKind kind);
String dumpColumnSet(const ColumnSet & columns);

}
