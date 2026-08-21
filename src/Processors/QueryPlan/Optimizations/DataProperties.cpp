#include <Processors/QueryPlan/Optimizations/DataProperties.h>

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <algorithm>
#include <ranges>
#include <unordered_map>

namespace DB::QueryPlanOptimizations
{
namespace
{

bool columnSetLess(const ColumnSet & lhs, const ColumnSet & rhs)
{
    return std::lexicographical_compare(
        lhs.begin(),
        lhs.end(),
        rhs.begin(),
        rhs.end(),
        [](const auto & left, const auto & right) { return left.position < right.position; });
}

template <typename T, typename Less>
bool addIfMissing(std::vector<T> & facts, T fact, Less less)
{
    /// The vector is kept sorted by `less`, which orders over every identity field
    /// compared by `operator==`, so an existing fact can only sit at the lower bound.
    const auto position = std::ranges::lower_bound(facts, fact, less);
    if (position != facts.end() && *position == fact)
        return false;
    facts.insert(position, std::move(fact));
    return true;
}

}

size_t PlanColumnRefHash::operator()(const PlanColumnRef & column) const
{
    return std::hash<size_t>{}(column.position);
}

DataPropertyProvenance DataPropertyProvenance::storageDeclaration()
{
    return {.origin = DataPropertyOrigin::StorageDeclaration, .confidence = DataPropertyConfidence::DiagnosticOnly, .history = {}};
}

DataPropertyProvenance DataPropertyProvenance::aggregationGrouping()
{
    return {.origin = DataPropertyOrigin::AggregationGrouping, .confidence = DataPropertyConfidence::Proven, .history = {}};
}

DataPropertyProvenance DataPropertyProvenance::transformation(DataPropertyTransformationKind kind)
{
    return DataPropertyProvenance{}.transformed(kind);
}

DataPropertyProvenance DataPropertyProvenance::transformed(DataPropertyTransformationKind transformation) const
{
    DataPropertyProvenance result = *this;
    constexpr UInt64 length_mask = 0xF;
    constexpr UInt64 max_length = 15;
    const UInt64 length = result.history.value & length_mask;
    if (length >= max_length)
    {
        /// Failing closed is preferable to truncating transformation history and
        /// accidentally presenting incomplete evidence as trusted.
        result.confidence = DataPropertyConfidence::Unknown;
        return result;
    }

    const UInt64 encoded = static_cast<UInt64>(transformation);
    result.history.value |= encoded << (4 + length * 4);
    result.history.value = (result.history.value & ~length_mask) | (length + 1);
    return result;
}

UniqueKeyFact UniqueKeyFact::fromStorageDeclaration(ColumnSet columns)
{
    return {
        .columns = std::move(columns),
        .provenance = DataPropertyProvenance::storageDeclaration(),
        .equality_mode = DataPropertyEqualityMode::NonNullOrdinaryEquality};
}

UniqueKeyFact UniqueKeyFact::fromAggregationGrouping(ColumnSet columns)
{
    return {
        .columns = std::move(columns),
        .provenance = DataPropertyProvenance::aggregationGrouping(),
        .equality_mode = DataPropertyEqualityMode::NonNullOrdinaryEquality};
}

UniqueKeyFact UniqueKeyFact::remap(ColumnSet mapped_columns, DataPropertyPreservingTransformationKind transformation) const
{
    return {
        .columns = std::move(mapped_columns),
        .provenance = provenance.transformed(toDataPropertyTransformationKind(transformation)),
        .equality_mode = equality_mode};
}

FunctionalDependencyFact FunctionalDependencyFact::remap(
    ColumnSet mapped_determinant, ColumnSet mapped_dependents, DataPropertyPreservingTransformationKind transformation) const
{
    return {
        std::move(mapped_determinant),
        std::move(mapped_dependents),
        kind,
        provenance.transformed(toDataPropertyTransformationKind(transformation))};
}

bool isProvenStrongBagKey(const DataPropertyProvenance & provenance, DataPropertyEqualityMode equality_mode)
{
    /// Phase one has exactly one producer of strong bag keys: final aggregation
    /// grouping. Storage declarations are diagnostic-only and transformations
    /// preserve, but cannot create, a trusted origin.
    return provenance.confidence == DataPropertyConfidence::Proven && provenance.origin == DataPropertyOrigin::AggregationGrouping
        && equality_mode == DataPropertyEqualityMode::NonNullOrdinaryEquality;
}

bool normalizeColumnSet(ColumnSet & columns)
{
    std::ranges::sort(columns, {}, &PlanColumnRef::position);

    for (size_t index = 1; index < columns.size(); ++index)
    {
        if (columns[index - 1].position == columns[index].position && columns[index - 1].name != columns[index].name)
            return false;
    }

    columns.erase(std::unique(columns.begin(), columns.end()), columns.end());
    return true;
}

std::optional<ColumnSet> resolveColumnSetByName(std::span<const String> output_names, std::span<const String> column_names)
{
    std::unordered_map<String, size_t> unique_positions;
    std::unordered_map<String, size_t> occurrences;
    for (size_t position = 0; position < output_names.size(); ++position)
    {
        unique_positions[output_names[position]] = position;
        ++occurrences[output_names[position]];
    }

    ColumnSet result;
    result.reserve(column_names.size());
    for (const auto & name : column_names)
    {
        auto occurrence = occurrences.find(name);
        if (occurrence == occurrences.end() || occurrence->second != 1)
            return std::nullopt;
        result.push_back({unique_positions.at(name), name});
    }

    if (!normalizeColumnSet(result) || result.empty())
        return std::nullopt;
    return result;
}

bool DataPropertySet::empty() const
{
    return unique_keys.empty() && functional_dependencies.empty() && non_null_columns.empty() && lineage.empty();
}

bool DataPropertySet::addUniqueKey(UniqueKeyFact fact)
{
    if (!normalizeColumnSet(fact.columns) || fact.columns.empty())
        return false;
    return addIfMissing(
        unique_keys,
        std::move(fact),
        [](const auto & lhs, const auto & rhs)
        {
            if (lhs.columns != rhs.columns)
                return columnSetLess(lhs.columns, rhs.columns);
            return std::tie(lhs.provenance.origin, lhs.provenance.confidence, lhs.provenance.history.value, lhs.equality_mode)
                < std::tie(rhs.provenance.origin, rhs.provenance.confidence, rhs.provenance.history.value, rhs.equality_mode);
        });
}

bool DataPropertySet::addFunctionalDependency(FunctionalDependencyFact fact)
{
    if (!normalizeColumnSet(fact.determinant) || !normalizeColumnSet(fact.dependents) || fact.determinant.empty()
        || fact.dependents.empty())
        return false;
    return addIfMissing(
        functional_dependencies,
        std::move(fact),
        [](const auto & lhs, const auto & rhs)
        {
            if (lhs.determinant != rhs.determinant)
                return columnSetLess(lhs.determinant, rhs.determinant);
            if (lhs.dependents != rhs.dependents)
                return columnSetLess(lhs.dependents, rhs.dependents);
            return std::tie(lhs.kind, lhs.provenance.origin, lhs.provenance.confidence, lhs.provenance.history.value)
                < std::tie(rhs.kind, rhs.provenance.origin, rhs.provenance.confidence, rhs.provenance.history.value);
        });
}

bool DataPropertySet::addNonNullColumn(PlanColumnRef column)
{
    const auto position = std::ranges::lower_bound(non_null_columns, column.position, {}, &PlanColumnRef::position);
    if (position != non_null_columns.end() && position->position == column.position)
        return false;
    non_null_columns.insert(position, std::move(column));
    return true;
}

bool DataPropertySet::addLineage(ColumnLineageFact fact)
{
    return addIfMissing(
        lineage,
        std::move(fact),
        [](const auto & lhs, const auto & rhs)
        {
            return std::tie(
                       lhs.output.position,
                       lhs.input.child_index,
                       lhs.input.position,
                       lhs.kind,
                       lhs.provenance.origin,
                       lhs.provenance.confidence,
                       lhs.provenance.history.value)
                < std::tie(
                       rhs.output.position,
                       rhs.input.child_index,
                       rhs.input.position,
                       rhs.kind,
                       rhs.provenance.origin,
                       rhs.provenance.confidence,
                       rhs.provenance.history.value);
        });
}

String dataPropertyOriginToString(DataPropertyOrigin origin)
{
    switch (origin)
    {
        case DataPropertyOrigin::Unknown: return "unknown";
        case DataPropertyOrigin::StorageDeclaration: return "storage-declaration";
        case DataPropertyOrigin::AggregationGrouping: return "aggregation-grouping";
    }
    return "unknown";
}

String dataPropertyConfidenceToString(DataPropertyConfidence confidence)
{
    switch (confidence)
    {
        case DataPropertyConfidence::Unknown: return "unknown";
        case DataPropertyConfidence::DiagnosticOnly: return "diagnostic-only";
        case DataPropertyConfidence::Proven: return "proven";
    }
    return "unknown";
}

String dataPropertyEqualityModeToString(DataPropertyEqualityMode mode)
{
    switch (mode)
    {
        case DataPropertyEqualityMode::NonNullOrdinaryEquality: return "non-null-ordinary";
        case DataPropertyEqualityMode::NullsEqual: return "nulls-equal";
        case DataPropertyEqualityMode::Unsupported: return "unsupported";
    }
    return "unsupported";
}

static String dataPropertyTransformationToString(DataPropertyTransformationKind transformation)
{
    switch (transformation)
    {
        case DataPropertyTransformationKind::Identity: return "identity";
        case DataPropertyTransformationKind::ValuePreservingExpression: return "value-preserving-expression";
        case DataPropertyTransformationKind::NDVBoundExpression: return "ndv-bound-expression";
        case DataPropertyTransformationKind::FilterSubset: return "filter-subset";
        case DataPropertyTransformationKind::JoinPreservation: return "join-preservation";
    }
    return "unknown";
}

static String dataPropertyHistoryToString(DataPropertyTransformationHistory history)
{
    const size_t length = history.value & 0xF;
    std::vector<String> transformations;
    transformations.reserve(length);
    for (size_t index = 0; index < length; ++index)
    {
        const auto encoded = static_cast<DataPropertyTransformationKind>((history.value >> (4 + index * 4)) & 0xF);
        transformations.push_back(dataPropertyTransformationToString(encoded));
    }
    return fmt::format("[{}]", fmt::join(transformations, ","));
}

String dataPropertyProvenanceToString(const DataPropertyProvenance & provenance)
{
    return fmt::format(
        "origin={}, confidence={}, transformations={}",
        dataPropertyOriginToString(provenance.origin),
        dataPropertyConfidenceToString(provenance.confidence),
        dataPropertyHistoryToString(provenance.history));
}

String columnLineageKindToString(ColumnLineageKind kind)
{
    switch (kind)
    {
        case ColumnLineageKind::Identity: return "identity";
        case ColumnLineageKind::ValuePreserving: return "value-preserving";
        case ColumnLineageKind::NDVBound: return "ndv-bound";
        case ColumnLineageKind::Computed: return "computed";
        case ColumnLineageKind::Unknown: return "unknown";
    }
    return "unknown";
}

String dumpColumnSet(const ColumnSet & columns)
{
    return fmt::format(
        "[{}]",
        fmt::join(
            columns | std::views::transform([](const auto & column) { return fmt::format("{}:{}", column.position, column.name); }), ", "));
}

String DataPropertySet::dump() const
{
    auto unique_key_strings = unique_keys
        | std::views::transform(
                                  [](const auto & fact)
                                  {
                                      return fmt::format(
                                          "{} ({}; equality={})",
                                          dumpColumnSet(fact.columns),
                                          dataPropertyProvenanceToString(fact.provenance),
                                          dataPropertyEqualityModeToString(fact.equality_mode));
                                  });
    auto fd_strings = functional_dependencies
        | std::views::transform(
                          [](const auto & fact)
                          {
                              return fmt::format(
                                  "{} -> {} ({}; kind={})",
                                  dumpColumnSet(fact.determinant),
                                  dumpColumnSet(fact.dependents),
                                  dataPropertyProvenanceToString(fact.provenance),
                                  fact.kind == DataPropertyDependencyKind::Exact ? "exact" : "statistical");
                          });
    auto non_null_strings
        = non_null_columns | std::views::transform([](const auto & column) { return fmt::format("{}:{}", column.position, column.name); });
    auto lineage_strings = lineage
        | std::views::transform(
                               [](const auto & fact)
                               {
                                   return fmt::format(
                                       "{}:{} <- child {} {}:{} ({})",
                                       fact.output.position,
                                       fact.output.name,
                                       fact.input.child_index,
                                       fact.input.position,
                                       fact.input.name,
                                       columnLineageKindToString(fact.kind));
                               });

    return fmt::format(
        "unique_keys=[{}], fds=[{}], non_null=[{}], lineage=[{}]",
        fmt::join(unique_key_strings, ", "),
        fmt::join(fd_strings, ", "),
        fmt::join(non_null_strings, ", "),
        fmt::join(lineage_strings, ", "));
}

}
