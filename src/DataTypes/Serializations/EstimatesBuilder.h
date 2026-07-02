#pragma once

#include <Core/Names.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <Storages/Statistics/Estimate.h>

#include <map>

namespace DB
{

class IColumn;
class IDataType;
class Block;
class NamesAndTypesList;
class SerializationInfo;
class SerializationInfoByName;

/** Builds the lightweight, always-available estimates (currently the number of rows and the number of
  * default values) that are used to choose the serialization kind of a column (currently sparse, in the
  * future also `LowCardinality`).
  *
  * The counts are carried by the shared `Estimate` type (see `Storages/Statistics/Estimate.h`), the same
  * type produced by the explicit column statistics. A builder is created on every insert, merge and
  * mutation, independently of the `materialize_statistics_*` settings and of the merge type. It samples
  * the default counts from the data — only for the columns whose counts the explicit statistics do not
  * already provide (the constructor takes them) — and can be reconciled with the exact counts from the
  * statistics afterwards (`mergeEstimates`); the resulting estimates are then used to choose the kinds
  * and are written into `serialization.json`.
  *
  * The estimates are stored in a plain ordered map: every column and subcolumn is a separate entry keyed
  * by its subcolumn path. The `'\0'` path joiner (see `subcolumnEstimateKey`) makes a column's entry and
  * the entries of all its subcolumns a contiguous key range, so the accumulation methods scan key ranges
  * instead of walking a tree; the element names of a `Tuple` are taken from the sampled block itself.
  * The builder only tracks columns that can use sparse serialization (the same columns that get an entry
  * in `SerializationInfoByName`).
  */
class EstimatesBuilder
{
public:
    /// A column whose default count is already provided by `external_estimates` (the explicit column
    /// statistics built alongside, see `MergeTreeDataWriter`) is not sampled: its counts are taken
    /// from the statistics as-is and the accumulation methods skip it.
    EstimatesBuilder(const NamesAndTypesList & columns, const SerializationInfoSettings & settings, const Estimates & external_estimates);

    /// Sample estimates from the (tracked) columns of a block.
    void add(const Block & block);
    /// Account for `length` rows of a column that is entirely default (e.g. a column missing from a
    /// source part during a merge). No-op for columns that are not tracked.
    void addDefaults(const String & name, size_t length);
    /// Combine the estimates of a source part (additive). Used during merges to choose the output kind
    /// from the summed counts of all source parts. `part_estimates` is keyed by subcolumn path; a tracked
    /// (sub)column missing from it contributes all-default rows.
    void add(const Estimates & part_estimates);

    /// Override the sampled counts with the exact counts from the explicit column statistics where they
    /// are available (`Estimate::num_defaults`). Only top-level columns have explicit statistics.
    void mergeEstimates(const Estimates & external_estimates);

    /// The same override applied to an arbitrary set of estimates instead of the builder's own.
    static void mergeEstimates(Estimates & estimates, const Estimates & external_estimates);

    /// The accumulated estimates for every tracked column and subcolumn, keyed by subcolumn path.
    Estimates getEstimates() const;

    /// Choose the serialization kind for each tracked column (and tuple element) from the accumulated
    /// estimates and write the kind stacks into `infos`.
    void chooseKinds(SerializationInfoByName & infos) const;

    /// Choose the serialization kinds of `infos` from an arbitrary set of `estimates` (keyed by subcolumn
    /// path). Walks the info tree; a (sub)column with no estimate keeps its current (default) kind. Shared
    /// by the builder and by the per-table serialization hints.
    static void chooseKinds(SerializationInfoByName & infos, const Estimates & estimates);

    static ISerialization::KindStack chooseKindStack(const Estimate & estimate, const SerializationInfoSettings & settings);

    /// Add / subtract the counts of `src` into/from `dst`, treating an absent default count as 0.
    /// Subtraction saturates at 0 instead of wrapping (see the per-table serialization hints).
    static void addCounts(Estimate & dst, const Estimate & src);
    static void subtractCounts(Estimate & dst, const Estimate & src);

    /// Add `src`'s counts into `dst` (per subcolumn path, additive). Used to combine the counts of the
    /// streams of a vertical merge and to maintain the per-table serialization-hint aggregate.
    static void addEstimates(Estimates & dst, const Estimates & src);

    /// Remove the estimates of the columns (and subcolumns) that are not present in `infos`, e.g. of
    /// columns fully expired by a TTL and removed from the part after being written: the persisted file,
    /// the in-memory part and the infos must agree on the set of columns.
    static void filterEstimates(Estimates & estimates, const SerializationInfoByName & infos);

private:
    /// Create zero estimates for the column with key `key` and, recursively, for its tuple subcolumns.
    void addKeys(const String & key, const IDataType & type);
    void sampleColumn(const String & key, const IColumn & column, const IDataType & type);

    static void chooseKindsImpl(const String & key, SerializationInfo & info, const Estimates & estimates);

    /// All tracked columns and their subcolumns, keyed by full subcolumn path (see the class comment
    /// on the key ranges provided by the ordering).
    std::map<String, Estimate> estimates;
    /// Top-level columns whose exact counts were provided to the constructor by the explicit statistics.
    /// The accumulation methods skip them, so the exact counts are never mixed with accumulated ones.
    NameSet columns_with_exact_counts;
    SerializationInfoSettings settings;
};

}
