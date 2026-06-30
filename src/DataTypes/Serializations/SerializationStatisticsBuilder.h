#pragma once

#include <Core/Names.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <DataTypes/Serializations/SerializationInfoSettings.h>
#include <DataTypes/Serializations/SerializationStatistics.h>

#include <map>
#include <memory>
#include <vector>

namespace DB
{

class IColumn;
class IDataType;
class Block;
class NamesAndTypesList;
class SerializationInfo;
class SerializationInfoByName;

/** Builds the lightweight, always-available statistics (currently the number of rows and the
  * sampled number of default values) that are used to choose the serialization kind of a column
  * (currently sparse, in the future also `LowCardinality`).
  *
  * These statistics used to be built and updated inside `SerializationInfo` itself; they have been
  * extracted here so that `SerializationInfo` only carries the chosen kind (and a passive copy of
  * the counts for persistence). A builder is created on every insert, merge and mutation,
  * independently of the `materialize_statistics_*` settings and of the merge type. It mirrors the
  * tuple tree so the kind is still chosen independently for each tuple element.
  *
  * The builder only tracks columns that can use sparse serialization (the same columns that get an
  * entry in `SerializationInfoByName`). Columns whose default count is taken from external
  * statistics are still tracked here as a fallback; whether their inline counts are persisted is
  * decided separately at write time (see `MergedBlockOutputStream`).
  */
class SerializationStatisticsBuilder
{
public:
    SerializationStatisticsBuilder(const NamesAndTypesList & columns, const SerializationInfoSettings & settings);

    /// Sample statistics from the (tracked) columns of a block.
    void add(const Block & block);
    /// Account for `length` rows of a column that is entirely default (e.g. a column missing from a
    /// source part during a merge). No-op for columns that are not tracked.
    void addDefaults(const String & name, size_t length);
    /// Combine the persisted counts of a source part's serialization infos (additive). Used during
    /// merges to choose the output kind from the summed counts of all source parts.
    void add(const SerializationInfoByName & infos);

    /// Choose the serialization kind for each tracked column (and tuple element) from the
    /// accumulated counts and write the kind stacks into `infos`. Used before writing a part.
    void chooseKinds(SerializationInfoByName & infos) const;
    /// Write the accumulated counts into `infos` (keeping the already-chosen kinds). Used at
    /// finalization to persist the counts of the actually-written data.
    void applyStatistics(SerializationInfoByName & infos) const;

    static ISerialization::KindStack chooseKindStack(const SerializationStatistics & statistics, const SerializationInfoSettings & settings);

    /// Maintain a running aggregate of serialization statistics directly inside a long-lived
    /// `SerializationInfo` (the per-table serialization hints), whose counts are carried by the infos
    /// themselves rather than by a builder node tree. `addStatistics` adds `src`'s counts into `dst`
    /// (recursing tuple elements; kinds untouched); `chooseKindsFromStatistics` re-chooses the kind of
    /// `info` (and its tuple elements) from its own carried counts. The aggregate is only ever added
    /// to — removing a part's contribution is handled by recomputing the hints from the active parts.
    static void addStatistics(SerializationInfo & dst, const SerializationInfo & src);
    static void chooseKindsFromStatistics(SerializationInfo & info);

private:
    /// Recursive node mirroring a column's type tree: a leaf for ordinary columns, with `elements`
    /// for the elements of a `Tuple` (so each element gets its own statistics and kind decision).
    /// `element_names` is parallel to `elements` and is used to match the elements of a source
    /// part's `SerializationInfoTuple` by name (as the former per-element building did).
    struct Node
    {
        SerializationStatistics statistics;
        std::vector<std::shared_ptr<Node>> elements;
        Names element_names;
    };

    using NodePtr = std::shared_ptr<Node>;

    static NodePtr createNode(const IDataType & type);
    static void addColumn(Node & node, const IColumn & column);
    static void addDefaultsToNode(Node & node, size_t length);
    static void addInfo(Node & node, const SerializationInfo & info);

    void chooseKindsImpl(const Node & node, SerializationInfo & info) const;
    void applyStatisticsImpl(const Node & node, SerializationInfo & info) const;

    std::map<String, NodePtr> nodes;
    SerializationInfoSettings settings;
};

}
