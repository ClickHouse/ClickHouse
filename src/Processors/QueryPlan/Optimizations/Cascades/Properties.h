#pragma once

#include <Core/Names.h>
#include <Core/SortDescription.h>
#include <Columns/Collator.h>
#include <base/types.h>
#include <vector>
#include <functional>
#include <boost/functional/hash.hpp>

namespace DB
{

/// A set of columns, but each column can also have multiple equivalent names derived from equality predicates
using DistributionColumns = std::vector<NameSet>;

struct DistributionDescription
{
    DistributionColumns columns;    /// Columns by which data is distributed. E.g. for shuffle exchange or partitioned table scan.
    /// Type each key is cast to before hashing (parallel to `columns`; empty when the raw
    /// types agree). Aligns buckets across both sides of a shuffle join.
    Names hash_type_names;
    bool is_replicated = false;     /// All data is replicated to all nodes, so any distribution is satisfied. E.g. for small tables that are broadcasted to all nodes.
    size_t node_count = 1;          /// Number of nodes among which data is distributed. E.g. for a shuffle exchange or a partitioned read.

    bool operator==(const DistributionDescription & other) const = default;

    void dump(WriteBuffer & out) const;
    String dump() const;
};

/// How one node's streams relate to each other. `sorting` always describes each single
/// stream; the layout says what crossing a stream boundary means.
enum class StreamLayout : uint8_t
{
    Unknown,    /// several streams with no relation (reads, joins, exchange receive sides)
    Single,     /// one stream per node
    Disjoint,   /// streams disjoint on `stream_disjoint_columns`: rows equal on them share a stream
};

struct ExpressionProperties
{
    SortDescription sorting;
    DistributionDescription distribution;
    StreamLayout stream_layout = StreamLayout::Unknown;
    DistributionColumns stream_disjoint_columns;    /// Nonempty exactly for `Disjoint`; set via `setDisjointStreams`

    /// Marks the streams disjoint on the columns (must be nonempty sets). Orders the column
    /// sets canonically, so equal layouts compare and hash equal (disjointness has no key order).
    void setDisjointStreams(DistributionColumns columns);

    bool operator==(const ExpressionProperties & other) const = default;

    bool isSatisfiedBy(const ExpressionProperties & existing_properties) const;

    static bool isSortingSatisfiedBy(const SortDescription & required, const SortDescription & existing);
    static bool isDistributionSatisfiedBy(const DistributionDescription & required, const DistributionDescription & existing);
    static bool isStreamLayoutSatisfiedBy(const ExpressionProperties & required, const ExpressionProperties & existing);

    void dump(WriteBuffer & out) const;
    String dump() const;
};

struct ExpressionPropertiesHash
{
    size_t operator()(const ExpressionProperties & props) const
    {
        size_t h = std::hash<size_t>()(props.distribution.node_count);
        boost::hash_combine(h, props.distribution.is_replicated);
        for (const auto & col_set : props.distribution.columns)
        {
            /// Equal sets must hash equally regardless of insertion order.
            size_t set_hash = 0;
            for (const auto & name : col_set)
                set_hash += std::hash<String>()(name);
            boost::hash_combine(h, set_hash);
        }
        for (const auto & type_name : props.distribution.hash_type_names)
            boost::hash_combine(h, type_name);
        boost::hash_combine(h, static_cast<UInt8>(props.stream_layout));
        for (const auto & col_set : props.stream_disjoint_columns)
        {
            /// Equal sets must hash equally regardless of insertion order.
            size_t set_hash = 0;
            for (const auto & name : col_set)
                set_hash += std::hash<String>()(name);
            boost::hash_combine(h, set_hash);
        }
        /// Hash exactly the fields SortColumnDescription::operator== compares, so the hash is
        /// consistent with equality (`ORDER BY k ASC` and k DESC must hash differently).
        for (const auto & col : props.sorting)
        {
            boost::hash_combine(h, col.column_name);
            boost::hash_combine(h, col.direction);
            boost::hash_combine(h, col.nulls_direction);
            if (col.collator)
                boost::hash_combine(h, col.collator->getLocale());
        }
        return h;
    }
};

/// Composite key for tracking which (rule, properties) pairs have been applied.
struct RulePropertiesKey
{
    const void * rule_ptr = nullptr;  /// Address of the rule (stable for the optimizer's lifetime)
    ExpressionProperties properties;

    bool operator==(const RulePropertiesKey & other) const = default;
};

struct RulePropertiesKeyHash
{
    size_t operator()(const RulePropertiesKey & key) const
    {
        size_t h = std::hash<const void *>()(key.rule_ptr);
        boost::hash_combine(h, ExpressionPropertiesHash()(key.properties));
        return h;
    }
};

}
