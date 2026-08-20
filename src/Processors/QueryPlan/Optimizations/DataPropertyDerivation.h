#pragma once

#include <Processors/QueryPlan/Optimizations/DataProperties.h>
#include <Processors/QueryPlan/QueryPlan.h>

#include <Core/Block_fwd.h>
#include <Core/Joins.h>
#include <Core/Names.h>

#include <span>

namespace DB
{

class IQueryPlanStep;
struct StorageInMemoryMetadata;

namespace QueryPlanOptimizations
{

/// Pure helper used by storage-backed source steps and focused tests.
DataPropertySet deriveDataPropertiesForStorageRead(const Block & output_header, const StorageInMemoryMetadata * metadata);

struct AggregationDataPropertyOptions
{
    bool final = false;
    bool has_grouping_sets = false;
    bool has_overflow_row = false;
};

DataPropertySet
deriveDataPropertiesForAggregation(const Block & output_header, const Names & grouping_keys, AggregationDataPropertyOptions options);

struct DataPropertyInputView
{
    const Block & header;
    const DataPropertySet & properties;
};

DataPropertySet deriveDataPropertiesForJoin(
    JoinKind kind, JoinStrictness strictness, const Block & output_header, DataPropertyInputView left, DataPropertyInputView right);

/// Derive properties local to one step without modifying the caller's values.
DataPropertySet deriveDataProperties(const IQueryPlanStep & step, std::span<const DataPropertySet> child_properties);

/// Derive properties through one-shot iterative DAG discovery and evaluation.
/// Results are released after their last ordinary or common-subplan consumer; the
/// traversal neither mutates the plan nor persists derived data in it.
DataPropertySet deriveDataPropertiesForPlanDAG(const QueryPlan::Node & root);
}
}
