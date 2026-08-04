#pragma once

#include <Processors/QueryPlan/Optimizations/DataProperties.h>

#include <Core/Block_fwd.h>

#include <span>

namespace DB
{

class IQueryPlanStep;
struct StorageInMemoryMetadata;

namespace QueryPlanOptimizations
{

/// Pure helper used by storage-backed source steps and focused tests.
DataPropertySet deriveDataPropertiesForStorageRead(const Block & output_header, const StorageInMemoryMetadata * metadata);

/// Derive properties local to one step without modifying the caller's values.
DataPropertySet deriveDataProperties(const IQueryPlanStep & step, std::span<const DataPropertySet> child_properties);

}
}
