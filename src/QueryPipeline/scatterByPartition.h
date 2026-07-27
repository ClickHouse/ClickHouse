#pragma once

#include <Core/ColumnNumbers.h>
#include <DataTypes/IDataType_fwd.h>

namespace DB
{

class QueryPipelineBuilder;

/// Repartitions the data by the hash of the key columns: the pipeline ends up with
/// num_partitions streams where stream i carries exactly the rows of partition i.
/// `hash_cast_types` (one entry per key, optional) selects a type to cast each key to before hashing.
void scatterByPartition(QueryPipelineBuilder & pipeline, size_t num_partitions, const ColumnNumbers & key_columns, const DataTypes & hash_cast_types = {});

}
