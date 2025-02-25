#pragma once

#include <Core/ColumnNumbers.h>

namespace DB
{

class QueryPipelineBuilder;

void scatterDataByKeysIfNeeded(
    QueryPipelineBuilder & pipeline,
    const ColumnNumbers & key_columns,
    size_t threads,
    size_t streams
);

}
