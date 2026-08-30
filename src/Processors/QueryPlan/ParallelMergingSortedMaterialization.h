#pragma once

#include <cstddef>
#include <memory>

namespace DB
{

class MergingSortedTransformStats;
class Pipe;
class QueryPipelineBuilder;

void addParallelMergingSortedMaterialization(
    Pipe & pipe,
    size_t materialization_threads,
    size_t max_rows_to_buffer,
    const std::shared_ptr<MergingSortedTransformStats> & stats);

void addParallelMergingSortedMaterialization(
    QueryPipelineBuilder & pipeline,
    size_t materialization_threads,
    size_t max_rows_to_buffer,
    const std::shared_ptr<MergingSortedTransformStats> & stats);

}
