#pragma once

#include <Processors/Port.h>
#include <Processors/IAccumulatingTransform.h>

namespace DB
{

/** Reorders rows so that every row appears after all rows it depends on.
  * Implements Kahn's BFS topological sort.
  *
  * key_column: the sort key column (any comparable type).
  * deps_column: Array(T) column listing keys this row depends on.
  *
  * Rows with the same key are treated as a single node in the DAG.
  * If a cycle is detected an exception is thrown.
  */
class TopologicalSortTransform : public IAccumulatingTransform
{
public:
    TopologicalSortTransform(
        SharedHeader header,
        const String & key_column_name,
        const String & deps_column_name);

    String getName() const override { return "TopologicalSortTransform"; }

protected:
    void consume(Chunk chunk) override;
    Chunk generate() override;

private:
    String key_column_name;
    String deps_column_name;
    size_t key_column_pos;
    size_t deps_column_pos;

    Chunks accumulated;
    bool generated = false;
};

}
