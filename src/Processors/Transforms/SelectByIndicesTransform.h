#pragma once

#include <memory>
#include <Processors/ISimpleTransform.h>
#include <Processors/Merges/Algorithms/ReplacingSortedAlgorithm.h>
#include <Common/Exception.h>

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

namespace DB
{

class SelectByIndicesTransform : public ISimpleTransform
{
public:
    explicit SelectByIndicesTransform(const Block & header)
        : ISimpleTransform(header, header, true)
    {
    }

    String getName() const override { return "SelectByIndicesTransform"; }

    void transform(Chunk & chunk) override
    {
        size_t num_rows = chunk.getNumRows();

        auto select_all_rows_info = chunk.getChunkInfos().extract<ChunkSelectFinalAllRows>();
        if (select_all_rows_info)
            return;

        auto select_final_indices_info = chunk.getChunkInfos().extract<ChunkSelectFinalIndices>();
        if (!select_final_indices_info || !select_final_indices_info->select_final_indices)
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Chunk passed to SelectByIndicesTransform without indices column");

        const auto & index_column = select_final_indices_info->select_final_indices;

        if (index_column->size() != num_rows)
        {
            auto columns = chunk.detachColumns();
            for (auto & column : columns)
                column = column->index(*index_column, 0);

            chunk.setColumns(std::move(columns), index_column->size());
        }
    }
};

}
