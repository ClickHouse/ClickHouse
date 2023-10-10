
#include <memory>
#include <Processors/ISimpleTransform.h>
#include <Processors/Merges/Algorithms/ReplacingSortedAlgorithm.h>
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
        auto select_final_indices_info = std::dynamic_pointer_cast<const ChunkSelectFinalIndices>(chunk.getChunkInfo());

        if (!select_final_indices_info)
            return;

        const auto & index_column = select_final_indices_info->select_final_indices;

        if (index_column && index_column->size() != num_rows)
        {
            auto columns = chunk.detachColumns();
            for (auto & column : columns)
                column = column->index(*index_column, 0);

            chunk.setColumns(std::move(columns), index_column->size());
        }
        chunk.setChunkInfo(nullptr);
    }

private:
    String index_column_name;
};

}
