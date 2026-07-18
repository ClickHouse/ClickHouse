#include <Storages/ObjectStorage/DataLakes/DuckLake/DuckLakePartitionConstantsTransform.h>

#if USE_PARQUET

#include <Columns/ColumnConst.h>

namespace DB
{

DuckLakePartitionConstantsTransform::DuckLakePartitionConstantsTransform(
    const SharedHeader & input_header_,
    std::vector<ConstantColumn> constants_)
    : ISimpleTransform(input_header_, makeOutputHeader(input_header_, constants_), /* skip_empty_chunks */ false)
{
    const auto & input_header = getInputPort().getHeader();
    constants.reserve(constants_.size());
    for (auto & constant : constants_)
    {
        size_t input_position = std::numeric_limits<size_t>::max();
        const auto * found = input_header.findByName(constant.name);
        if (found)
            input_position = input_header.getPositionByName(constant.name);
        constants.push_back(ResolvedConstant{
            .input_position = input_position,
            .type = std::move(constant.type),
            .value = std::move(constant.value),
        });
    }
}

SharedHeader DuckLakePartitionConstantsTransform::makeOutputHeader(
    const SharedHeader & input_header_,
    const std::vector<ConstantColumn> & constants_)
{
    Block output_header = *input_header_;
    for (const auto & constant : constants_)
    {
        ColumnConstPtr column = constant.type->createColumnConst(1, constant.value);
        if (output_header.has(constant.name))
        {
            auto & existing = output_header.getByName(constant.name);
            existing.column = column;
            existing.type = constant.type;
        }
        else
        {
            output_header.insert({column, constant.type, constant.name});
        }
    }
    return std::make_shared<const Block>(std::move(output_header));
}

void DuckLakePartitionConstantsTransform::transform(Chunk & chunk)
{
    const size_t num_rows = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    for (const auto & constant : constants)
    {
        ColumnConstPtr materialized = constant.type->createColumnConst(num_rows, constant.value);
        if (constant.input_position != std::numeric_limits<size_t>::max())
            columns[constant.input_position] = std::move(materialized);
        else
            columns.push_back(std::move(materialized));
    }
    chunk.setColumns(std::move(columns), num_rows);
}

}

#endif
