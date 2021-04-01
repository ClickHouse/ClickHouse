#include <Processors/Transforms/AddingSelectorTransform.h>
#include <Processors/Transforms/SelectorInfo.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

AddingSelectorTransform::AddingSelectorTransform(
    const Block & header, size_t num_outputs_, ColumnNumbers key_columns_)
    : ISimpleTransform(header, header, false)
    , num_outputs(num_outputs_)
    , key_columns(std::move(key_columns_))
    , hash(0)
{
    setInputNotNeededAfterRead(false);

    if (num_outputs <= 1)
        throw Exception("SplittingByHashTransform expects more than 1 outputs, got " + std::to_string(num_outputs),
                        ErrorCodes::LOGICAL_ERROR);

    if (key_columns.empty())
        throw Exception("SplittingByHashTransform cannot split by empty set of key columns",
                        ErrorCodes::LOGICAL_ERROR);

    for (auto & column : key_columns)
        if (column >= header.columns())
            throw Exception("Invalid column number: " + std::to_string(column) +
                            ". There is only " + std::to_string(header.columns()) + " columns in header",
                            ErrorCodes::LOGICAL_ERROR);
}

static void calculateWeakHash32(const Chunk & chunk, const ColumnNumbers & key_columns, WeakHash32 & hash)
{
    auto num_rows = chunk.getNumRows();
    const auto & columns = chunk.getColumns();

    hash.reset(num_rows);

    for (const auto & column_number : key_columns)
        columns[column_number]->updateWeakHash32(hash);
}

static IColumn::Selector fillSelector(const WeakHash32 & hash, size_t num_outputs)
{
    /// Row from interval [(2^32 / num_outputs) * i, (2^32 / num_outputs) * (i + 1)) goes to bucket with number i.

    const auto & hash_data = hash.getData();
    size_t num_rows = hash_data.size();
    IColumn::Selector selector(num_rows);

    for (size_t row = 0; row < num_rows; ++row)
    {
        selector[row] = hash_data[row]; /// [0, 2^32)
        selector[row] *= num_outputs; /// [0, num_outputs * 2^32), selector stores 64 bit values.
        selector[row] >>= 32u; /// [0, num_outputs)
    }

    return selector;
}

void AddingSelectorTransform::transform(Chunk & input_chunk, Chunk & output_chunk)
{
    auto chunk_info = std::make_shared<SelectorInfo>();

    calculateWeakHash32(input_chunk, key_columns, hash);
    chunk_info->selector = fillSelector(hash, num_outputs);

    input_chunk.swap(output_chunk);
    output_chunk.setChunkInfo(std::move(chunk_info));
}

}
