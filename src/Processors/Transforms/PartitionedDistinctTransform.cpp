#include <iterator>
#include <memory>
#include <mutex>
#include <Core/ColumnWithTypeAndName.h>
#include <Processors/Transforms/PartitionedDistinctTransform.h>
#include <Common/BitHelpers.h>
#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
extern const int SET_SIZE_LIMIT_EXCEEDED;
extern const int LOGICAL_ERROR;
}

class PartitionedChunkInfo : public ChunkInfo
{
public:
    using SelectorPtr = std::shared_ptr<detail::Selector>;
    using RefCount = std::shared_ptr<std::atomic<size_t>>;
    using FilterPtr = std::shared_ptr<IColumn::Filter>;

    explicit PartitionedChunkInfo(const SelectorPtr & selector_, RefCount ref_count_, FilterPtr filter_)
        : selector(selector_)
        , ref_count(ref_count_)
        , filter(filter_)
    {
    }

    ChunkInfo::Ptr clone() const override
    { 
        return std::make_shared<PartitionedChunkInfo>(selector, ref_count, filter);
    }

    SelectorPtr selector;
    RefCount ref_count;
    FilterPtr filter;
};

PartitionDistinctTransformBase::PartitionDistinctTransformBase(SharedHeader header_, size_t input_streams_, size_t output_streams_)
    : IProcessor(InputPorts(input_streams_, header_), OutputPorts(output_streams_, header_))
    , header(std::move(header_))
    , input_streams(input_streams_)
    , output_streams(output_streams_)
    , input_chunks(input_streams_)
    , output_chunks(output_streams_)
{
}


IProcessor::Status PartitionDistinctTransformBase::prepare(const PortNumbers & updated_input_ports, const PortNumbers & updated_output_ports)
{
    if (input_ports.empty() || output_ports.empty()) [[unlikely]]
    {
        for (auto & output : outputs)
            output_ports.emplace_back(&output, PortStatus::NotActive);
        for (auto & input : inputs)
            input_ports.emplace_back(&input, PortStatus::NotActive);
    }

    for (const auto & num : updated_output_ports)
    {
        auto & output_port = output_ports[num];
        if (output_port.port->isFinished())
        {
            if (output_port.status != PortStatus::Finished)
            {
                finished_output_ports += 1;
                output_port.status = PortStatus::Finished;
            }
            continue;
        }
        if (output_port.port->canPush())
        {
            if (output_port.status != PortStatus::NeedData)
            {
                waiting_output_ports.push(num);
                output_port.status = PortStatus::NeedData;
            }
        }
    }

    bool all_output_finished = finished_output_ports == output_ports.size();
    if (all_output_finished)
    {
        for (auto & input_port : input_ports)
        {
            input_port.port->close();
            input_port.status = PortStatus::Finished;
        }
        return Status::Finished;
    }

    for (const auto & num : updated_input_ports)
    {
        auto & input_port = input_ports[num];
        if (input_port.port->isFinished())
        {
            if (input_port.status != PortStatus::Finished)
            {
                finished_input_ports += 1;
                input_port.status = PortStatus::Finished;
            }
            continue;
        }
        if (input_port.port->hasData())
        {
            ChunkQueue & chunk_queue = input_chunks[num];
            Chunk chunk = input_port.port->pull();
            chunk_queue.push(std::move(chunk));
            input_port.status = PortStatus::Blocked;
            pending_input_chunks_num += 1;
        }
    }

    bool all_inputs_finished = finished_input_ports == input_ports.size();
    if (all_inputs_finished)
    {
        // No inputs and outputs, finish this processor.
        if (!pending_output_chunks_num && !pending_input_chunks_num)
        {
            for (auto & output : output_ports)
            {
                output.port->finish();
                output.status = PortStatus::Finished;
            }
            return Status::Finished;
        }
    }

    std::queue<size_t> temp_waiting_output_ports;
    while (!waiting_output_ports.empty())
    {
        auto output_port_index = waiting_output_ports.front();
        waiting_output_ports.pop();
        auto & chunk_queue = output_chunks[output_port_index];
        auto & output_port = output_ports[output_port_index];

        if (output_port.port->isFinished())
        {
            output_port.status = PortStatus::Finished;
            continue;
        }
        if (!output_port.port->canPush())
        {
            output_port.status = PortStatus::Blocked;
            continue;
        }

        if (!chunk_queue.empty())
        {
            auto & chunk = chunk_queue.front();
            output_port.port->push(std::move(chunk));
            --pending_output_chunks_num;
            output_port.status = PortStatus::Blocked;
            chunk_queue.pop();
        }
        else
        {
            temp_waiting_output_ports.push(output_port_index);
        }
    }
    waiting_output_ports.swap(temp_waiting_output_ports);

    if (pending_input_chunks_num)
        return Status::Ready;

    if (!waiting_output_ports.empty())
    {
        for (auto & input_port : input_ports)
        {
            if (!input_port.port->isFinished())
            {
                input_port.port->setNeeded();
                input_port.status = PortStatus::NeedData;
            }
        }
        return Status::NeedData;
    }

    return Status::PortFull;
}

static ColumnNumbers getColumnsPositions(const Block & header, const Names & column_names)
{
    ColumnNumbers positions;
    positions.reserve(column_names.size());
    for (const auto & name : column_names)
    {
        positions.push_back(header.getPositionByName(name));
    }
    return positions;
}

PartitionDistinctMapTransform::PartitionDistinctMapTransform(SharedHeader header_, const Names & key_columns_names_, size_t output_streams_)
    : PartitionDistinctTransformBase(header_, 1, output_streams_)
    , key_columns_pos(getColumnsPositions(*header_, key_columns_names_))
{
    constexpr auto threshold = sizeof(IColumn::Selector::value_type);
    const auto & data_types = header_->getDataTypes();
    use_copy_scatter = std::accumulate(
                           data_types.begin(),
                           data_types.end(),
                           0u,
                           [](size_t sum, const DataTypePtr & type)
                           { return sum + (type->haveMaximumSizeOfValue() ? type->getMaximumSizeOfValueInMemory() : threshold + 1); })
        <= threshold;
}

std::vector<std::shared_ptr<detail::Selector>> PartitionDistinctMapTransform::scatterBlock(const Block & block)
{
    size_t rows = block.rows();
    WeakHash32 hash(rows);
    const auto & columns = block.getColumnsWithTypeAndName();
    for (const auto & pos : key_columns_pos)
    {
        hash.update(columns[pos].column->getWeakHash32());
    }

    std::vector<ScatteredBlock::IndexesPtr> indexes(output_streams);
    for (size_t i = 0; i < output_streams; ++i)
    {
        indexes[i] = ScatteredBlock::Indexes::create();
        indexes[i]->reserve(rows / output_streams + 1);
    }
    if (isPowerOf2(output_streams))
    {
        for (size_t i = 0; i < rows; ++i)
        {
            const auto & hash_value = hash.getData()[i];
            size_t part = hash_value & (output_streams - 1);
            indexes[part]->getData().push_back(i);
        }
    }
    else
    {
        for (size_t i = 0; i < rows; ++i)
        {
            const auto & hash_value = hash.getData()[i];
            size_t part = hash_value % output_streams;
            indexes[part]->getData().push_back(i);
        }
    }

    std::vector<std::shared_ptr<detail::Selector>> selectors;
    selectors.reserve(output_streams);
    for (size_t i = 0; i < output_streams; ++i)
    {
        selectors.emplace_back(std::make_shared<detail::Selector>(std::move(indexes[i])));
    }
    return selectors;
}

Blocks PartitionDistinctMapTransform::scatterBlockByCopying(const ColumnNumbers & key_columns_pos_, const Block & block)
{
    size_t rows = block.rows();
    WeakHash32 hash(rows);
    const auto & columns = block.getColumnsWithTypeAndName();
    for (const auto & pos : key_columns_pos_)
    {
        hash.update(columns[pos].column->getWeakHash32());
    }

    IColumn::Selector selector(rows);
    if (isPowerOf2(output_streams))
    {
        for (size_t i = 0; i < rows; ++i)
        {
            selector[i] = hash.getData()[i] & (output_streams - 1);
        }
    }
    else
    {
        for (size_t i = 0; i < rows; ++i)
        {
            selector[i] = hash.getData()[i] % output_streams;
        }
    }

    Blocks blocks(output_streams);
    for (size_t i = 0; i < output_streams; ++i)
    {
        blocks[i] = block.cloneEmpty();
    }
    for (size_t i = 0; i < block.columns(); ++i)
    {
        auto dispatched_columns = block.getByPosition(i).column->scatter(output_streams, selector);
        chassert(blocks.size() == dispatched_columns.size());
        for (size_t block_index = 0; block_index < output_streams; ++block_index)
        {
            blocks[block_index].getByPosition(i).column = std::move(dispatched_columns[block_index]);
        }
    }
    return blocks;
}

void PartitionDistinctMapTransform::work()
{
    if (!pending_input_chunks_num)
        return;

    Chunk pending_chunk(std::move(input_chunks[0].front()));
    input_chunks[0].pop();
    pending_input_chunks_num--;
    if (unlikely(!pending_chunk.hasRows()))
        return;
    convertToFullIfSparse(pending_chunk);
    convertToFullIfConst(pending_chunk);

    size_t rows = pending_chunk.getNumRows();
    auto columns = pending_chunk.detachColumns();

    auto block = header->cloneWithColumns(columns);

    if (use_copy_scatter)
    {
        auto scattered_blocks = scatterBlockByCopying(key_columns_pos, block);
        for (size_t i = 0; i < output_streams; ++i)
        {
            Chunk output_chunk(scattered_blocks[i].getColumns(), scattered_blocks[i].rows());
            output_chunks[i].push(std::move(output_chunk));
            pending_output_chunks_num++;
        }
    }
    else
    {
        auto selectors = scatterBlock(block);
        auto ref_count = std::make_shared<std::atomic<size_t>>(output_streams);
        auto filter = std::make_shared<IColumn::Filter>(rows, 0);
        for (size_t i = 0; i < output_streams; ++i)
        {
            Chunk output_chunk(columns, rows);
            output_chunk.getChunkInfos().add(std::make_shared<PartitionedChunkInfo>(selectors[i], ref_count, filter));
            output_chunks[i].push(std::move(output_chunk));
            pending_output_chunks_num++;
        }
    }
}

template <typename Method>
void buildFilter(
    Method & method,
    const ColumnRawPtrs & columns,
    const ScatteredBlock::Selector * selector,
    IColumn::Filter & filter,
    SetVariants & variants,
    Sizes & column_sizes)
{
    typename Method::State state(columns, column_sizes, nullptr);

    if (!selector)
    {
        auto columns_rows = columns[0]->size();
        for (size_t i = 0; i < columns_rows; ++i)
        {
            const auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);
            filter[i] = emplace_result.isInserted();
        }
    }
    else
    {
        if (selector->isContinuousRange())
        {
            const auto range = selector->getRange();
            for (size_t i = range.first; i < range.second; ++i)
            {
                const auto emplace_result = state.emplaceKey(method.data, i, variants.string_pool);
                filter[i] = emplace_result.isInserted();
            }
        }
        else
        {
            size_t partition_rows = selector->size();
            const auto & indexes = selector->getIndexes().getData();
            for (size_t i = 0; i < partition_rows; ++i)
            {
                size_t row = indexes[i];
                const auto emplace_result = state.emplaceKey(method.data, row, variants.string_pool);
                filter[row] = emplace_result.isInserted();
            }
        }
    }
}

PartitionDistinctReduceTransform::PartitionDistinctReduceTransform(
    SharedHeader header_, const Names & key_columns_names_, size_t input_streams_)
    : PartitionDistinctTransformBase(header_, input_streams_, 1)
    , key_columns_pos(getColumnsPositions(*header_, key_columns_names_))
{
    ColumnRawPtrs column_ptrs;
    for (const auto & pos : key_columns_pos)
    {
        const auto & col = header_->getByPosition(pos).column;
        column_ptrs.emplace_back(col.get());
    }
    set_variants.init(SetVariants::chooseMethod(column_ptrs, key_column_sizes));
}

void PartitionDistinctReduceTransform::work()
{
    if (!pending_input_chunks_num)
        return;

    Chunk input_chunk;
    for (size_t i = next_input_stream; i < input_streams + next_input_stream; ++i)
    {
        size_t index = i % input_streams;
        auto & queue = input_chunks[index];
        if (!queue.empty())
        {
            input_chunk.swap(queue.front());
            queue.pop();
            pending_input_chunks_num--;
            next_input_stream = (index + 1) % input_streams;
            break;
        }
    }
    auto columns = input_chunk.detachColumns();
    auto block = header->cloneWithColumns(columns);
    ColumnRawPtrs key_columns;
    for (const auto & pos : key_columns_pos)
    {
        const auto & col = block.getByPosition(pos).column;
        key_columns.emplace_back(col.get());
        if (!key_columns.back())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Key column is nullptr");
    }

    auto chunk_info = input_chunk.getChunkInfos().get<PartitionedChunkInfo>();
    if (!chunk_info)
    {
        IColumn::Filter filter(columns[0]->size(), 0);
        switch (set_variants.type)
        {
            case SetVariants::Type::EMPTY:
                break;
#define M(NAME) \
            case SetVariants::Type::NAME: \
                buildFilter(*(set_variants.NAME), key_columns, nullptr, filter, set_variants, key_column_sizes); \
                break;
            APPLY_FOR_SET_VARIANTS(M)
#undef M
        }
        Columns filtered_columns;
        size_t rows = 0;
        filtered_columns.push_back(columns[0]->filter(filter, -1));
        rows = filtered_columns[0]->size();
        if (!rows)
            return;
        for (size_t i = 1; i < columns.size(); ++i)
        {
            filtered_columns.push_back(columns[i]->filter(filter, rows));
        }
        Chunk output_chunk(std::move(filtered_columns), rows);
        output_chunks[0].push(std::move(output_chunk));
        pending_output_chunks_num++;
        return;
    }
    else
    {
        auto & filter = chunk_info->filter;
        auto & ref_count = chunk_info->ref_count;
        auto & selector = chunk_info->selector;

        switch (set_variants.type)
        {
            case SetVariants::Type::EMPTY:
                break;
#define M(NAME) \
            case SetVariants::Type::NAME: \
                buildFilter(*(set_variants.NAME), key_columns, selector.get(), *filter, set_variants, key_column_sizes); \
                break;
            APPLY_FOR_SET_VARIANTS(M)
#undef M
        }

        auto prev_count = ref_count->fetch_sub(1);
        if (prev_count == 1)
        {
            // The last node to handle this chunk, produce output.
            Columns filtered_columns;
            size_t rows = 0;
            filtered_columns.push_back(columns[0]->filter(*filter, -1));
            rows = filtered_columns[0]->size();
            if (!rows)
                return;
            for (size_t i = 1; i < columns.size(); ++i)
            {
                filtered_columns.push_back(columns[i]->filter(*filter, rows));
            }
            Chunk output_chunk(std::move(filtered_columns), rows);
            output_chunks[0].push(std::move(output_chunk));
            pending_output_chunks_num++;
        }
    }
}

}
