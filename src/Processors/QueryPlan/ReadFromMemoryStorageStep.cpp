#include "ReadFromMemoryStorageStep.h"

#include <atomic>
#include <functional>
#include <memory>
#include <stack>

#include <Common/FieldVisitors.h>
#include <Common/FieldVisitorsAccurateComparison.h>
#include <Functions/IFunction.h>
#include <Interpreters/getColumnFromBlock.h>
#include <Interpreters/inplaceBlockConversions.h>
#include <Interpreters/InterpreterSelectQuery.h>
#include <Storages/StorageSnapshot.h>
#include <Storages/StorageMemory.h>

#include <QueryPipeline/Pipe.h>
#include <QueryPipeline/QueryPipelineBuilder.h>
#include <Processors/ISource.h>
#include <Processors/Sources/NullSource.h>

namespace DB
{

namespace
{

std::vector<size_t> getIntersect(std::vector<size_t> && first, std::vector<size_t> && second)
{
    if (first.empty())
    {
        return second;
    }

    if (second.empty())
    {
        return first;
    }

    auto temp = std::move(second);
    std::set_intersection(std::make_move_iterator(first.begin()), std::make_move_iterator(first.end()),
                          std::make_move_iterator(temp.begin()), std::make_move_iterator(temp.end()),
                          std::back_inserter(second));

    return second;
}

std::vector<size_t> getNecessaryPositions(const Block & block, const ReadFromMemoryStorageStep::FixedColumns & fixed_columns)
{
    std::vector<size_t> block_necessary_positions;

    for (const auto & fixed_column : fixed_columns)
    {
        if (!fixed_column)
        {
            fmt::print("broken fixed column\n");
            continue;
        }

        fmt::print(">>> fixed column: name={}\n", fixed_column->result_name);
        std::vector<size_t> fixed_column_necessary_positions;


        if (const auto * column_in_block = block.findByName(fixed_column->result_name); column_in_block != nullptr)
        {
            fmt::print("column_in_block: name={}\n", column_in_block->name);

            std::vector<size_t> result_fields_positions;

            if (column_in_block->column)
            {
                const auto constant = (*fixed_column->column)[0];
                const auto & field = *column_in_block->column;

                for (size_t i=0; i<column_in_block->column->size(); i++)
                {
                    fmt::print("#{} field in block: type={}, value={}, fixed_field={}\n",
                               i,
                               field[i].getType(),
                               toString(field[i]),
                               toString(constant));

                    if (applyVisitor(FieldVisitorAccurateEquals(), constant, field[i]))
                    {
                        fmt::print("necessary field with pos={}\n", i);
                        result_fields_positions.push_back(i);
                    }
                    else
                    {
                        fmt::print("unnecessary field with pos={}\n", i);
                    }
                }
                fmt::print("\n\n");
            }
            fmt::print("for column_in_block: name={} need only this fields: ", column_in_block->name);
            for (const auto& n : result_fields_positions)
            {
                fmt::print("{}, ", n);
            }
            fmt::print("\n");

            fixed_column_necessary_positions = getIntersect(std::move(result_fields_positions),
                                                             std::move(fixed_column_necessary_positions));
        }

        fmt::print("for current column need only this fields: ");
        for (const auto& n : fixed_column_necessary_positions)
        {
            fmt::print("{}, ", n);
        }
        fmt::print("\n\n");

        block_necessary_positions = getIntersect(std::move(fixed_column_necessary_positions),
                                                  std::move(block_necessary_positions));
    }

    fmt::print("result necessary columns: ");
    for (const auto& n : block_necessary_positions)
    {
        fmt::print("{}, ", n);
    }
    fmt::print("\n\n");

    return block_necessary_positions;
}

ColumnWithTypeAndName makeColumnNecessaryFields(const ColumnWithTypeAndName * block_column, const std::vector<size_t> & block_necessary_positions)
{
    assert(block_column != nullptr);
    assert(!block_necessary_positions.empty());

    ColumnWithTypeAndName filtered_column = block_column->cloneEmpty();
    auto c = filtered_column.column->cloneFinalized();

    for (size_t pos : block_necessary_positions)
    {
        fmt::print("try to copy usefully field: pos={}, value={}\n", pos, toString(block_column->column->getPtr()->operator[](pos)));
        c->insertFrom(*block_column->column->getPtr(), pos);
    }

    ColumnWithTypeAndName res(c->getPtr(), filtered_column.type, filtered_column.name);

    fmt::print("new column after filters:\n");
    for (size_t i=0; i<res.column->size(); i++)
    {
        fmt::print("field={}\n", toString((*res.column)[i]));
    }
    fmt::print("\n\n");

    return res;
}

}

class MemorySource : public ISource
{
    using InitializerFunc = std::function<void(std::shared_ptr<const Blocks> &)>;
public:

    MemorySource(
        Names column_names_,
        const StorageSnapshotPtr & storage_snapshot,
        std::shared_ptr<const Blocks> data_,
        std::shared_ptr<std::atomic<size_t>> parallel_execution_index_,
        InitializerFunc initializer_func_ = {})
        : ISource(storage_snapshot->getSampleBlockForColumns(column_names_))
        , column_names_and_types(storage_snapshot->getColumnsByNames(
              GetColumnsOptions(GetColumnsOptions::All).withSubcolumns().withExtendedObjects(), column_names_))
        , data(data_)
        , parallel_execution_index(parallel_execution_index_)
        , initializer_func(std::move(initializer_func_))
    {
    }

    String getName() const override { return "Memory"; }

protected:
    Chunk generate() override
    {
        if (initializer_func)
        {
            initializer_func(data);
            initializer_func = {};
        }

        size_t current_index = getAndIncrementExecutionIndex();

        if (!data || current_index >= data->size())
        {
            return {};
        }

        const Block & src = (*data)[current_index];

        Columns columns;
        size_t num_columns = column_names_and_types.size();
        columns.reserve(num_columns);

        auto name_and_type = column_names_and_types.begin();
        for (size_t i = 0; i < num_columns; ++i)
        {
            columns.emplace_back(tryGetColumnFromBlock(src, *name_and_type));
            ++name_and_type;
        }

        fillMissingColumns(columns, src.rows(), column_names_and_types, column_names_and_types, {}, nullptr);
        assert(std::all_of(columns.begin(), columns.end(), [](const auto & column) { return column != nullptr; }));

        return Chunk(std::move(columns), src.rows());
    }

private:
    size_t getAndIncrementExecutionIndex()
    {
        if (parallel_execution_index)
        {
            return (*parallel_execution_index)++;
        }
        else
        {
            return execution_index++;
        }
    }

    const NamesAndTypesList column_names_and_types;
    size_t execution_index = 0;
    std::shared_ptr<const Blocks> data;
    std::shared_ptr<std::atomic<size_t>> parallel_execution_index;
    InitializerFunc initializer_func;
};

ReadFromMemoryStorageStep::ReadFromMemoryStorageStep(const Names & columns_to_read_,
                                                     const StorageSnapshotPtr & storage_snapshot_,
                                                     const size_t num_streams_,
                                                     const bool delay_read_for_global_sub_queries_) :
    SourceStepWithFilter(DataStream{.header=storage_snapshot_->getSampleBlockForColumns(columns_to_read_)}),
    columns_to_read(columns_to_read_),
    storage_snapshot(storage_snapshot_),
    num_streams(num_streams_),
    delay_read_for_global_sub_queries(delay_read_for_global_sub_queries_)
{
}

void ReadFromMemoryStorageStep::initializePipeline(QueryPipelineBuilder & pipeline, const BuildQueryPipelineSettings &)
{
    auto pipe = makePipe();

    if (pipe.empty())
    {
        assert(output_stream != std::nullopt);
        pipe = Pipe(std::make_shared<NullSource>(output_stream->header));
    }

    pipeline.init(std::move(pipe));
}

ReadFromMemoryStorageStep::FixedColumns ReadFromMemoryStorageStep::makeFixedColumns()
{
    FixedColumns fixed_columns;
    for (const auto * filter_expression : getFilterNodes().nodes)
    {
        if (!filter_expression)
        {
            continue;
        }

        std::stack<const ActionsDAG::Node *> stack;
        stack.push(filter_expression);

        while (!stack.empty())
        {
            const auto * node = stack.top();
            stack.pop();
            if (node->type == ActionsDAG::ActionType::FUNCTION)
            {
                const auto & func_name = node->function_base->getName();
                if (func_name == "and")
                {
                    for (const auto * arg : node->children)
                        stack.push(arg);
                }
                else if (func_name == "equals")
                {
                    fmt::print("make fixed: has equals\n");
                    ActionsDAG::Node * maybe_fixed_column = nullptr;
                    size_t num_constant_columns = 0;

                    fmt::print("size children: {}\n", node->children.size());

                    // @TODO why one children with empty column?
                    for (const auto & child : node->children)
                    {
                        if (child->column)
                        {
                            fmt::print("fixed: child has column: name={}\n", child->result_name);
                            fmt::print("fixed: column size={}\n", node->children.size());

                            // @TODO why children size == 2 and value eq?
                            for (size_t i=0; i<node->children.size(); i++)
                            {
                                fmt::print("child: filed={}\n", toString((*child->column)[i]));
                            }
                            ++num_constant_columns;

                            if (maybe_fixed_column && maybe_fixed_column->column == nullptr)
                            {
                                maybe_fixed_column->column = child->column;
                            }
                        }
                        else
                        {
                            fmt::print("fixed: child has empty column: name={}\n", child->result_name);
                            maybe_fixed_column = const_cast<ActionsDAG::Node *>(child);
                        }
                    }
                    if (maybe_fixed_column && num_constant_columns + 1 == node->children.size())
                    {
                        fmt::print(">>> add to fixed_columns\n");
                        fixed_columns.insert(maybe_fixed_column);
                    }
                }
            }
        }
    }

    for (const auto & c : fixed_columns)
    {
        fmt::print("total: name={}\n", c->result_name);
        fmt::print("total: type={}\n", c->result_type);
        fmt::print("total: column name={}\n", c->column->getName());
        fmt::print("total: size columns={}\n", c->column->size());
        for (size_t i=0; i<c->column->size(); i++)
        {
            fmt::print("field={}\n", toString((*c->column)[i]));
        }
    }
    return fixed_columns;
}

std::shared_ptr<const Blocks> ReadFromMemoryStorageStep::filteredByFixedColumns(std::shared_ptr<const Blocks> blocks_ptr)
{
    if (!blocks_ptr || blocks_ptr->empty())
    {
        return blocks_ptr;
    }

    const auto fixed_columns = makeFixedColumns();
    if (fixed_columns.empty())
    {
        return blocks_ptr;
    }

    Blocks blocks_with_fixed_columns;
    blocks_with_fixed_columns.reserve(columns_to_read.size());

    std::vector<size_t> necessary_positions;

    for (const auto & block : *blocks_ptr)
    {
        Block block_with_fixed_columns;

        fmt::print("\nstart filtered block\n");

        auto block_necessary_positions = getNecessaryPositions(block, fixed_columns);

        for (const auto & column_to_read : columns_to_read)
        {
            if (const auto * block_column = block.findByName(column_to_read); block_column != nullptr)
            {
                auto res = makeColumnNecessaryFields(block_column, block_necessary_positions);
                block_with_fixed_columns.insert(res);
            }
        }

        if (block_with_fixed_columns)
        {
            blocks_with_fixed_columns.push_back(std::move(block_with_fixed_columns));
        }
    }
    return std::make_shared<const Blocks>(std::move(blocks_with_fixed_columns));
}

Pipe ReadFromMemoryStorageStep::makePipe()
{
    storage_snapshot->check(columns_to_read);

    const auto & snapshot_data = assert_cast<const StorageMemory::SnapshotData &>(*storage_snapshot->data);

    auto blocks_with_fixed_columns = filteredByFixedColumns(snapshot_data.blocks);
//    auto blocks_with_fixed_columns = snapshot_data.blocks;

    if (delay_read_for_global_sub_queries)
    {
        /// Note: for global subquery we use single source.
        /// Mainly, the reason is that at this point table is empty,
        /// and we don't know the number of blocks are going to be inserted into it.
        ///
        /// It may seem to be not optimal, but actually data from such table is used to fill
        /// set for IN or hash table for JOIN, which can't be done concurrently.
        /// Since no other manipulation with data is done, multiple sources shouldn't give any profit.

        return Pipe(std::make_shared<MemorySource>(
            columns_to_read,
            storage_snapshot,
            nullptr /* data */,
            nullptr /* parallel execution index */,
            [blocks_with_fixed_columns](std::shared_ptr<const Blocks> & data_to_initialize)
            {
                data_to_initialize = blocks_with_fixed_columns;
            }));
    }

    size_t size = blocks_with_fixed_columns->size();

    if (num_streams > size)
        num_streams = size;

    Pipes pipes;

    auto parallel_execution_index = std::make_shared<std::atomic<size_t>>(0);

    for (size_t stream = 0; stream < num_streams; ++stream)
    {
        pipes.emplace_back(std::make_shared<MemorySource>(columns_to_read, storage_snapshot, blocks_with_fixed_columns, parallel_execution_index));
    }
    return Pipe::unitePipes(std::move(pipes));
}

}
