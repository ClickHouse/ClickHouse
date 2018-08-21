#include <DataStreams/RollupBlockInputStream.h>
#include <Interpreters/ExpressionActions.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Columns/FilterDescription.h>
#include <Common/typeid_cast.h>

#include <Poco/Logger.h>
#include <common/logger_useful.h>


namespace DB
{

RollupBlockInputStream::RollupBlockInputStream(
    const BlockInputStreamPtr & input_, const Aggregator::Params & params_) : params(params_), aggregator(params)
{
    children.push_back(input_);

    LOG_DEBUG(&Logger::get("Rollup"), "children: " << children.size());

    LOG_DEBUG(&Logger::get("Rollup"), "input columns: " << input_->getHeader().columns());

    Aggregator::CancellationHook hook = [&]() { return this->isCancelled(); };
    aggregator.setCancellationHook(hook);


    /// Initialize current totals with initial state.

    arena = std::make_shared<Arena>();
    Block source_header = children.at(0)->getHeader();

    current_totals.reserve(source_header.columns());
    for (const auto & elem : source_header)
    {
        if (const ColumnAggregateFunction * column = typeid_cast<const ColumnAggregateFunction *>(elem.column.get()))
        {
            /// Create ColumnAggregateFunction with initial aggregate function state.

            auto target = ColumnAggregateFunction::create(column->getAggregateFunction(), Arenas(1, arena));
            current_totals.emplace_back(std::move(target));
        }
        else
        {

            /// Not an aggregate function state. Just create a column with default value.

            MutableColumnPtr new_column = elem.type->createColumn();
            current_totals.emplace_back(std::move(new_column));
            ++not_aggregate_columns;
        }
    }
}


static void finalize(Block & block)
{
    for (size_t i = 0; i < block.columns(); ++i)
    {
        ColumnWithTypeAndName & current = block.getByPosition(i);
        const DataTypeAggregateFunction * unfinalized_type = typeid_cast<const DataTypeAggregateFunction *>(current.type.get());

        if (unfinalized_type)
        {
            current.type = unfinalized_type->getReturnType();
            if (current.column)
                current.column = typeid_cast<const ColumnAggregateFunction &>(*current.column).convertToValues();
        }
    }
}



Block RollupBlockInputStream::getHeader() const
{
    Block res = children.at(0)->getHeader();
    finalize(res);
    return res;
}


Block RollupBlockInputStream::readImpl()
{
    Block finalized;
    Block block;

    while(1)
    {

        block = children[0]->read();

        /// Block with values not included in `max_rows_to_group_by`. We'll postpone it.
        if (!block)
            return finalized;
       
        blocks.push_back(block);
        auto rollup_block = std::move(block);

        for (int i = static_cast<int>(params.keys_size) - 1; i >= 0; --i) 
        {
            auto & current = rollup_block.getByPosition(params.keys[i]);
            current.column = current.column->cloneEmpty()->cloneResized(rollup_block.rows());

            Aggregator aggregator(params);

            auto result = std::make_shared<AggregatedDataVariants>();

            StringRefs key(params.keys_size);
            ColumnRawPtrs key_columns(params.keys_size);
            AggregateColumns aggregate_columns(params.aggregates_size);

            bool no_more_keys = false;

            aggregator.executeOnBlock(rollup_block, *result, key_columns, aggregate_columns, key, no_more_keys);
            aggregator.mergeStream(children.back(), *result, 1);
            auto current_blocks = aggregator.convertToBlocks(*result, true, 1);
            blocks.insert(blocks.end(), current_blocks.begin(), current_blocks.end());
        }

        finalized = aggregator.mergeBlocks(blocks, false);
        finalize(finalized);

        total_keys += finalized.rows();
        // createRollupScheme(block);
        // addToTotals(block);
        // executeRollup(block);

        if (!finalized)
            continue;

        passed_keys += finalized.rows();
        return finalized;
    }
}

void RollupBlockInputStream::createRollupScheme(const Block & block)
{
    size_t num_columns = block.columns();
    group_borders.resize(not_aggregate_columns);
    size_t total = 1;
    for(size_t i = 0; i + 1 < not_aggregate_columns; ++i)
    {
        const ColumnWithTypeAndName & current = block.getByPosition(i);
        const auto & column = current.column.get();
        size_t size = column->size();
        for (size_t j = 1; j < size; ++j) 
        {
            if (column->getDataAt(j) != column->getDataAt(j - 1))
            {
                ++total;
                group_borders[i].push_back(j);
            }
        }
        ++total;
        group_borders[i].push_back(block.rows());
    }

    for (size_t i = 0; i < num_columns; ++i)
    {
        const ColumnWithTypeAndName & current = block.getByPosition(i);
        if (const ColumnAggregateFunction * column = typeid_cast<const ColumnAggregateFunction *>(current.column.get()))
        {
            IAggregateFunction * function = column->getAggregateFunction().get();
            auto & target = typeid_cast<ColumnAggregateFunction &>(*current_totals[i]);
            for (size_t j = 0; j < total; ++j)
            {   
                AggregateDataPtr data = arena->alloc(function->sizeOfData());
                function->create(data);
                target.getData().push_back(data);
            }
        }
        else 
        {
            for (size_t j = 0; j < total; ++j)
                current.type->insertDefaultInto(*current_totals[i]);
        }
    }
    // LOG_DEBUG(&Logger::get("Rollup"), "rollup size: " << rollups.size());
    // for(auto p : rollups)
    //     LOG_DEBUG(&Logger::get("Rollup"), "rollup: (" << p.first << ", " << p.second << ")");
}

void RollupBlockInputStream::executeRollup(const Block & block )
{
    // if (!not_aggregate_columns)    
    //     return;

    LOG_DEBUG(&Logger::get("Rollup"), "execute rollup");
    for (size_t i = not_aggregate_columns; i < block.columns(); ++i)
    {
        const ColumnWithTypeAndName & current = block.getByPosition(i);
        const ColumnAggregateFunction * column = typeid_cast<const ColumnAggregateFunction *>(current.column.get());

        LOG_DEBUG(&Logger::get("Rollup"), "name: " << current.name);

        auto & target = typeid_cast<ColumnAggregateFunction &>(*current_totals[i]);
        IAggregateFunction * function = target.getAggregateFunction().get();
        AggregateDataPtr data = target.getData()[0];

        const ColumnAggregateFunction::Container & vec = column->getData();
        size_t size = vec.size();
        LOG_DEBUG(&Logger::get("Rollup"), "size: " << size);
        int current_pos = 0;

        for (int j = static_cast<int>(not_aggregate_columns) - 2; j >= 0; --j)
        {
            size_t ptr = 0;
            for (size_t k = 0; k < size; ++k)
            {
                if (k == group_borders[j][ptr])
                {
                    ++ptr;
                    ++current_pos;
                    data = target.getData()[current_pos];
                }
                function->merge(data, vec[k], arena.get());
            }
            ++current_pos;
        }
        data = target.getData()[current_pos];
        for (size_t j = 0; j < size; ++j)
            function->merge(data, vec[j], arena.get());
    }
}


void RollupBlockInputStream::addToTotals(const Block & block)
{
    LOG_DEBUG(&Logger::get("Rollup"), "add totals in rollup");
    LOG_DEBUG(&Logger::get("Rollup"), "block rows : " << block.rows());
    for (size_t i = 0, num_columns = block.columns(); i < num_columns; ++i)
    {
        const ColumnWithTypeAndName & current = block.getByPosition(i);
        LOG_DEBUG(&Logger::get("Rollup"), "name: " + current.name);
        if (const ColumnAggregateFunction * column = typeid_cast<const ColumnAggregateFunction *>(current.column.get()))
        {
            auto & target = typeid_cast<ColumnAggregateFunction &>(*current_totals[i]);
            IAggregateFunction * function = target.getAggregateFunction().get();
            AggregateDataPtr data = target.getData()[0];

            /// Accumulate all aggregate states into that value.


            const ColumnAggregateFunction::Container & vec = column->getData();
            size_t size = vec.size();

            for (size_t j = 0; j < size; ++j)
            {   
                function->merge(data, vec[j], arena.get());
            }
        }
    }
}

}
