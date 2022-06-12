#include <Processors/Transforms/JoiningTransform.h>
#include <Interpreters/ExpressionAnalyzer.h>
#include <Interpreters/join_common.h>

#include <Common/logger_useful.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
}

Block JoiningTransform::transformHeader(Block header, const JoinPtr & join)
{
    LOG_DEBUG(&Poco::Logger::get("JoiningTransform"), "Before join block: '{}'", header.dumpStructure());
    join->checkTypesOfKeys(header);
    ExtraBlockPtr tmp;
    join->joinBlock(header, tmp);
    LOG_DEBUG(&Poco::Logger::get("JoiningTransform"), "After join block: '{}'", header.dumpStructure());
    return header;
}

JoiningTransform::JoiningTransform(
    Block input_header,
    JoinPtr join_,
    size_t max_block_size_,
    bool on_totals_,
    bool default_totals_,
    FinishCounterPtr finish_counter_)
    : IProcessor({input_header}, {transformHeader(input_header, join_)})
    , join(std::move(join_))
    , on_totals(on_totals_)
    , default_totals(default_totals_)
    , finish_counter(std::move(finish_counter_))
    , max_block_size(max_block_size_)
{
    if (!join->isFilled())
        inputs.emplace_back(Block(), this);
}

IProcessor::Status JoiningTransform::prepare()
{
    auto & output = outputs.front();

    /// Check can output.
    if (output.isFinished() || stop_reading)
    {
        output.finish();
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto & input : inputs)
            input.setNotNeeded();
        return Status::PortFull;
    }

    /// Output if has data.
    if (has_output)
    {
        output.push(std::move(output_chunk));
        has_output = false;

        return Status::PortFull;
    }

    if (inputs.size() > 1)
    {
        auto & last_in = inputs.back();
        if (!last_in.isFinished())
        {
            last_in.setNeeded();
            if (last_in.hasData())
                throw Exception(ErrorCodes::LOGICAL_ERROR, "No data is expected from second JoiningTransform port");

            return Status::NeedData;
        }
    }

    if (has_input)
        return Status::Ready;

    auto & input = inputs.front();
    if (input.isFinished())
    {
        if (process_non_joined)
            return Status::Ready;

        output.finish();
        return Status::Finished;
    }

    input.setNeeded();

    if (!input.hasData())
        return Status::NeedData;

    input_chunk = input.pull(true);
    has_input = true;
    return Status::Ready;
}

void JoiningTransform::work()
{
    if (has_input)
    {
        transform(input_chunk);
        output_chunk.swap(input_chunk);
        has_input = not_processed != nullptr;
        has_output = !output_chunk.empty();
    }
    else
    {
        if (!non_joined_blocks)
        {
            if (!finish_counter || !finish_counter->isLast())
            {
                process_non_joined = false;
                return;
            }

            non_joined_blocks = join->getNonJoinedBlocks(
                inputs.front().getHeader(), outputs.front().getHeader(), max_block_size);
            if (!non_joined_blocks)
            {
                process_non_joined = false;
                return;
            }
        }

        Block block = non_joined_blocks->read();
        if (!block)
        {
            process_non_joined = false;
            return;
        }

        auto rows = block.rows();
        output_chunk.setColumns(block.getColumns(), rows);
        has_output = true;
    }
}

void JoiningTransform::transform(Chunk & chunk)
{
    if (!initialized)
    {
        initialized = true;

        if (join->alwaysReturnsEmptySet() && !on_totals)
        {
            stop_reading = true;
            chunk.clear();
            return;
        }
    }

    Block block;
    if (on_totals)
    {
        const auto & left_totals = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());
        const auto & right_totals = join->getTotals();

        /// Drop totals if both out stream and joined stream doesn't have ones.
        /// See comment in ExpressionTransform.h
        if (default_totals && !right_totals)
            return;

        block = outputs.front().getHeader().cloneEmpty();
        JoinCommon::joinTotals(left_totals, right_totals, join->getTableJoin(), block);
    }
    else
        block = readExecute(chunk);

    auto num_rows = block.rows();
    chunk.setColumns(block.getColumns(), num_rows);
}

Block JoiningTransform::readExecute(Chunk & chunk)
{
    Block res;

    if (!not_processed)
    {
        if (chunk.hasColumns())
            res = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());

        if (res)
            join->joinBlock(res, not_processed);
    }
    else if (not_processed->empty()) /// There's not processed data inside expression.
    {
        if (chunk.hasColumns())
            res = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());

        not_processed.reset();
        join->joinBlock(res, not_processed);
    }
    else
    {
        res = std::move(not_processed->block);
        join->joinBlock(res, not_processed);
    }

    return res;
}

FillingRightJoinSideTransform::FillingRightJoinSideTransform(Block input_header, JoinPtr join_)
    : IProcessor({input_header}, {Block()})
    , join(std::move(join_))
{}

InputPort * FillingRightJoinSideTransform::addTotalsPort()
{
    if (inputs.size() > 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Totals port was already added to FillingRightJoinSideTransform");

    return &inputs.emplace_back(inputs.front().getHeader(), this);
}

IProcessor::Status FillingRightJoinSideTransform::prepare()
{
    auto & output = outputs.front();

    /// Check can output.
    if (output.isFinished())
    {
        for (auto & input : inputs)
            input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        for (auto & input : inputs)
            input.setNotNeeded();
        return Status::PortFull;
    }

    auto & input = inputs.front();

    if (stop_reading)
    {
        input.close();
    }
    else if (!input.isFinished())
    {
        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        chunk = input.pull(true);
        return Status::Ready;
    }

    if (inputs.size() > 1)
    {
        auto & totals_input = inputs.back();
        if (!totals_input.isFinished())
        {
            totals_input.setNeeded();

            if (!totals_input.hasData())
                return Status::NeedData;

            chunk = totals_input.pull(true);
            for_totals = true;
            return Status::Ready;
        }
    }
    else if (!set_totals)
    {
        chunk.setColumns(inputs.front().getHeader().cloneEmpty().getColumns(), 0);
        for_totals = true;
        return Status::Ready;
    }

    output.finish();
    return Status::Finished;
}

void FillingRightJoinSideTransform::work()
{
    auto block = inputs.front().getHeader().cloneWithColumns(chunk.detachColumns());

    if (for_totals)
        join->setTotals(block);
    else
        stop_reading = !join->addJoinedBlock(block);

    set_totals = for_totals;
}

IProcessor::Status ParallelJoinTransform::prepare()
{
    LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "top of prepare");
    auto & output = outputs.front();

    /// Check can output.

    if (output.isFinished())
    {
        for (; current_input != inputs.end(); ++current_input)
            current_input->close();

        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "output.isFinished() - Finished");
        return Status::Finished;
    }

    if (!output.isNeeded())
    {
        if (current_input != inputs.end())
            current_input->setNotNeeded();
        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "!output.isNeeded() - PortFull");

        return Status::PortFull;
    }

    if (!output.canPush())
    {
        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "!output.canPush() - PortFull");
        return Status::PortFull;
    }


    /// Check can input.




    for (;; )
    {
        assert(current_input != inputs.end());


        if (!current_input->isFinished())
        {
            current_input->setNeeded();
            if (!current_input->hasData())
            {
                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "returning NeedData");

                return Status::NeedData;
            }

            // input.setNeeded();
            chunk = current_input->pull(true);

            if (current_input == inputs.begin())
            {
                block = current_input->getHeader().cloneWithColumns(chunk.detachColumns());
            }
            else
            {
                Block add_block = current_input->getHeader();
                add_block.setColumns(chunk.detachColumns());
                size_t i = 0;
                for (; i < add_block.columns(); ++i)
                {
                    block.insert(add_block.getByPosition(i));
                }
                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "added {} columns", i);

            }

            // auto & input = *current_input;

            current_input->setNeeded();

            // if (!input.hasData())
            //     return Status::NeedData;

            /// Move data.
            if (++current_input == inputs.end())
            {
                // output.push(block);
                Chunk output_chunk;

                output_chunk.setColumns(block.getColumns(), block.rows());
                output.push(std::move(output_chunk));


                current_input = inputs.begin();


                /// Now, we pushed to output, and it must be full.
                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "returning PortFull");
                return Status::PortFull;
            }

        }
        else
        {
            break;
        }
    }

    output.finish();
    LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "returning FInished");
    return Status::Finished;
}

void ParallelJoinTransform::work()
{
    LOG_DEBUG(&Poco::Logger::get("JoiningTransform"), "ParallelJoinTransform, top of work");
    // current_output_chunk = std::move(current_input_chunk);
}

void ParallelJoinTransform::addHeader(Block block_)
{
    input_headers.push_back(block_);
    inputs.emplace_back(block_, this);
    current_input = inputs.begin();
}

Block ParallelJoinTransform::getHeader()
{
    Block b;
    for (const auto & ih : input_headers)
    {
        for (size_t i = 0; i < ih.columns(); ++i)
        {
            b.insert(ih.getByPosition(i));
        }
    }


    return b;
}

void ParallelJoinTransform::mk_ports()
{
    // inputs = input_headers;

    // auto & first = input_headers.front();
    // for(size_t block_num = 1; block_num < input_headers; ++block_num)
    // {
    //     auto & current = input_headers[block_num];
    //     for ( auto it = current.begin(); it != current.end(); ++it)
    //     {
    //         first.insert(*it);
    //     }
    // }
    // outputs.emplace_back(first) ;

}





}
