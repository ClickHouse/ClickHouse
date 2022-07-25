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
        // if (current_input != inputs.end())
        //     current_input->setNotNeeded();
        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "!output.isNeeded() - PortFull");

        return Status::PortFull;
    }

    if (!output.canPush())
    {
        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "!output.canPush() - PortFull");
        return Status::PortFull;
    }

    /// Check can input.
    for (;;)
    {
        // if (inp == num_inputs)
        // {
        //     inp = 0;
        //     current_input = inputs.begin();
        // }

        assert(current_input != inputs.end());
        bool all_finished = true;
        bool left_read = false;

        if (!current_input->isFinished())
        {
            all_finished = false;
            if (current_input == inputs.begin())
            { // left
                left_read = false;
                assert(!inp);
                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "reading left");
                //  only when no right input, that refers
                //    active block
                bool left_in_use = false;
                for (size_t right_inp = 1; right_inp < num_inputs; ++right_inp)
                {
                    if (status[right_inp].left_rest)
                    {
                        left_in_use = true;
                        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "left in use");
                        goto EOFL;
                    }
                }

                if (!left_in_use)
                {
                    current_input->setNeeded();
                    if (current_input->hasData())
                    {
                        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "not left in use - has data");
                        chunk = current_input->pull(true);
                        status[0].block = current_input->getHeader().cloneWithColumns(chunk.detachColumns());

                        auto & block = status[0].block;
                        auto const & col = block.getByPosition(0).column;

                        left_read = true;

                        // remove duplicates
                        IColumn::Filter filt;
                        size_t shift = 1;
                        size_t row = 0;
                        for (; row+shift < col->size();)
                        {
                            auto comp = col->compareAt(row, row + shift, *col, 0);
                            if (comp)
                            {
                                row = row + shift;
                                filt[row + shift] = 1;
                            }
                            else
                            {
                                filt[row + shift] = 0;
                                row++;
                            }

                            shift++;
                        }
                        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "duplicates are removed");
                    }
                    else
                    {
                        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "no data for left");
                        //   needdata and return ?
                    }
                }
            }
            else
            { // right
                if (!left_read && status[inp].right_rest)
                {
                    LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "need left while it is not read");
                    goto EOFL;
                }
                if (!status[inp].current_pos) // status[inp].left_rest ?
                {
                    current_input->setNeeded();
                    if (current_input->hasData())
                    {
                        chunk = current_input->pull(true);
                        status[inp].block = current_input->getHeader();
                        status[inp].block.setColumns(chunk.detachColumns());
                    }
                    else if (!status[inp].right_rest)
                    {
                        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "no right, but cannot read - continue");
                        goto EOFL;
                    }
                }
                Block & add_block = status[inp].block;

                size_t & left_row = status[inp].left_pos;
                size_t & right_row = status[inp].current_pos;
                auto const & left_col = status[0].block.getByPosition(0).column;  //safeGetByPosition ??
                auto const & right_col = add_block.getByPosition(0).column;

                for (;;)
                {
                    // less, equal, greater than rhs[m] respectively
                    auto comp = left_col->compareAt(left_row, right_row, *right_col, 0);
                    LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "comp {}", comp);
                    if (comp < 0)
                    {
                        left_filt[left_row] = 1;
                        left_row++;
                        if (left_row == left_col->size())
                        {
                            status[inp].right_rest = true;
                            break;
                        }
                    }
                    if (comp > 0)
                    {
                        status[inp].right_filt[right_row] = 1;
                        right_row++;
                        if (right_row == right_col->size())
                        {
                            status[inp].left_rest = true;
                            break;
                        }
                    }
                    else
                    {
                        status[inp].right_filt[right_row] = -left_row-1;
                        left_row++;
                        right_row++;

                        if (left_row == left_col->size() && right_row == right_col->size())
                        {
                            break;
                        }
                        else if (left_row == left_col->size())
                        {
                            status[inp].right_rest = true;
                            break;
                        }
                        else if (right_row == right_col->size())
                        {
                            status[inp].left_rest = true;
                            break;
                        }
                    }
                }


                if (status[inp].right_rest)
                {
                    current_input->setNeeded();
                    // return Status::NeedData;  // sequence guaranteed ?

                }
                // else if (status[inp].left_rest)
                // {
                // }

                // if (!status[inp].right_rest)
                // {
                //     right_col.filter(filt, -1);
                //     LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "added {} columns", i);
                // }
            }
          EOFL:


            // current_input->setNeeded();

            // if (!input.hasData())
            //     return Status::NeedData;

            /// Move data.
            if (inp == num_inputs - 1) /*++current_input == inputs.end()*/
            {
                bool left_in_use = false;  // ???

                for (size_t right_inp = 1; right_inp < num_inputs; ++right_inp)
                {
                    if (status[right_inp].left_rest)
                    {
                        left_in_use = true;
                    }
                }
                if (!left_in_use)
                {
                    LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "inputs.end - not left in use");

                    if (!status[0].block.rows())
                    {
                        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "no left block - returning NeedData");
                        return Status::NeedData;

                    }


                    for (size_t right_inp = 1; right_inp < num_inputs; ++right_inp)
                    {
                        IColumn::Filter filt(status[right_inp].right_filt.size());

                        for (size_t f = 0; f < status[right_inp].right_filt.size(); ++f)
                        {
                            auto filt_val = status[right_inp].right_filt[f];
                            if (filt_val < 0)
                            {
                                filt[f] = left_filt[filt_val - 1] ? 1 : 0;
                            }
                            // adjust current_pos ?
                        }

                        Block & add_block = status[inp].block;
                        // add_block.setColumns(chunk.detachColumns());
                        size_t i = 0;
                        for (; i < status[inp].block.columns(); ++i)
                        {
                            add_block.getByPosition(i).column = add_block.getByPosition(i).column->filter(filt, -1);

#if 1
                            // block.insert(add_block.getByPosition(i));
                            auto new_col = add_block.getByPosition(i).column->cloneEmpty();
                            new_col->insertRangeFrom(*add_block.getByPosition(i).column, 0, status[inp].current_pos);

                            // status[0].block.column.insertRangeFrom(add_block.getByPosition(i).column, 0, status[inp].current_pos);
                            status[0].block.insert(ColumnWithTypeAndName(std::move(new_col), add_block.getByPosition(i).type, add_block.getByPosition(i).name));
#else
                            // block.insert(add_block.getByPosition(i));
                            auto new_col = add_block.getByPosition(i).cloneEmpty();
                            auto new_col_col = IColumn::mutate(std::move(new_col.column));

                            new_col_col->insertRangeFrom(*add_block.getByPosition(i).column, 0, status[inp].current_pos);
                            new_col.column = std::move(new_col_col);


                            // status[0].block.column.insertRangeFrom(add_block.getByPosition(i).column, 0, status[inp].current_pos);
                            status[0].block.insert(std::move(new_col));
#endif
                            add_block.getByPosition(i).column = add_block.getByPosition(i).column->cut(0, status[inp].current_pos);
                            status[inp].current_pos = 0;
                        }
                    }
                }

                // output.push(block);
                Chunk output_chunk;

                output_chunk.setColumns(status[0].block.getColumns(), status[0].block.rows());
                output.push(std::move(output_chunk));

                current_input = inputs.begin();
                inp = 0;

                if (all_finished)
                {
                    output.finish();
                    LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "returning FInished");
                    return Status::Finished;
                }
                else
                {
                    /// Now, we pushed to output, and it must be full.
                    LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "returning PortFull");
                    return Status::PortFull;
                }
            }
            else
            {
                ++inp;
                ++current_input;
                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "going to next input {}", inp);
            }

        }
        else
        {
            LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "current input is finished");
            ++inp;
            ++current_input;
        }
    }
    assert(!"end of loop");

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
