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


std::optional<IProcessor::Status> ParallelJoinTransform::processLeft()
{
    assert(current_input == inputs.begin());

    { // left
        // assert(!left_read);
        assert(!inp);
        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "reading left");
        //  only when no right input, that refers
        //    active block
        if (!status[0].blocks.empty())
        {
            LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "left already read");
            inp++;
            ++current_input;
            // continue
            return std::nullopt;
        }



        bool left_in_use = false;
        for (size_t right_inp = 1; right_inp < num_inputs; ++right_inp)
        {
            if (status[right_inp].left_rest)
            {
                left_in_use = true;
                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "left in use");
                return std::nullopt;
            }
        }

        if (!left_in_use)  // not needed ?
        {
            current_input->setNeeded();
            if (current_input->hasData())
            {
                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "not left in use - has data");
                chunk = current_input->pull(true);

                current_input->setNeeded();

                // only one left block
                status[0].blocks.resize(1);

                status[0].blocks[0] = current_input->getHeader().cloneWithColumns(chunk.detachColumns());
                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "read left block with {} rows", status[0].blocks[0].rows());

                auto & block = status[0].blocks[0];
                auto const & col = block.getByPosition(0).column;

                // left_read = true;

                // remove duplicates
                IColumn::Filter filt(col->size());
                size_t row = 0;
                size_t shift = 1;
                filt[0] = 1;
                for (; shift < col->size();)
                {
                    auto comp = col->compareAt(row, shift, *col, 0);
                    if (!comp)
                    {
                        filt[shift] = 0;
                        row = shift;
                    }
                    else
                    {
                        filt[shift] = 1;
                    }

                    shift++;
                }
                for (size_t i = 0; i < block.columns(); ++i)
                {
                    block.getByPosition(i).column = block.getByPosition(i).column->filter(filt, -1);
                }
                left_filt.resize(block.rows());
                for (auto & elem : left_filt)
                {
                    elem = 1;
                }

                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "duplicates are removed");
            }
            else
            {
                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "no data for left");
                return std::nullopt;
                //   needdata and return ?
            }
        }
    }
    return std::nullopt;

}

std::optional<IProcessor::Status> ParallelJoinTransform::processRight()
{
    if (status[0].blocks.empty() /* || (!status[inp].blocks.empty() && !status[inp].left_rest)*/)
    {
        if (inputs.begin()->isFinished())
        {
            return std::nullopt;
        }

        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "need left while it is not read");
        inputs.begin()->setNeeded();
        inp = 0;
        current_input = inputs.begin();
        // continue;
        return std::nullopt;
        // goto EOFL;
    }
    if (!status[inp].blocks.empty() && !status[inp].left_rest && !status[inp].right_rest)
    {
        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "!status[inp].blocks.empty() && !status[inp].left_rest) && !status[inp].right_rest");
        return std::nullopt;
    }

    // if (!status[inp].current_pos) // status[inp].left_rest ?
    // {
    //     assert(!status[inp].left_rest);
    //     assert(status[inp].blocks.empty());
    current_input->setNeeded();
    if (!status[inp].right_rest && current_input->hasData())
    {
        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "has data for input {}", inp);
        // current_input->setNotNeeded();

        chunk = current_input->pull(true);
        // status[inp].current_block++;
        auto & inserted_block = status[inp].blocks.emplace_back(current_input->getHeader());
        // status[inp].blocks[status[inp].current_block] = current_input->getHeader();
        inserted_block.setColumns(chunk.detachColumns());
        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "read right block with {} rows", inserted_block.rows());

        current_input->setNeeded();
    }
    else if (!status[inp].right_rest)
    {
        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "no right, but cannot read - continue; isFinished {}",
            current_input->isFinished());
        return std::nullopt;
    }
    // }
    // else
    // {
    //     assert(status[inp].left_rest);
    //     // append block

    //     LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "current_pos {} for input {}", status[inp].current_pos, inp);
    // }

    if (status[0].blocks.empty() || !status[0].blocks[0].rows())
    {
        // assert(!left_read);
        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "No left - setting right_rest for input {}", inp);
        status[inp].right_rest = true;
        return std::nullopt;
    }

    Block & add_block = status[inp].blocks.back();

    size_t & left_row = status[inp].left_pos;
    size_t & right_row = status[inp].current_pos;
    auto const & left_col = status[0].blocks[0].getByPosition(0).column;  //safeGetByPosition ??
    auto const & right_col = add_block.getByPosition(0).column;


    size_t total_rows = std::accumulate(status[inp].blocks.begin(), status[inp].blocks.end(), 0, [](size_t s, const Block & b)
    {
        return s + b.rows();
    });

    LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "total_rows is {}, current_pos {}", total_rows, status[inp].current_pos);

    status[inp].right_filt.resize(total_rows);
    status[inp].left_rest = false;
    status[inp].right_rest = false;

    for (size_t right_block_shift = right_row; ;)
    {
        // less, equal, greater than rhs[m] respectively
        auto comp = left_col->compareAt(left_row, right_row - right_block_shift, *right_col, 0);
        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "comp {}, left_row {}, right_row {}", comp, left_row, right_row);
        if (comp < 0)
        {
            left_filt[left_row] = 0;
            left_row++;
            if (left_row == left_col->size())
            {
                // if (current_right_block == status[inp].blocks.size() - 1)
                // {
                status[inp].right_rest = true;
                break;
                // }
                // else
                // {
                //     ++current_right_block;
                //     add_block = status[inp].blocks[current_right_block];
                //     right_block_shift = right_row;
                // }
            }
        }
        else if (comp > 0)
        {
            status[inp].right_filt[right_row] = 1;
            right_row++;
            if (right_row - right_block_shift == right_col->size())
            {
                // if (current_right_block == status[inp].blocks.size() - 1)
                // {
                status[inp].left_rest = true;
                break;
                // }
                // else
                // {
                //     ++current_right_block;
                //     add_block = status[inp].blocks[current_right_block];
                //     right_block_shift = right_row;
                // }
            }
        }
        else
        {
            status[inp].right_filt[right_row] = -left_row-1;
            left_row++;
            right_row++;

            if (left_row == left_col->size() && right_row - right_block_shift == right_col->size() /*&& current_right_block == status[inp].blocks.size() - 1*/)
            {
                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "left_row == left_col->size() && right_row == right_col->size(); left_row {}, right_row {}", left_row, right_row);
                break;
            }
            else if (left_row == left_col->size())
            {
                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "left_row == left_col->size(); left_row {}, right_row {}", left_row, right_row);
                status[inp].right_rest = true;
                break;
            }
            else if (right_row - right_block_shift == right_col->size() /* && current_right_block == status[inp].blocks.size() - 1*/)
            {
                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "right_row == right_col->size(); left_row {}, right_row {}", left_row, right_row);
                status[inp].left_rest = true;
                break;
            }
            // else if (current_right_block == status[inp].blocks.size() - 1)
            // {
            //     ++current_right_block;
            //     add_block = status[inp].blocks[current_right_block];
            //     right_block_shift = right_row;
            // }

        }
    }

    if (!status[inp].left_rest)
    {
        status[inp].left_pos = 0;
    }

    if (current_input->isFinished())
    {
        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "finished - clear flags");
        status[inp].left_rest = false;
        // status[inp].right_rest = false;
    }
    else if (status[inp].left_rest)
    {
        current_input->setNeeded();
        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "left_rest - continue");
        // continue;
        return std::nullopt;

        // return Status::NeedData;  // sequence guaranteed ?

    }
    return std::nullopt;
}

void ParallelJoinTransform::finish()
{
    LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "finish");
    outputs.front().finish();
    for (auto & inp_to_close : inputs)
    {
        inp_to_close.close();
    }
}


std::optional<IProcessor::Status> ParallelJoinTransform::topCheck()
{

    LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "top of loop for input {}", inp);
    bool need_data = true;

    size_t cur_inp_num = 0;

    auto cur_inp = inputs.begin();
    LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "input {}, hasData {}, isFinished {}, right_rest {}, left_rest {} blocks {}", cur_inp_num, cur_inp->hasData(), cur_inp->isFinished(), status[cur_inp_num].right_rest, status[cur_inp_num].left_rest, status[cur_inp_num].blocks.size());
    bool left_already_read = !status[cur_inp_num].blocks.empty();
    bool left_finished = cur_inp->isFinished();

    ++cur_inp;
    ++cur_inp_num;

    bool left_in_use = false;
    bool all_finished = true;
    bool all_have_block = true;


    for (; cur_inp != inputs.end(); ++cur_inp, ++cur_inp_num)
    {
        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "input {}, hasData {}, isFinished {}, right_rest {}, left_rest {} blocks {}", cur_inp_num, cur_inp->hasData(), cur_inp->isFinished(), status[cur_inp_num].right_rest, status[cur_inp_num].left_rest, status[cur_inp_num].blocks.size());

        if (status[cur_inp_num].blocks.empty())
        {
            all_have_block = false;
        }
        if (cur_inp->isFinished())
        {
            if (status[cur_inp_num].blocks.empty())
            {
                finish();

                return Status::Finished;
            }
        }
        else if (!left_already_read || cur_inp->hasData())
        {
            need_data = false;
            cur_inp->setNeeded();
            all_finished = false;
        }
        else
        {
            all_finished = false;
        }

        if (status[cur_inp_num].left_rest)
        {
            left_in_use = true;
            LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "topcheck - left_in_use");
        }
    }
    if (all_finished && all_have_block && (left_already_read || !left_finished))
    {
        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "topcheck - all_finished && all_have_block - go further");
        return std::nullopt;
    }

    if (inputs.begin()->hasData())
    {
        if (left_in_use)
        {
            assert(!status[0].blocks.empty());
            LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "topcheck - left not needed");
            inputs.begin()->setNotNeeded();
        }
    }
    else if (!left_already_read)
    {
        inp = 0;
        current_input = inputs.begin();
        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "topcheck - needdata left");
        if (left_finished)
        {
            finish();

            return Status::Finished;
        }

        inputs.begin()->setNeeded();
        return Status::NeedData;
    }

    if (left_already_read && need_data)
    {
        inp = 1;
        current_input = inputs.begin();
        current_input++;
        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "topcheck - need_data && left_ready_read - needdata");
        return Status::NeedData;
    }
    LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "topcheck - go further");
    return std::nullopt;
}


std::optional<IProcessor::Status> ParallelJoinTransform::endOfInputs()
{
    LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "inputs.end - not left in use");


    {
        size_t cur_inp_num = 0;
        bool needed = false;
        for (auto cur_inp = inputs.begin(); cur_inp != inputs.end(); ++cur_inp, ++cur_inp_num)
        {
            if (status[cur_inp_num].blocks.empty() && !cur_inp->isFinished() && !inputs.begin()->isFinished()/*status[cur_inp_num].blocks[0].rows()*/)
            {
                cur_inp->setNeeded();
                needed = true;
                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "input {} does not have block", cur_inp_num);
            }
        }
        if (needed)  // ???
        {
            inp = 0;
            current_input = inputs.begin();
            return Status::NeedData;
        }
    }


    if (status[0].blocks.empty() || !status[0].blocks[0].rows())
    {
        if (inputs.begin()->isFinished())
        {
            return std::nullopt;
        }

        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "no left block - continue");
        // inputs.begin()->setNeeded();
        // inp = 0;
        // current_input = inputs.begin();

        // return Status::NeedData;
    }

    Block & left_block = status[0].blocks[0];

    LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "left block before filter has {} rows", left_block.rows());
    for (size_t i = 0; i < left_block.columns(); ++i)
    {
        left_block.getByPosition(i).column = left_block.getByPosition(i).column->filter(left_filt, -1);
    }
    LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "left block after filter has {} rows", left_block.rows());

    for (size_t right_inp = 1; right_inp < num_inputs; ++right_inp)
    {

        assert(!status[right_inp].blocks.empty());

        std::vector<IColumn::Filter> filts(status[right_inp].blocks.size());


        // size_t block_shift;
        for (size_t filt_shift = 0, block_num = 0; block_num < filts.size(); ++block_num)
        {
            size_t filt_len = (block_num == filts.size() - 1) ?
                status[right_inp].current_pos - filt_shift :
                status[right_inp].blocks[block_num].rows();

            LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "filts for input {}, block {} has length {} (block length is {}, current_pos {}, filt_shift {}",
                right_inp, block_num, filt_len, status[right_inp].blocks[block_num].rows(), status[right_inp].current_pos, filt_shift);

            filts[block_num].resize(filt_len);

            for (size_t filt_row = 0; filt_row < filts[block_num].size(); ++filt_row, ++filt_shift)
            {
                auto filt_val = status[right_inp].right_filt[filt_shift];
                if (filt_val < 0)
                {
                    if (!left_filt[-filt_val - 1])
                    {
                        filts[block_num][filt_row] = 0;
                        // num_filtered_out++;
                    }
                    else
                    {
                        filts[block_num][filt_row] = 1;
                    }
                }
                else
                {
                    filts[block_num][filt_row] = 0;
                }
            }
        }



        // IColumn::Filter filt(status[right_inp].right_filt.size());
        // size_t num_filtered_out = 0; // per block !!!
        // for (size_t f = 0; f < status[right_inp].right_filt.size(); ++f)
        // {
        //     auto filt_val = status[right_inp].right_filt[f];
        //     if (filt_val < 0)
        //     {
        //         if (left_filt[-filt_val - 1])
        //         {
        //             filt[f] = 1;
        //             num_filtered_out++;
        //         }
        //         else
        //         {
        //             filt[f] = 0;
        //         }

        //     }
        //     // adjust current_pos ?
        // }



        // add_block.setColumns(chunk.detachColumns());
        // size_t filter_shift = 0;
        size_t i = 0;
        auto const & sample_block = status[right_inp].blocks[0];
        auto & last_block = status[right_inp].blocks.back();
        for (; i < status[right_inp].blocks[0].columns(); ++i)
        {
            auto new_col = sample_block.getByPosition(i).cloneEmpty();
            auto new_col_col = IColumn::mutate(std::move(new_col.column));

            // auto new_col = sample_block.getByPosition(i).column->cloneEmpty();
            LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "right_inp {}, column {} has {} blocks",
                right_inp, i, status[right_inp].blocks.size());

            size_t current_block = 0;
            for (; current_block < status[right_inp].blocks.size() - 1; ++current_block)
            {
                Block & add_block = status[right_inp].blocks[current_block];
                // add_block.getByPosition(i).column = add_block.getByPosition(i).column->filter(filts[current_block], -1);

                // size_t num_rows = filts[current_block].size();
                // block.insert(add_block.getByPosition(i));
                // new_col->insertRangeFrom(*add_block.getByPosition(i).column, 0, num_rows);

                // status[0].block.column.insertRangeFrom(add_block.getByPosition(i).column, 0, status[right_inp].current_pos);

                // block.insert(add_block.getByPosition(i));
                // auto filtered_col = add_block.getByPosition(i).column->filter(filts[current_block], -1);


                // new_col_col->insertRangeFrom(*add_block.getByPosition(i).column, 0, num_rows);
                // new_col_col->insertRangeFrom(*filtered_col, 0, filtered_col->size());




                add_block.getByPosition(i).column = add_block.getByPosition(i).column->filter(filts[current_block], -1);
                new_col_col->insertRangeFrom(*add_block.getByPosition(i).column, 0, add_block.getByPosition(i).column->size());


                // new_col.column = std::move(new_col_col);


                // status[0].block.column.insertRangeFrom(add_block.getByPosition(i).column, 0, status[right_inp].current_pos);
                // new_col.column = new_col_col->filter(filts[current_block], -1); // try to filter before
                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "column to insert has {} rows for block {}", new_col_col->size(), current_block);
            }

            assert(current_block == status[right_inp].blocks.size() - 1);
            size_t num_rows = last_block.getByPosition(i).column->size();
            LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "num_rows {}, filts[current_block].size() {}", num_rows, filts[current_block].size());
            if (num_rows != filts[current_block].size())
            {
                assert(num_rows > filts[current_block].size());
                assert(status[right_inp].right_rest);

                auto part_col = last_block.getByPosition(i).column->cut(0, filts[current_block].size());
                auto filtered_col = part_col->filter(filts[current_block], -1);
                new_col_col->insertRangeFrom(*filtered_col, 0, filtered_col->size());
                // new_col.column = std::move(new_col_col);

                status[right_inp].blocks[0].getByPosition(i).column = last_block.getByPosition(i).column->cut(
                    filts[current_block].size(), num_rows - filts[current_block].size());

                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "cut {} rows", status[right_inp].blocks[0].getByPosition(i).column->size());
            }
            else
            {
                assert(!status[right_inp].right_rest);
                last_block.getByPosition(i).column = last_block.getByPosition(i).column->filter(filts[current_block], -1);
                new_col_col->insertRangeFrom(*last_block.getByPosition(i).column, 0, last_block.getByPosition(i).column->size());
                // new_col.column = std::move(*new_col_col);




                //   works
                // new_col_col->insertRangeFrom(*last_block.getByPosition(i).column, 0, filts[current_block].size());
                // new_col.column = new_col_col->filter(filts[current_block], -1); // try to filter before



                // does not work
                // new_col_col->insertRangeFrom(*last_block.getByPosition(i).column->filter(filts[current_block], -1),
                //     0, filts[current_block].size());
            }



            // status[0].blocks[0].insert(std::move(new_col_col));
            LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "inserting column with {} rows", new_col_col->size());
            status[0].blocks[0].insert(ColumnWithTypeAndName(std::move(new_col_col), sample_block.getByPosition(i).type, sample_block.getByPosition(i).name));


            // status[0].blocks[0].insert(std::move(new_col));
        }
        if (/*status[right_inp].blocks.size() > 1 && */ status[right_inp].right_rest)
        {
            // status[right_inp].blocks[0] = std::move(last_block);
            if (status[right_inp].blocks.size() > 1)
            {
                status[right_inp].blocks.resize(1);
            }

            LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "right rest block has {} rows", status[right_inp].blocks[0].rows());
            // assert(status[right_inp].right_rest);
        }
        else
        {
            status[right_inp].blocks.clear();
        }

        status[right_inp].current_pos = 0;
        status[right_inp].left_pos = 0;
        status[right_inp].left_rest = false;
    }

    Chunk output_chunk;

    output_chunk.setColumns(status[0].blocks[0].getColumns(), status[0].blocks[0].rows());
    outputs.front().push(std::move(output_chunk));

    current_input = inputs.begin();
    inp = 0;
    status[0].blocks.clear();

    // left_read = false;
    LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "pushed");


    LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "returning PortFull");   // !!!!!!!!!!
    return Status::PortFull;
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





    bool all_finished = true;
    bool left_read = false;  // needed ?

    for (;;)
    {
        // if (inp == num_inputs)
        // {
        //     inp = 0;
        //     current_input = inputs.begin();
        // }

        assert(current_input != inputs.end());
        auto check_result = topCheck();
        if (check_result)
        {
            return check_result.value();
        }


        if (current_input->isFinished() && !status[inp].blocks.empty() && !status[inp].right_rest)
        {
            LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "input finished - EOFL");
            goto EOFL;
        }


        if (!current_input->isFinished() || current_input->hasData() || status[inp].right_rest)
        {
            if (current_input == inputs.begin())
            {
                processLeft();
            }
            else
            {
                processRight();
            }


          EOFL:
            LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "EOFL");
            if (!left_read && status[0].blocks.empty() && !inputs.begin()->isFinished())
            {

                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "no left block - continue");
            }

            if (inp == num_inputs - 1) /*++current_input == inputs.end()*/
            {
                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "last input");
                bool left_in_use = false;  // ???
                bool all_rights_have_blocks = true;
                {
                    auto cur_inp = inputs.begin();
                    cur_inp ++;
                    for (size_t right_inp = 1; right_inp < num_inputs; ++right_inp, ++cur_inp)
                    {
                        if (cur_inp->isFinished())
                        {
                            for (size_t lp = status[right_inp].left_pos; lp < left_filt.size(); ++lp)
                            {
                                left_filt[lp] = 0;
                            }
                        }
                        else if (status[right_inp].left_rest)
                        {
                            left_in_use = true;
                        }
                        if (status[right_inp].blocks.empty())
                        {
                            all_rights_have_blocks = false;
                        }
                    }
                }

                if (!left_in_use && all_rights_have_blocks)
                {

                    auto eoi_ret = endOfInputs();
                    left_read = false;
                    if (eoi_ret)
                    {
                        LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "returning after endOfInput");
                        return eoi_ret.value();
                    }
                }

            }
            else
            {
                ++inp;
                ++current_input;
                LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "going to next input {}", inp);
            }
            // goto EOFL  !!!
        }

        else
        {
            LOG_DEBUG(&Poco::Logger::get("ParallelJoinTransform"), "current input {} is finished", inp);
            if (inp == num_inputs - 1)
            {
                if (all_finished)
                {
                    break;
                }

                inp = 0;
                current_input = inputs.begin();
                if (!current_input->hasData())
                {
                    return Status::NeedData;
                }
                left_read = false;
            }
            else
            {
                ++inp;
                ++current_input;

            }
        }
    }






    for (auto & inp_to_close : inputs)
    {
        inp_to_close.close();
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
