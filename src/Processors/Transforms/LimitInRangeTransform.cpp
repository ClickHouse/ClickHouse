#include <algorithm>
#include <Processors/Transforms/LimitInRangeTransform.h>

#include <Columns/ColumnSparse.h>
#include <Columns/ColumnsCommon.h>
#include <Core/Field.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNullable.h>
#include <Interpreters/ExpressionActions.h>
#include <Processors/Merges/Algorithms/ReplacingSortedAlgorithm.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER;
    extern const int UNKNOWN_TYPE_OF_QUERY;
}

Block LimitInRangeTransform::transformHeader(
    Block header, const String & from_filter_column_name, const String & to_filter_column_name, bool remove_filter_column)
{
    std::cerr << "from_filter_column_name=" << from_filter_column_name << '\n';
    std::cerr << "to_filter_column_name=" << to_filter_column_name << '\n';

    std::cerr << "Original header: " << header.dumpStructure() << " " << remove_filter_column << '\n';

    if (!from_filter_column_name.empty())
    {
        auto from_filter_type = header.getByName(from_filter_column_name).type;
        if (!from_filter_type->onlyNull() && !isUInt8(removeNullable(removeLowCardinality(from_filter_type))))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                "Illegal type {} of column {} for from_filter. Must be UInt8 or Nullable(UInt8).",
                from_filter_type->getName(),
                from_filter_column_name);

        if (remove_filter_column)
            header.erase(from_filter_column_name);
        //  else
        //      replaceFilterToConstant(header, filter_column_name);
    }


    if (!to_filter_column_name.empty())
    {
        auto to_filter_type = header.getByName(to_filter_column_name).type;
        if (!to_filter_type->onlyNull() && !isUInt8(removeNullable(removeLowCardinality(to_filter_type))))
            throw Exception(
                ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                "Illegal type {} of column {} for to_filter. Must be UInt8 or Nullable(UInt8).",
                to_filter_type->getName(),
                to_filter_column_name);

        if (remove_filter_column)
            header.erase(to_filter_column_name);
        //  else
        //      replaceFilterToConstant(header, filter_column_name);
    }
    std::cerr << "TransformHeader ending structure: " << header.dumpStructure() << '\n';
    return header;
}


LimitInRangeTransform::LimitInRangeTransform(
    const Block & header_,
    String from_filter_column_name_,
    String to_filter_column_name_,
    bool remove_filter_column_,
    bool on_totals_,
    std::shared_ptr<std::atomic<size_t>> rows_filtered_)
    : ISimpleTransform(header_, transformHeader(header_, from_filter_column_name_, to_filter_column_name_, remove_filter_column_), true)
    , from_filter_column_name(std::move(from_filter_column_name_))
    , to_filter_column_name(std::move(to_filter_column_name_))
    , remove_filter_column(remove_filter_column_)
    , on_totals(on_totals_)
    , rows_filtered(rows_filtered_)
{
    transformed_header = getInputPort().getHeader();

    if (from_filter_column_name != "")
        from_filter_column_position = transformed_header.getPositionByName(from_filter_column_name);

    if (to_filter_column_name != "")
        to_filter_column_position = transformed_header.getPositionByName(to_filter_column_name);

    std::cerr << "Constructor header ending structure: " << transformed_header.dumpStructure() << '\n';
}

IProcessor::Status LimitInRangeTransform::prepare()
{
    std::cerr << "IProcessor::Status LimitInRangeTransform::prepare\n";

    // TODO: need some optimization here

    // if (!on_totals
    //     && (constant_filter_description.always_false
    //         /// Optimization for `WHERE column in (empty set)`.
    //         /// The result will not change after set was created, so we can skip this check.
    //         /// It is implemented in prepare() stop pipeline before reading from input port.
    //         || (!are_prepared_sets_initialized && expression && expression->checkColumnIsAlwaysFalse(filter_column_name))))
    // {
    //     input.close();
    //     output.finish();
    //     return Status::Finished;
    // }

    // auto status = ISimpleTransform::prepare();
    // /// Until prepared sets are initialized, output port will be unneeded, and prepare will return PortFull.
    // if (status != IProcessor::Status::PortFull)
    //     are_prepared_sets_initialized = true;

    // return status;

    if (output.isFinished())
    {
        input.close();
        return Status::Finished;
    }

    if (!output.canPush())
    {
        input.setNotNeeded();
        return Status::PortFull;
    }

    /// Output if has data.
    if (has_output)
    {
        output.pushData(std::move(output_data));
        has_output = false;

        if (!no_more_data_needed)
            return Status::PortFull;
    }

    /// Stop if don't need more data.
    if (no_more_data_needed)
    {
        input.close();
        output.finish();
        return Status::Finished;
    }

    /// Check can input.
    if (!has_input)
    {
        if (input.isFinished())
        {
            output.finish();
            if (!to_filter_column_name.empty())
            {
                if (!to_index_found)
                    throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER, "TO3 INDEX NOT FOUND {}", to_index_found);
            }
            return Status::Finished;
        }

        input.setNeeded();

        if (!input.hasData())
            return Status::NeedData;

        input_data = input.pullData(set_input_not_needed_after_read);
        has_input = true;

        if (input_data.exception)
            /// No more data needed. Exception will be thrown (or swallowed) later.
            input.setNotNeeded();
    }

    /// Now transform.
    return Status::Ready;
}

void LimitInRangeTransform::removeFilterIfNeed(Chunk & chunk) const
{
    if (chunk && remove_filter_column)
    {
        if (!from_filter_column_name.empty())
            chunk.erase(from_filter_column_position);
        if (!to_filter_column_name.empty())
            chunk.erase(to_filter_column_position);
    }
}

const IColumn::Filter * initializeColumn(const Columns & columns, size_t filter_column_position)
{
    ColumnPtr filter_column = columns[filter_column_position];
    ColumnPtr data_holder = nullptr;
    if (filter_column->isSparse())
        data_holder = recursiveRemoveSparse(filter_column->getPtr());

    if (filter_column->lowCardinality() && !data_holder)
    {
        data_holder = filter_column->convertToFullColumnIfLowCardinality();
    }

    const auto & column = data_holder ? *data_holder : *filter_column;
    const IColumn::Filter * filter_concrete_column = nullptr;
    if (const ColumnUInt8 * concrete_column = typeid_cast<const ColumnUInt8 *>(&column))
        filter_concrete_column = &concrete_column->getData();

    return filter_concrete_column;
}

std::optional<size_t> findFirstMatchingIndex(const IColumn::Filter * filter)
{
    if (!filter || memoryIsZero(filter->data(), 0, filter->size()))
        return std::nullopt; // Return nullopt if filter is nullptr or no match can be found

    auto it = std::find(filter->begin(), filter->end(), 1);
    return std::distance(filter->begin(), it);
}

void LimitInRangeTransform::transform(Chunk & chunk)
{
    std::cerr << "In LimitInRangeTransform::transform\n";

    auto chunk_rows_before = chunk.getNumRows();
    if (!from_filter_column_name.empty() && !to_filter_column_name.empty())
        doFromAndToTransform(chunk);
    else if (!from_filter_column_name.empty())
        doFromTransform(chunk);
    else if (!to_filter_column_name.empty())
        doToTransform(chunk);
    if (rows_filtered)
        *rows_filtered += chunk_rows_before - chunk.getNumRows();
}

void LimitInRangeTransform::doFromTransform(Chunk & chunk)
{
    std::cerr << "FilterTransform::doFromTransform\n";

    size_t num_rows_before_filtration = chunk.getNumRows();
    auto columns = chunk.detachColumns();
    // DataTypes types;
    // auto select_final_indices_info = getSelectByFinalIndices(chunk);
    // std::cerr << constant_filter_description.always_true << " " << constant_filter_description.always_false << '\n';

    if (from_index_found)
    {
        std::cerr << "##############\n";
        std::cerr << "INDEX ALREADY EXIST;\n";
        std::cerr << "##############\n";
        chunk.setColumns(std::move(columns), num_rows_before_filtration);
        removeFilterIfNeed(chunk);
        return;
    }

    auto from_filter_mask = initializeColumn(columns, from_filter_column_position);
    std::optional<size_t> index = findFirstMatchingIndex(from_filter_mask);

    if (index.has_value())
    {
        from_index_found = true;
        std::cerr << "##############\n";
        std::cerr << "FOUND INDEX: " << index.value() << "; " << '\n';
        std::cerr << "##############\n";
    }
    else
    {
        // also can be checked with memory size = 0;
        std::cerr << "##############\n";
        std::cerr << "NOT FOUND"
                  << "; " << '\n';
        std::cerr << "##############\n";
        return;
    }

    size_t length = num_rows_before_filtration - index.value();
    for (auto & col : columns)
        col = col->cut(index.value(), length);

    chunk.setColumns(std::move(columns), length);
    removeFilterIfNeed(chunk);
}

void LimitInRangeTransform::doToTransform(Chunk & chunk)
{
    std::cerr << "FilterTransform::doToTransform\n";

    size_t num_rows_before_filtration = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    // can be wrapped into filterDescription
    auto to_filter_mask = initializeColumn(columns, to_filter_column_position);
    std::optional<size_t> index = findFirstMatchingIndex(to_filter_mask);
    if (index.has_value())
    {
        to_index_found = true;
        std::cerr << "##############\n";
        std::cerr << "FOUND INDEX: " << index.value() << "; " << '\n';
        std::cerr << "##############\n";
    }
    else
    {
        // also can be checked with memory size = 0;
        std::cerr << "##############\n";
        std::cerr << "NOT FOUND" << ";\n";
        std::cerr << "##############\n";
        chunk.setColumns(std::move(columns), num_rows_before_filtration);
        removeFilterIfNeed(chunk);
        return;
    }


    size_t length = index.value() + 1;
    for (auto & col : columns)
        col = col->cut(0, length);

    stopReading(); // optimization as to_index_found
    chunk.setColumns(std::move(columns), length);
    removeFilterIfNeed(chunk);
    // SELECT * FROM temp_table150 LIMIT INRANGE TO id = 100
}

void LimitInRangeTransform::doFromAndToTransform(Chunk & chunk)
{
/** Main algo:
  * if from_index was already found:
  *   search to in current:
  *   if found: cut(0, to_index + 1) and stop, else: add all columns to chunk
  *
  * search for from_index and to_index in current:
  * if found from_index and to_index: cut(from_index, to_index - from_index + 1) and stop
  * else if found from_index only: cut(from_index, length - from_index)
  * else if found to_index only: cut(0, to_index + 1) stop and throw
  * else: return empty chunk
  */

    std::cerr << "FilterTransform::doFromAndToTransform\n";

    size_t num_rows_before_filtration = chunk.getNumRows();
    auto columns = chunk.detachColumns();

    // can be wraped into filterDescription if needed(check below functions)? with including the cut
    auto from_filter_mask = initializeColumn(columns, from_filter_column_position);
    auto to_filter_mask = initializeColumn(columns, to_filter_column_position);

    if (from_index_found)
    {
        std::cerr << "##############\n";
        std::cerr << "FROM INDEX ALREADY EXIST" << ";\n";
        std::cerr << "##############\n";

        std::optional<size_t> to_index = findFirstMatchingIndex(to_filter_mask);
        if (to_index.has_value())
        {
            to_index_found = true;
            std::cerr << "##############\n";
            std::cerr << "TO INDEX FOUND: " << to_index.value() << ";\n";
            std::cerr << "##############\n";

            for (auto & col : columns)
                col = col->cut(0, to_index.value() + 1);

            chunk.setColumns(std::move(columns), to_index.value() + 1);
            stopReading(); // SELECT * FROM temp_table150 LIMIT INRANGE FROM id = 67000 TO id = 67100
        }
        else
        {
            // also can be checked with memory size = 0;
            std::cerr << "##############\n";
            std::cerr << "TO INDEX NOT FOUND" << ";\n";
            std::cerr << "##############\n";
            chunk.setColumns(std::move(columns), num_rows_before_filtration);
        }
        removeFilterIfNeed(chunk);
        return;
    }

    std::optional<size_t> from_index = findFirstMatchingIndex(from_filter_mask);
    std::optional<size_t> to_index = findFirstMatchingIndex(to_filter_mask);
    if (from_index.has_value() && to_index.has_value())
    { // SELECT * FROM temp_table150 LIMIT INRANGE FROM id = 5 TO id = 100
        from_index_found = true;
        to_index_found = true;
        // check that from_index <= to_index else throw exception

        std::cerr << "##############\n";
        std::cerr << "FROM AND TO INDICES FOUND: " << from_index.value() << ", " << to_index.value() << '\n';
        std::cerr << "##############\n";
        if (to_index.value() < from_index.value())
        { // SELECT * FROM temp_table150 LIMIT INRANGE FROM id = 100 TO id = 5
            throw Exception(
                ErrorCodes::UNKNOWN_TYPE_OF_QUERY,
                "First occurence of to_expression (index = {}) found earlier than first occurence of from_expression (index = {})",
                to_index.value(),
                from_index.value());
        }

        for (auto & col : columns)
            col = col->cut(from_index.value(), to_index.value() - from_index.value() + 1);

        chunk.setColumns(std::move(columns), to_index.value() - from_index.value() + 1);
        removeFilterIfNeed(chunk);
        stopReading();
    }
    else if (from_index.has_value())
    { // SELECT * FROM temp_table150 LIMIT INRANGE FROM id = 5 TO id = 70000
        std::cerr << "##############\n";
        std::cerr << "FROM INDEX FOUND: " << from_index.value() << '\n';
        std::cerr << "##############\n";
        from_index_found = true;
        for (auto & col : columns)
            col = col->cut(from_index.value(), num_rows_before_filtration - from_index.value());

        chunk.setColumns(std::move(columns), num_rows_before_filtration - from_index.value());
        removeFilterIfNeed(chunk);
    }
    else if (to_index.has_value())
    { // SELECT * FROM temp_table150 LIMIT INRANGE FROM id = 90000 TO id = 100
        std::cerr << "##############\n";
        std::cerr << "TO INDEX FOUND earlier (when FROM not): " << to_index.value() << '\n';
        std::cerr << "##############\n";
        stopReading();
        throw Exception(
            ErrorCodes::UNKNOWN_TYPE_OF_QUERY, "First occurence of to_expression found earlier than first occurence of from_expression");
    }

    // nothing found, return for all;

    // check in prepare what if from/to indices not found; maybe just returning nothing is ok
    // or exception?
}


}
