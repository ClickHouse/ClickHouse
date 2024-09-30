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

    auto processFilterColumn = [&](const String & filter_column_name, const char * filter_name)
    {
        if (!filter_column_name.empty())
        {
            auto filter_type = header.getByName(filter_column_name).type;
            if (!filter_type->onlyNull() && !isUInt8(removeNullable(removeLowCardinality(filter_type))))
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER,
                    "Illegal type {} of column '{}' for {}. Must be UInt8 or Nullable(UInt8).",
                    filter_type->getName(),
                    filter_column_name,
                    filter_name);
            }

            if (remove_filter_column)
                header.erase(filter_column_name);
            // else
            // replaceFilterToConstant(header, filter_column_name);
        }
    };

    processFilterColumn(from_filter_column_name, "from_filter");
    processFilterColumn(to_filter_column_name, "to_filter");

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
    std::cerr << on_totals << '\n';
}

IProcessor::Status LimitInRangeTransform::prepare()
{
    std::cerr << "IProcessor::Status LimitInRangeTransform::prepare\n";

    // TODO: maybe some optimizations here. check in other.

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
            if (!from_filter_column_name.empty() && !to_filter_column_name.empty() && !from_index_found) {} // if from index is not found in FROM expr TO expr, then do not throw. The to_index_found is optional here.
            else if (!to_filter_column_name.empty() && !to_index_found)
            {
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_COLUMN_FOR_FILTER, "The 'TO' condition was not satisfied: 'TO' index was not found in the data");
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
        {
            size_t adjusted_position = !from_filter_column_name.empty() ? to_filter_column_position - 1 : to_filter_column_position;
            chunk.erase(adjusted_position);
        }
    }
}

const IColumn::Filter * initializeColumn(const Columns & columns, size_t filter_column_position)
{
    const IColumn & filter_column = *columns[filter_column_position];
    FilterDescription filter_description(filter_column);

    return filter_description.data;
}

void cutChunkColumns(Chunk & chunk, size_t start, size_t length)
{
    auto columns = chunk.detachColumns();

    for (auto & col : columns)
        col = col->cut(start, length);

    chunk.setColumns(std::move(columns), length);
}

std::optional<size_t> findFirstMatchingIndex(const IColumn::Filter* filter, size_t start = 0)
{
    // if all elements are zero or starting index is out-of-bound.
    if (!filter || memoryIsZero(filter->data(), 0, filter->size()) || start >= filter->size())
        return std::nullopt;

    auto it = std::find(filter->begin() + start, filter->end(), 1);
    if (it == filter->end())
        return std::nullopt;

    // the index of the found element.
    return static_cast<size_t>(std::distance(filter->begin(), it));
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
    // TODO: constant_filter_description 

    if (from_index_found)
    {
        std::cerr << "##############\n";
        std::cerr << "INDEX ALREADY EXIST;\n";
        std::cerr << "##############\n";
        removeFilterIfNeed(chunk);
        return;
    }

    auto from_filter_mask = initializeColumn(chunk.getColumns(), from_filter_column_position);
    std::optional<size_t> index = findFirstMatchingIndex(from_filter_mask);

    if (!index)
    {
        // also can be checked with memory size = 0;
        std::cerr << "##############\n";
        std::cerr << "FROM INDEX NOT FOUND"
                  << "; " << '\n';
        std::cerr << "##############\n";
        chunk.clear();
        return;
    }

    from_index_found = true;
    std::cerr << "##############\n";
    std::cerr << "FOUND FROM INDEX: " << *index << "; " << '\n';
    std::cerr << "##############\n";

    cutChunkColumns(chunk, *index, chunk.getNumRows() - *index);
    removeFilterIfNeed(chunk);
}

void LimitInRangeTransform::doToTransform(Chunk & chunk)
{
    // If 'to index' is not found, return all chunks and then throw an exception.
    std::cerr << "FilterTransform::doToTransform\n";

    auto to_filter_mask = initializeColumn(chunk.getColumns(), to_filter_column_position);
    std::optional<size_t> index = findFirstMatchingIndex(to_filter_mask);

    if (!index) {
        // also can be checked with memory size = 0;
        std::cerr << "##############\n";
        std::cerr << "TO INDEX NOT FOUND" << ";\n";
        std::cerr << "##############\n";
        removeFilterIfNeed(chunk);
        return;
    }

    to_index_found = true;
    std::cerr << "##############\n";
    std::cerr << "FOUND TO INDEX: " << *index << "; " << '\n';
    std::cerr << "##############\n";

    cutChunkColumns(chunk, 0, *index + 1);

    stopReading(); // optimization as to_index_found

    removeFilterIfNeed(chunk);
    // SELECT * FROM temp_table150 LIMIT INRANGE TO id = 100
}

void LimitInRangeTransform::doFromAndToTransform(Chunk & chunk)
{
    // if from index not found, throw exception.
    
    std::cerr << "FilterTransform::doFromAndToTransform\n";

    auto from_filter_mask = initializeColumn(chunk.getColumns(), from_filter_column_position);
    auto to_filter_mask = initializeColumn(chunk.getColumns(), to_filter_column_position);

    if (from_index_found) // in prev chunks
    {
        std::cerr << "##############\n";
        std::cerr << "FROM INDEX EXIST IN PREV CHUNK" << ";\n";
        std::cerr << "##############\n";

        std::optional<size_t> to_index = findFirstMatchingIndex(to_filter_mask);
        if (to_index)
        {
            to_index_found = true;
            std::cerr << "##############\n";
            std::cerr << "TO INDEX FOUND HERE: " << *to_index << ";\n";
            std::cerr << "##############\n";

            cutChunkColumns(chunk, 0, *to_index + 1);

            stopReading(); // SELECT * FROM temp_table150 LIMIT INRANGE FROM id = 67000 TO id = 67100
        } else
        {
        // also can be checked with memory size = 0;
        std::cerr << "##############\n";
        std::cerr << "TO INDEX NOT FOUND HERE" << ";\n";
        std::cerr << "##############\n";
        }

        removeFilterIfNeed(chunk);
        return;
    }

    std::optional<size_t> from_index = findFirstMatchingIndex(from_filter_mask);
    if (from_index)
    { // SELECT * FROM temp_table150 LIMIT INRANGE FROM id = 5 TO id = 100
        from_index_found = true;
        std::cerr << "##############\n";
        std::cerr << "FROM INDEX FOUND: " << *from_index << ", " << '\n';
        std::cerr << "##############\n";

        std::optional<size_t> to_index = findFirstMatchingIndex(to_filter_mask, *from_index + 1);
        if (to_index) {
            std::cerr << "##############\n";
            std::cerr << "FROM AND TO INDICES FOUND: " << *from_index << ", " << *to_index << '\n';
            std::cerr << "##############\n";

            cutChunkColumns(chunk, *from_index, *to_index - *from_index + 1);
            stopReading();
        } else {
            cutChunkColumns(chunk, *from_index, chunk.getNumRows() - *from_index);
        }

        removeFilterIfNeed(chunk);
        return;
    }

    chunk.clear();
    // return nothing if none of the indices are found.
}


}
