#pragma once

#include <common/logger_useful.h>

#include <Core/Row.h>
#include <Core/ColumnNumbers.h>
#include <DataStreams/MergingSortedBlockInputStream.h>
#include <AggregateFunctions/IAggregateFunction.h>
#include <Columns/ColumnAggregateFunction.h>
#include <Common/AlignedBuffer.h>


namespace DB
{

/** Merges several sorted streams to one.
  * During this for each group of consecutive identical values of the primary key (the columns by which the data is sorted),
  * merges them into one row. When merging, the data is pre-aggregated - merge of states of aggregate functions,
  * corresponding to a one value of the primary key. For columns that are not part of the primary key and which do not have the AggregateFunction type,
  * when merged, the first value is selected.
  */
class AggregatingSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
    AggregatingSortedBlockInputStream(
        const BlockInputStreams & inputs_, const SortDescription & description_, size_t max_block_size_);

    String getName() const override { return "AggregatingSorted"; }

    bool isSortedOutput() const override { return true; }

protected:
    /// Can return 1 more records than max_block_size.
    Block readImpl() override;

private:
    Logger * log = &Logger::get("AggregatingSortedBlockInputStream");

    /// Read finished.
    bool finished = false;

    struct SimpleAggregateDescription;

    /// Columns with which numbers should be aggregated.
    ColumnNumbers column_numbers_to_aggregate;
    ColumnNumbers column_numbers_not_to_aggregate;
    std::vector<ColumnAggregateFunction *> columns_to_aggregate;
    std::vector<SimpleAggregateDescription> columns_to_simple_aggregate;

    RowRef current_key;        /// The current primary key.
    RowRef next_key;           /// The primary key of the next row.

    /** We support two different cursors - with Collation and without.
     *  Templates are used instead of polymorphic SortCursor and calls to virtual functions.
     */
    void merge(MutableColumns & merged_columns, std::priority_queue<SortCursor> & queue);

    /** Extract all states of aggregate functions and merge them with the current group.
      */
    void addRow(SortCursor & cursor);

    /** Insert all values of current row for simple aggregate functions
     */
    void insertSimpleAggregationResult(MutableColumns & merged_columns);

    /// Stores information for aggregation of SimpleAggregateFunction columns
    struct SimpleAggregateDescription
    {
        /// An aggregate function 'anyLast', 'sum'...
        AggregateFunctionPtr function;
        IAggregateFunction::AddFunc add_function;
        size_t column_number;
        AlignedBuffer state;
        bool created = false;

        SimpleAggregateDescription(const AggregateFunctionPtr & function_, const size_t column_number_) : function(function_), column_number(column_number_)
        {
            add_function = function->getAddressOfAddFunction();
            state.reset(function->sizeOfData(), function->alignOfData());
        }

        void createState()
        {
            if (created)
                return;
            function->create(state.data());
            created = true;
        }

        void destroyState()
        {
            if (!created)
                return;
            function->destroy(state.data());
            created = false;
        }

        /// Explicitly destroy aggregation state if the stream is terminated
        ~SimpleAggregateDescription()
        {
            destroyState();
        }

        SimpleAggregateDescription() = default;
        SimpleAggregateDescription(SimpleAggregateDescription &&) = default;
        SimpleAggregateDescription(const SimpleAggregateDescription &) = delete;
    };
};

}
