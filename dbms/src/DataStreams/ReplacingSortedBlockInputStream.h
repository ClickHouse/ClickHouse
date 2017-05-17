#pragma once

#include <common/logger_useful.h>

#include <DataStreams/MergingSortedBlockInputStream.h>


namespace DB
{

/** Merges several sorted streams into one.
  * For each group of consecutive identical values of the primary key (the columns by which the data is sorted),
  *  keeps row with max `version` value.
  */
class ReplacingSortedBlockInputStream : public MergingSortedBlockInputStream
{
public:
    ReplacingSortedBlockInputStream(BlockInputStreams inputs_, const SortDescription & description_,
        const String & version_column_, size_t max_block_size_)
        : MergingSortedBlockInputStream(inputs_, description_, max_block_size_),
        version_column(version_column_)
    {
    }

    String getName() const override { return "ReplacingSorted"; }

    String getID() const override
    {
        std::stringstream res;
        res << "ReplacingSorted(inputs";

        for (size_t i = 0; i < children.size(); ++i)
            res << ", " << children[i]->getID();

        res << ", description";

        for (size_t i = 0; i < description.size(); ++i)
            res << ", " << description[i].getID();

        res << ", version_column, " << version_column << ")";
        return res.str();
    }

protected:
    /// Can return 1 more records than max_block_size.
    Block readImpl() override;

private:
    String version_column;
    ssize_t version_column_number = -1;

    Logger * log = &Logger::get("ReplacingSortedBlockInputStream");

    /// All data has been read.
    bool finished = false;

    RowRef current_key;            /// Primary key of current row.
    RowRef next_key;            /// Primary key of next row.

    RowRef selected_row;        /// Last row with maximum version for current primary key.

    UInt64 max_version = 0;        /// Max version for current primary key.

    template<class TSortCursor>
    void merge(ColumnPlainPtrs & merged_columns, std::priority_queue<TSortCursor> & queue);

    /// Output into result the rows for current primary key.
    void insertRow(ColumnPlainPtrs & merged_columns, size_t & merged_rows);
};

}
