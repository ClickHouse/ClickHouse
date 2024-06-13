#pragma once
#include <unordered_map>
#include <config.h>

#if USE_PARQUET
#include <memory>
#include <Columns/IColumn.h>
#include <Core/Field.h>
#include <Interpreters/ActionsDAG.h>
#include <Processors/Formats/Impl/Parquet/RowRanges.h>
#include <parquet/page_index.h>
#include <parquet/file_reader.h>
#include <Storages/MergeTree/KeyCondition.h>

namespace DB
{
using ParquetColumnReadSequence = std::vector<Int32>;

class RPNBuilderTreeNode;
class Context;
using ContextPtr = std::shared_ptr<const Context>;
class ParquetColumnIndexFilter;
class ParquetColumnIndex;
using ParquetColumnIndexPtr = std::unique_ptr<ParquetColumnIndex>;
using ParquetIndexs = std::vector<Int32>;
using ParquetColumnIndexStore = std::unordered_map<std::string, ParquetColumnIndexPtr>;
using ParquetColumnIndexFilterPtr = std::shared_ptr<ParquetColumnIndexFilter>;

struct ParquetIndexsBuilder
{
    static constexpr Int32 ALL_PAGES = -1;
    template <typename Predict>
    static ParquetIndexs filter(const size_t size, Predict predict)
    {
        ParquetIndexs pages;
        for (size_t from = 0; from != size; ++from)
            if (predict(from))
                pages.emplace_back(from);
        return pages;
    }
};

struct RowRangesBuilder
{
    const int64_t rg_count; // the total number of rows in the row-group
    const std::vector<parquet::PageLocation> & page_locations;

    RowRangesBuilder(int64_t row_group_row_count, const std::vector<parquet::PageLocation> & page_locations_)
        : rg_count(row_group_row_count), page_locations(page_locations_)
    {
    }

    /**
     * @param page_index
     *           the index of the page
     * @return the index of the first row in the page
     */
    size_t firstRowIndex(size_t page_index) const { return page_locations[page_index].first_row_index; }

    /**
     * @param page_index
     *          the index of the page
     * @return the calculated index of the last row of the given page
     */
    size_t lastRowIndex(size_t page_index) const
    {
        const size_t nextPageIndex = page_index + 1;
        const size_t pageCount = std::ssize(page_locations);

        const size_t lastRowIndex = (nextPageIndex >= pageCount ? rg_count : page_locations[nextPageIndex].first_row_index) - 1;
        return lastRowIndex;
    }

    RowRanges all() const { return RowRanges::createSingle(rg_count); }

    RowRanges toRowRanges(const ParquetIndexs & pages) const
    {
        if (pages.size() == 1 && pages[0] == ParquetIndexsBuilder::ALL_PAGES)
            return all();
        RowRanges row_ranges;
        std::ranges::for_each(pages, [&](size_t page_index) { row_ranges.add(RowRange{firstRowIndex(page_index), lastRowIndex(page_index)}); });
        return row_ranges;
    }
};

/**
 * Column index containing min/max and null count values for the pages in a column chunk. It also implements methods
 * to return the indexes of the matching pages.
 *
 * @see parquet::ColumnIndex
 */
class ParquetColumnIndex
{
public:
    virtual ~ParquetColumnIndex() = default;

    virtual const parquet::OffsetIndex & offsetIndex() const = 0;

    virtual bool hasParquetColumnIndex() const = 0;

    /// \brief Returns the row ranges where the column value is not equal to the given value.
    /// column != literal => (min, max) = literal =>
    /// min != literal || literal != max
    virtual ParquetIndexs notEq(const Field & value) const = 0;

    /// \brief Returns the row ranges where the column value is equal to the given value.
    /// column == literal => literal not in (min, max) =>
    ///  min <= literal && literal <= max
    virtual ParquetIndexs eq(const Field & value) const = 0;

    /// \brief Returns the row ranges where the column value is greater than the given value.
    /// column > literal
    virtual ParquetIndexs gt(const Field & value) const = 0;

    /// column >= literal
    virtual ParquetIndexs gtEg(const Field & value) const = 0;

    /// \brief Returns the row ranges where the column value is less than the given value.
    /// column < literal
    virtual ParquetIndexs lt(const Field & value) const = 0;

    /// column <= literal
    virtual ParquetIndexs ltEg(const Field & value) const = 0;

    virtual ParquetIndexs in(const ColumnPtr & column) const = 0;

    //TODO: parameters
    static ParquetColumnIndexPtr create(
        const parquet::ColumnDescriptor * descr,
        const std::shared_ptr<parquet::ColumnIndex> & column_index,
        const std::shared_ptr<parquet::OffsetIndex> & offset_index);
};

template <typename DType>
class TypedColumnIndex : public ParquetColumnIndex
{
public:
    using T = typename DType::c_type;
};

using ColumnIndexInt64 = TypedColumnIndex<parquet::Int64Type>;
using ColumnIndexInt32 = TypedColumnIndex<parquet::Int32Type>;

class ParquetColumnIndexFilter
{
public:
    /// The expression is stored as Reverse Polish Notation.
    struct RPNElement
    {
        enum Function
        {
            /// Atoms of a Boolean expression.
            FUNCTION_EQUALS,
            FUNCTION_NOT_EQUALS,
            FUNCTION_LESS,
            FUNCTION_GREATER,
            FUNCTION_LESS_OR_EQUALS,
            FUNCTION_GREATER_OR_EQUALS,
            FUNCTION_IN,
            FUNCTION_NOT_IN,
            FUNCTION_UNKNOWN, /// Can take any value.
            /// Operators of the logical expression.
            FUNCTION_NOT,
            FUNCTION_AND,
            FUNCTION_OR,
            /// Constants
            ALWAYS_FALSE,
            ALWAYS_TRUE,
        };

        explicit RPNElement(const Function function_ = FUNCTION_UNKNOWN) : function(function_) { }

        Function function = FUNCTION_UNKNOWN;
        std::string columnName;
        Field value;
        ColumnPtr column;
    };

    using RPN = std::vector<RPNElement>;
    using AtomMap = std::unordered_map<std::string, bool (*)(RPNElement & out, const Field & value)>;
    static const AtomMap atom_map;

    /// Construct key condition from ActionsDAG nodes
    ParquetColumnIndexFilter(std::shared_ptr<const KeyCondition> key_condition);

private:
    static bool extractAtomFromTree(const RPNBuilderTreeNode & node, RPNElement & out);
    std::unique_ptr<RPN> rpn;

public:
    RowRanges calculateRowRanges(const ParquetColumnIndexStore & index_store, size_t rowgroup_count) const;
};

/// Used for eache parquet file.
class ParquetFileColumnIndexFilter
{
public:
    using ReadRanges = std::vector<arrow::io::ReadRange>;
    ParquetFileColumnIndexFilter(
        parquet::ParquetFileReader & file_reader_,
        size_t source_file_size_,
        const std::vector<Int32> & required_column_indices_,
        bool column_name_case_insenstive_,
        std::shared_ptr<const KeyCondition> key_condition_);
    const RowRanges & calculateRowRanges(int row_group);
    std::pair<ReadRanges, ParquetColumnReadSequence> calculateReadSequence(int row_group, const String & col_name_);
private:
    parquet::ParquetFileReader & file_reader;
    size_t source_file_size;
    std::vector<Int32> required_column_indices;
    bool column_name_case_insenstive = false;
    std::unordered_map<Int32, std::unique_ptr<RowRanges>> row_group_row_ranges;
    std::unordered_map<Int32, std::unique_ptr<ParquetColumnIndexStore>> row_group_column_index_stores;
    std::unique_ptr<ParquetColumnIndexFilter> column_index_filter;
    std::unordered_map<String, Int32> col_name_to_index;


    ParquetColumnIndexStore * getColumnIndexStore(int row_group);
};
}
#endif
