#pragma once
#include <Storages/MergeTree/MergeTreePrimaryIndexColumn.h>
#include <boost/core/noncopyable.hpp>

namespace DB
{

class PrimaryIndex : private boost::noncopyable
{
public:
    struct Settings
    {
        bool compress = false;
        size_t min_block_size = 32;
        double max_ratio_to_compress = 0.7;
    };

    PrimaryIndex() = default;
    PrimaryIndex(Columns raw_columns, const Settings & settings);
    PrimaryIndex(Columns raw_columns, std::vector<size_t> num_equal_ranges, const Settings & settings);

    bool empty() const { return columns.empty(); }
    size_t getNumColumns() const { return columns.size(); }
    size_t getNumRows() const { return num_rows; }
    const IIndexColumn & getIndexColumn(size_t idx) const { return *columns.at(idx); }

    size_t bytes() const;
    size_t allocatedBytes() const;

    bool isCompressed() const;
    Columns getRawColumns() const;

    Field get(size_t col_idx, size_t row_idx) const;
    void get(size_t col_idx, size_t row_idx, Field & field) const;

private:
    void init(Columns raw_columns, const Settings & settings);

    size_t num_rows = 0;
    IndexColumns columns;
    std::vector<size_t> block_sizes;
};

using PrimaryIndexPtr = std::shared_ptr<const PrimaryIndex>;

}
