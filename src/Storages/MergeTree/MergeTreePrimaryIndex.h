#pragma once
#include <algorithm>
#include <Columns/IColumn.h>
#include <boost/core/noncopyable.hpp>
#include "Common/PODArray_fwd.h"
#include <Common/BitHelpers.h>
#include <Common/PODArray.h>
#include "base/types.h"

namespace DB
{

class IIndexColumn
{
public:
    virtual ~IIndexColumn() = default;

    virtual size_t bytes() const = 0;
    virtual size_t allocatedBytes() const = 0;
    virtual void get(size_t n, Field & field) const = 0;
    virtual bool isCompressed() const = 0;
    virtual ColumnPtr getRawColumn() const = 0;
};

using IndexColumnPtr = std::unique_ptr<IIndexColumn>;
using IndexColumns = std::vector<IndexColumnPtr>;

class PrimaryIndex : private boost::noncopyable
{
public:
    struct Settings
    {
        bool compress = false;
        size_t block_size = 32;
        double max_ratio_to_compress = 0.7;
    };

    PrimaryIndex() = default;
    explicit PrimaryIndex(Columns columns_, const Settings & settings);

    bool empty() const { return columns.empty(); }
    size_t getNumColumns() const { return columns.size(); }
    size_t getNumRows() const { return num_rows; }

    size_t bytes() const;
    size_t allocatedBytes() const;

    bool isCompressed() const;
    Columns getRawColumns() const;

    void get(size_t col_idx, size_t row_idx, Field & field) const;
    Field get(size_t col_idx, size_t row_idx) const;

private:
    size_t num_rows;
    IndexColumns columns;
};

using PrimaryIndexPtr = std::shared_ptr<const PrimaryIndex>;

class IndexColumnGeneric : public IIndexColumn
{
public:
    explicit IndexColumnGeneric(ColumnPtr column_) : column(std::move(column_)) {}

    size_t bytes() const override { return column->byteSize(); }
    size_t allocatedBytes() const override { return column->allocatedBytes(); }
    void get(size_t n, Field & field) const override { column->get(n, field); }
    bool isCompressed() const override { return false; }
    ColumnPtr getRawColumn() const override { return column; }

private:
    ColumnPtr column;
};

struct RawBlock
{
    ColumnPtr column;

    size_t bytes() const { return column->byteSize(); }
    size_t allocatedBytes() const { return column->allocatedBytes(); }
    void get(size_t n, Field & field) const { column->get(n, field); }
};

template <typename T>
struct ConstBlock
{
    T value;

    size_t bytes() const { return sizeof(T); }
    size_t allocatedBytes() const { return sizeof(T); }
    void get(size_t, Field & field) const { field = value; }
};

template <typename Offset>
struct RLEBlock
{
    ColumnPtr column;
    PaddedPODArray<Offset> offsets;

    size_t bytes() const { return column->byteSize() + offsets.size() * sizeof(offsets[0]); }
    size_t allocatedBytes() const { return column->allocatedBytes() + offsets.allocated_bytes(); }

    void get(size_t n, Field & field) const
    {
        auto it = std::upper_bound(offsets.begin(), offsets.end(), n);
        chassert(it != offsets.begin());
        n = it - offsets.begin() - 1;
        column->get(n, field);
    }
};

template <typename T>
struct DeltaBlock
{
    T base;
    UInt8 bit_size;
    PaddedPODArray<UInt64> delta;

    size_t bytes() const
    {
        return sizeof(T) + sizeof(UInt8) + delta.size() * sizeof(delta[0]);
    }

    size_t allocatedBytes() const
    {
        return sizeof(T) + sizeof(UInt8) + delta.allocated_bytes();
    }

    void get(size_t n, Field & field) const
    {
        UInt64 delta_value = readBitsPacked(delta.begin(), n * bit_size, bit_size);
        field = base + delta_value;
    }
};

template <bool is_signed>
class IndexColumnInt : public IIndexColumn
{
public:
    using T = std::conditional_t<is_signed, Int64, UInt64>;
    using Block = std::variant<RawBlock, ConstBlock<T>, DeltaBlock<T>, RLEBlock<UInt8>>;

    IndexColumnInt(ColumnPtr column, const PrimaryIndex::Settings & settings);

    size_t bytes() const override;
    size_t allocatedBytes() const override;
    void get(size_t n, Field & field) const override;
    bool isCompressed() const override;
    ColumnPtr getRawColumn() const override;

private:
    size_t block_size;
    std::vector<Block> blocks;
};

}
