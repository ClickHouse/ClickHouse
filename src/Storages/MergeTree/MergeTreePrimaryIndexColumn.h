#pragma once
#include <Columns/IColumn.h>

namespace DB
{

struct IIndexBlock
{
    enum class Type : UInt8
    {
        Raw,
        Const,
        Delta,
        RLE,
    };

    virtual ~IIndexBlock() = default;
    virtual size_t bytes() const = 0;
    virtual size_t allocatedBytes() const = 0;
    virtual void get(size_t n, Field & field) const = 0;
    // virtual Type getType() const = 0;
};

using IndexBlockPtr = std::unique_ptr<IIndexBlock>;
using IndexBlocks = std::vector<IndexBlockPtr>;

class IIndexColumn
{
public:
    virtual ~IIndexColumn() = default;
    virtual size_t bytes() const = 0;
    virtual size_t allocatedBytes() const = 0;
    virtual bool isCompressed() const = 0;
    virtual void get(size_t n, Field & field) const = 0;
    virtual ColumnPtr getRawColumn() const = 0;
};

using IndexColumnPtr = std::unique_ptr<IIndexColumn>;
using IndexColumns = std::vector<IndexColumnPtr>;

struct IndexBlockRaw final : IIndexBlock
{
    ColumnPtr column;

    explicit IndexBlockRaw(ColumnPtr column_) : column(std::move(column_)) {}

    size_t bytes() const override { return column->byteSize(); }
    size_t allocatedBytes() const override { return column->allocatedBytes(); }
    void get(size_t n, Field & field) const override { column->get(n, field); }
};

template <typename Offset>
struct IndexBlockRLE final : public IIndexBlock
{
    ColumnPtr column;
    PaddedPODArray<Offset> offsets;

    size_t bytes() const override { return column->byteSize() + offsets.size() * sizeof(Offset); }
    size_t allocatedBytes() const override { return column->allocatedBytes() + offsets.allocated_bytes(); }

    void get(size_t n, Field & field) const override
    {
        auto it = std::upper_bound(offsets.begin(), offsets.end(), n);
        chassert(it != offsets.begin());

        n = it - offsets.begin() - 1;
        column->get(n, field);
    }
};

class IndexColumnCommon final : public IIndexColumn
{
public:
    IndexColumnCommon(IndexBlocks blocks_, size_t block_size_);

    size_t bytes() const override;
    size_t allocatedBytes() const override;
    bool isCompressed() const override;

    void get(size_t n, Field & field) const override;
    ColumnPtr getRawColumn() const override;

private:
    IndexBlocks blocks;
    size_t block_size;
};

class IndexColumnLowCardinality : public IIndexColumn
{
public:
    IndexColumnLowCardinality(IndexColumnPtr index_, ColumnPtr dictionary_);

    size_t bytes() const override;
    size_t allocatedBytes() const override;
    bool isCompressed() const override;

    void get(size_t n, Field & field) const override;
    ColumnPtr getRawColumn() const override;

private:
    IndexColumnPtr index;
    ColumnPtr dictionary;
};

IndexColumnPtr createIndexColumnRaw(ColumnPtr column);
IndexColumnPtr createIndexColumnLowCardinality(ColumnPtr column, size_t block_size, double max_ratio_to_compress);

}
