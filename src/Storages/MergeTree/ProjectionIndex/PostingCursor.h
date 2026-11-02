#pragma once

#include <base/types.h>
#include <Common/PODArray.h>

#include <roaring/roaring.hh>

#include <memory>
#include <vector>

namespace DB
{

class IColumn;

class IPostingCursor
{
public:
    virtual ~IPostingCursor() = default;

    virtual void fill(UInt8 * out, size_t row_offset, size_t num_rows) = 0;

    virtual UInt32 cardinality() const { return 0; }
    virtual double density() const { return 0.0; }
};

using PostingCursorPtr = std::shared_ptr<IPostingCursor>;
using PostingList = roaring::Roaring;
using PostingListPtr = std::shared_ptr<PostingList>;

/// Cursor backed by a pre-materialized Roaring bitmap.
/// Used for has_block_index=0 tokens that are fully materialized during granule loading.
class BitmapCursor final : public IPostingCursor
{
public:
    explicit BitmapCursor(PostingListPtr bitmap_)
        : bitmap(std::move(bitmap_))
    {
    }

    void fill(UInt8 * out, size_t row_offset, size_t num_rows) override
    {
        if (!bitmap)
            return;

        uint32_t range_begin = static_cast<uint32_t>(row_offset);
        uint32_t range_end = static_cast<uint32_t>(row_offset + num_rows);

        roaring::api::roaring_uint32_iterator_t it;
        roaring_iterator_init(&bitmap->roaring, &it);
        if (!roaring_uint32_iterator_move_equalorlarger(&it, range_begin))
            return;

        while (it.current_value < range_end)
        {
            out[it.current_value - range_begin] = 1;
            if (!roaring_uint32_iterator_advance(&it))
                break;
        }
    }

    UInt32 cardinality() const override { return bitmap ? static_cast<UInt32>(bitmap->cardinality()) : 0; }

    double density() const override
    {
        if (!bitmap || bitmap->isEmpty())
            return 0.0;
        /// Compute span in UInt64 to avoid wrap when bitmap covers the full UInt32 range
        /// (max=UINT32_MAX, min=0 → span would wrap to 0 in UInt32 → division by zero → inf,
        /// which is UB when used as a sort key).
        UInt64 span = static_cast<UInt64>(bitmap->maximum()) - bitmap->minimum() + 1;
        return static_cast<double>(bitmap->cardinality()) / static_cast<double>(span);
    }

private:
    PostingListPtr bitmap;
};
class OrCursor final : public IPostingCursor
{
public:
    explicit OrCursor(std::vector<PostingCursorPtr> children);

    void fill(UInt8 * out, size_t row_offset, size_t num_rows) override;

private:
    std::vector<PostingCursorPtr> children;
};
class AndCursor final : public IPostingCursor
{
public:
    explicit AndCursor(std::vector<PostingCursorPtr> children);

    void fill(UInt8 * out, size_t row_offset, size_t num_rows) override;

private:
    void fillBitmapPath(UInt8 * out, size_t row_offset, size_t num_rows, size_t start_child);
    void fillSeekPath(UInt8 * out, size_t row_offset, size_t num_rows, size_t set_count, size_t start_child);

    static constexpr size_t SEEK_THRESHOLD = 4096;

    std::vector<PostingCursorPtr> children;
    bool all_children_are_leaf = false;
    PaddedPODArray<UInt8> tmp_buf;
    PaddedPODArray<uint32_t> doc_ids_buf;
};

}
