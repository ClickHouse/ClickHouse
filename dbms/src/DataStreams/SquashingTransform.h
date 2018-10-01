#pragma once

#include <Core/Block.h>


namespace DB
{


/** Merging consecutive passed blocks to specified minimum size.
  *
  * (But if one of input blocks has already at least specified size,
  *  then don't merge it with neighbours, even if neighbours are small.)
  *
  * Used to prepare blocks to adequate size for INSERT queries,
  *  because such storages as Memory, StripeLog, Log, TinyLog...
  *  store or compress data in blocks exactly as passed to it,
  *  and blocks of small size are not efficient.
  *
  * Order of data is kept.
  */
class SquashingTransform
{
public:
    /// Conditions on rows and bytes are OR-ed. If one of them is zero, then corresponding condition is ignored.
    SquashingTransform(size_t min_block_size_rows, size_t min_block_size_bytes);

    /// When not ready, you need to pass more blocks to add function.
    struct Result
    {
        bool ready = false;
        MutableColumns columns;

        Result(bool ready_) : ready(ready_) {}
        Result(MutableColumns && columns) : ready(true), columns(std::move(columns)) {}
    };

    /** Add next block and possibly returns squashed block.
      * At end, you need to pass empty block. As the result for last (empty) block, you will get last Result with ready = true.
      */
    Result add(MutableColumns && columns);

private:
    size_t min_block_size_rows;
    size_t min_block_size_bytes;

    MutableColumns accumulated_columns;

    void append(MutableColumns && columns);

    bool isEnoughSize(const MutableColumns & columns);
    bool isEnoughSize(size_t rows, size_t bytes) const;
};

}
