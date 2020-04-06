#pragma once

#include <DataStreams/IBlockInputStream.h>

#include <Common/HashTable/HashMap.h>
#include <Common/UInt128.h>


namespace DB
{

/** Implements LIMIT BY clause witch can be used to obtain a "top N by subgroup".
  *
  * For example, if you have table T like this (Num: 1 1 3 3 3 4 4 5 7 7 7 7),
  * the query SELECT Num FROM T LIMIT 2 BY Num
  *    will give you the following result: (Num: 1 1 3 3 4 4 5 7 7).
  */
class LimitByBlockInputStream : public IBlockInputStream
{
public:
    LimitByBlockInputStream(const BlockInputStreamPtr & input, size_t group_length_, size_t group_offset_, const Names & columns);

    String getName() const override { return "LimitBy"; }

    Block getHeader() const override { return children.at(0)->getHeader(); }

protected:
    Block readImpl() override;

private:
    ColumnRawPtrs getKeyColumns(Block & block) const;

private:
    using MapHashed = HashMap<UInt128, UInt64, UInt128TrivialHash>;

    const Names columns_names;
    const size_t group_length;
    const size_t group_offset;
    MapHashed keys_counts;
};

}
