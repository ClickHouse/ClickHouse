#pragma once

#include <DataStreams/IProfilingBlockInputStream.h>

#include <Common/HashTable/HashMap.h>
#include <Common/SipHash.h>
#include <Common/UInt128.h>

namespace DB
{

/** Implements LIMIT BY clause witch can be used to obtain a "top N by subgroup".
  *
  * For example, if you have table T like this (Num: 1 1 3 3 3 4 4 5 7 7 7 7),
  * the query SELECT Num FROM T LIMIT 2 BY Num
  *    will give you the following result: (Num: 1 1 3 3 4 4 5 7 7).
  */
class LimitByBlockInputStream : public IProfilingBlockInputStream
{
public:
    LimitByBlockInputStream(BlockInputStreamPtr input_, size_t group_size_, Names columns_);

    String getName() const override { return "LimitBy"; }

    String getID() const override;

protected:
    Block readImpl() override;

private:
    ConstColumnPlainPtrs getKeyColumns(Block & block) const;

private:
    using MapHashed = HashMap<UInt128, UInt64, UInt128TrivialHash>;

    const Names columns_names;
    const size_t group_size;
    MapHashed keys_counts;
};

}
