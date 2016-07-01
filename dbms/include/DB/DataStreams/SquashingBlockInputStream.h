#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>


namespace DB
{

/** Merging consequtive blocks of stream to specified minimum size.
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
class SquashingBlockInputStream : public IProfilingBlockInputStream
{
public:
	/// Conditions on rows and bytes are OR-ed. If one of them is zero, then corresponding condition is ignored.
	SquashingBlockInputStream(BlockInputStreamPtr & src, size_t min_block_size_rows, size_t min_block_size_bytes);

	String getName() const override { return "Squashing"; }

	String getID() const override
	{
		std::stringstream res;
		res << "Squashing(" << children.at(0)->getID() << ")";
		return res.str();
	}

protected:
	Block readImpl() override;

private:
	size_t min_block_size_rows;
	size_t min_block_size_bytes;

	Block accumulated_block;
	bool all_read = false;

	void append(Block && block);

	bool isEnoughSize(size_t rows, size_t bytes) const;
};

}
