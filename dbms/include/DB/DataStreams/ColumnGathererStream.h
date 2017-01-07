#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Storages/IStorage.h>
#include <DB/Common/PODArray.h>


namespace DB
{


/// Tiny struct, stores number of a Part from which current row was fetched, and insertion flag.
struct RowSourcePart
{
	RowSourcePart() = default;

	RowSourcePart(size_t source_num, bool flag = false)
	{
		static_assert(sizeof(*this) == 1, "Size of RowSourcePart is too big due to compiler settings");
		setSourceNum(source_num);
		setSkipFlag(flag);
	}

	/// Data is equal to getSourceNum() if flag is false
	UInt8 getData() const		{ return data; }

	size_t getSourceNum() const { return data & MASK_NUMBER; }

	/// In CollapsingMergeTree case flag means "skip this rows"
	bool getSkipFlag() const 	{ return (data & MASK_FLAG) != 0; }

	void setSourceNum(size_t source_num)
	{
		data = (data & MASK_FLAG) | (static_cast<UInt8>(source_num) & MASK_NUMBER);
	}

	void setSkipFlag(bool flag)
	{
		data = flag ? data | MASK_FLAG : data & ~MASK_FLAG;
	}

	static constexpr size_t MAX_PARTS = 0x7F;
	static constexpr UInt8 MASK_NUMBER = 0x7F;
	static constexpr UInt8 MASK_FLAG = 0x80;

private:
	UInt8 data;
};

using MergedRowSources = PODArray<RowSourcePart>;


/** Gather single stream from multiple streams according to streams mask.
  * Stream mask maps row number to index of source stream.
  * Streams should conatin exactly one column.
  */
class ColumnGathererStream : public IProfilingBlockInputStream
{
public:
	ColumnGathererStream(const BlockInputStreams & source_streams, const String & column_name_,
						 const MergedRowSources & row_source_, size_t block_preferred_size_ = DEFAULT_MERGE_BLOCK_SIZE);

	String getName() const override { return "ColumnGatherer"; }

	String getID() const override;

	Block readImpl() override;

	void readSuffixImpl() override;

private:

	String name;
	ColumnWithTypeAndName column;
	const MergedRowSources & row_source;

	/// Cache required fileds
	struct Source
	{
		const IColumn * column;
		size_t pos;
		size_t size;
		Block block;

		Source(Block && block_, const String & name) : block(std::move(block_))
		{
			update(name);
		}

		void update(const String & name)
		{
			column = block.getByName(name).column.get();
			size = block.rows();
			pos = 0;
		}
	};

	void fetchNewBlock(Source & source, size_t source_num);

	std::vector<Source> sources;

	size_t pos_global_start = 0;
	size_t block_preferred_size;

	Logger * log = &Logger::get("ColumnGathererStream");
};

}
