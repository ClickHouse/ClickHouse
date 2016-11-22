#pragma once

#include <DB/DataStreams/IProfilingBlockInputStream.h>
#include <DB/Storages/IStorage.h>
#include <DB/Common/PODArray.h>


namespace DB
{


struct __attribute__((__packed__)) RowSourcePart
{
	unsigned int flag: 1;
	unsigned int source_id: 7;

	RowSourcePart(unsigned source_id_, bool flag_ = false)
	{
		source_id = source_id_;
		flag = flag_;
	}

	static constexpr size_t MAX_PARTS = 127;
};

using MergedRowSources = PODArray<RowSourcePart>;


/** Gather single stream from multiple streams according to streams mask.
  * Stream mask maps row number to index of source stream.
  * Streams should conatin exactly one column.
  */
class ColumnGathererStream : public IProfilingBlockInputStream
{
public:
	ColumnGathererStream(const BlockInputStreams & source_streams, const MergedRowSources& pos_to_source_idx_,
						 size_t block_size_ = DEFAULT_BLOCK_SIZE);

	String getName() const override { return "ColumnGatherer"; }

	String getID() const override;

	Block readImpl() override;

private:

	const MergedRowSources & pos_to_source_idx;

	/// Cache required fileds
	struct Source
	{
		const IColumn * column;
		size_t pos;
		size_t size;
		Block block;

		Source(Block && block_) : block(std::move(block_))
		{
			update();
		}

		void update()
		{
			column = block.getByPosition(0).column.get();
			size = block.rowsInFirstColumn();
			pos = 0;
		}
	};

	std::vector<Source> sources;

	size_t pos_global = 0;
	size_t block_size;

	Logger * log = &Logger::get("ColumnGathererStream");
};

}
