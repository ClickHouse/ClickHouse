#pragma once

#include <DB/Storages/StorageMergeTree.h>
#include <DB/DataStreams/IBlockOutputStream.h>
#include <iomanip>


namespace DB
{

class MergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
	MergeTreeBlockOutputStream(StorageMergeTree & storage_)
		: storage(storage_) {}

	void write(const Block & block) override
	{
		storage.data.delayInsertIfNeeded();

		auto part_blocks = storage.writer.splitBlockIntoParts(block);
		for (auto & current_block : part_blocks)
		{
			Int64 temp_index = storage.increment.get();
			MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block, temp_index);
			storage.data.renameTempPartAndAdd(part, &storage.increment);

			/// Initiate async merge - it will be done if it's good time for merge and if there are space in 'background_pool'.
			storage.merge_task_handle->wake();
		}
	}

private:
	StorageMergeTree & storage;
};

}
