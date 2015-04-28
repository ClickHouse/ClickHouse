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

	void writePrefix() override
	{
		storage.data.delayInsertIfNeeded();
	}

	void write(const Block & block) override
	{
		auto part_blocks = storage.writer.splitBlockIntoParts(block);
		for (auto & current_block : part_blocks)
		{
			UInt64 temp_index = storage.increment.get();
			MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block, temp_index);
			storage.data.renameTempPartAndAdd(part, &storage.increment);
			storage.merge_task_handle->wake();
		}
	}

private:
	StorageMergeTree & storage;
};

}
