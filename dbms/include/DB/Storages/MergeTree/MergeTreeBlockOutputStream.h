#pragma once

#include <DB/Storages/StorageMergeTree.h>

namespace DB
{

class MergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
	MergeTreeBlockOutputStream(StoragePtr storage_)
		: IBlockOutputStream(storage_), storage(dynamic_cast<StorageMergeTree &>(*storage_)),
		structure(storage.data.getLockedStructure(true)) {}

	void write(const Block & block)
	{
		BlocksList part_blocks = storage.writer.splitBlockIntoParts(block, structure);
		for (const Block & current_block : part_blocks)
		{
			UInt64 temp_index = storage.increment.get();
			String temp_name = storage.writer.writeTempPart(current_block, temp_index, structure);
			storage.writer.renameTempPart(temp_name, &storage.increment);
		}
	}

private:
	StorageMergeTree & storage;
	MergeTreeData::LockedTableStructurePtr structure;
};

}
