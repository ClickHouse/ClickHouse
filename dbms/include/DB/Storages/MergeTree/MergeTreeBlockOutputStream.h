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
		auto part_blocks = storage.writer.splitBlockIntoParts(block, structure);
		for (auto & current_block : part_blocks)
		{
			UInt64 temp_index = storage.increment.get();
			MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block, temp_index, structure);
			storage.data.renameTempPartAndAdd(part, &storage.increment, structure);
			storage.merge(2);
		}
	}

private:
	StorageMergeTree & storage;
	MergeTreeData::LockedTableStructurePtr structure;
};

}
