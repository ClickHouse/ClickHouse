#pragma once

#include <DB/Storages/StorageMergeTree.h>

namespace DB
{

class MergeTreeBlockOutputStream : public IBlockOutputStream
{
public:
	MergeTreeBlockOutputStream(StorageMergeTree & storage_)
		: storage(storage_) {}

	void write(const Block & block)
	{
		auto part_blocks = storage.writer.splitBlockIntoParts(block);
		for (auto & current_block : part_blocks)
		{
			size_t parts_count = storage.data.getDataPartsCount();
			if (parts_count > storage.data.settings.parts_to_delay_insert)
			{
				double delay = std::pow(storage.data.settings.insert_delay_step, parts_count - storage.data.settings.parts_to_delay_insert);
				delay /= 1000;
				LOG_INFO(storage.log, "Delaying inserting block by " << delay << "s because there are " << parts_count << " parts");
				std::this_thread::sleep_for(std::chrono::duration<double>(delay));
			}

			UInt64 temp_index = storage.increment.get();
			MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block, temp_index);
			storage.data.renameTempPartAndAdd(part, &storage.increment);
		}
	}

private:
	StorageMergeTree & storage;
};

}
