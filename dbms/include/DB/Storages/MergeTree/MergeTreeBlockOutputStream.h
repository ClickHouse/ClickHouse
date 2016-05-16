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
		/// Если слишком много кусков - делаем внеочередные мерджи, синхронно, в текущем потоке.
		/// Почему 10? - на всякий случай, вместо бесконечного цикла.
		for (size_t i = 0; i < 10; ++i)
		{
			size_t parts_count = storage.data.getMaxPartsCountForMonth();
			if (parts_count <= storage.data.settings.parts_to_delay_insert)
				break;

			ProfileEvents::increment(ProfileEvents::SynchronousMergeOnInsert);
			storage.merge(0, {}, {}, {}, {});
		}
	}

	void write(const Block & block) override
	{
		auto part_blocks = storage.writer.splitBlockIntoParts(block);
		for (auto & current_block : part_blocks)
		{
			Int64 temp_index = storage.increment.get();
			MergeTreeData::MutableDataPartPtr part = storage.writer.writeTempPart(current_block, temp_index);
			storage.data.renameTempPartAndAdd(part, &storage.increment);

			/// Инициируем асинхронный мердж - он будет произведён, если пора делать мердж и если в background_pool-е есть место.
			storage.merge_task_handle->wake();
		}
	}

private:
	StorageMergeTree & storage;
};

}
