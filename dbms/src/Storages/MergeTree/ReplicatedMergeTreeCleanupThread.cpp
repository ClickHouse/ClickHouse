#include <DB/Storages/MergeTree/ReplicatedMergeTreeCleanupThread.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Common/setThreadName.h>


namespace DB
{

ReplicatedMergeTreeCleanupThread::ReplicatedMergeTreeCleanupThread(StorageReplicatedMergeTree & storage_)
	: storage(storage_),
	log(&Logger::get(storage.database_name + "." + storage.table_name + " (StorageReplicatedMergeTree, CleanupThread)")),
	thread([this] { run(); }) {}


void ReplicatedMergeTreeCleanupThread::run()
{
	setThreadName("ReplMTCleanup");

	const auto CLEANUP_SLEEP_MS = 30 * 1000;

	while (!storage.shutdown_called)
	{
		try
		{
			iterate();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}

		storage.shutdown_event.tryWait(CLEANUP_SLEEP_MS);
	}

	LOG_DEBUG(log, "Cleanup thread finished");
}


void ReplicatedMergeTreeCleanupThread::iterate()
{
	clearOldParts();

	if (storage.unreplicated_data)
		storage.unreplicated_data->clearOldParts();

	if (storage.is_leader_node)
	{
		clearOldLogs();
		clearOldBlocks();
	}
}


void ReplicatedMergeTreeCleanupThread::clearOldParts()
{
	auto table_lock = storage.lockStructure(false);
	auto zookeeper = storage.getZooKeeper();

	MergeTreeData::DataPartsVector parts = storage.data.grabOldParts();
	size_t count = parts.size();

	if (!count)
	{
		LOG_TRACE(log, "No old parts");
		return;
	}

	try
	{
		while (!parts.empty())
		{
			MergeTreeData::DataPartPtr & part = parts.back();

			LOG_DEBUG(log, "Removing " << part->name);

			zkutil::Ops ops;
			storage.removePartFromZooKeeper(part->name, ops);
			auto code = zookeeper->tryMulti(ops);
			if (code != ZOK)
				LOG_WARNING(log, "Couldn't remove " << part->name << " from ZooKeeper: " << zkutil::ZooKeeper::error2string(code));

			part->remove();
			parts.pop_back();
		}
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
		storage.data.addOldParts(parts);
		throw;
	}

	LOG_DEBUG(log, "Removed " << count << " old parts");
}


void ReplicatedMergeTreeCleanupThread::clearOldLogs()
{
	auto zookeeper = storage.getZooKeeper();

	zkutil::Stat stat;
	if (!zookeeper->exists(storage.zookeeper_path + "/log", &stat))
		throw Exception(storage.zookeeper_path + "/log doesn't exist", ErrorCodes::NOT_FOUND_NODE);

	int children_count = stat.numChildren;

	/// Будем ждать, пока накопятся в 1.1 раза больше записей, чем нужно.
	if (static_cast<double>(children_count) < storage.data.settings.replicated_logs_to_keep * 1.1)
		return;

	Strings replicas = zookeeper->getChildren(storage.zookeeper_path + "/replicas", &stat);
	UInt64 min_pointer = std::numeric_limits<UInt64>::max();
	for (const String & replica : replicas)
	{
		String pointer = zookeeper->get(storage.zookeeper_path + "/replicas/" + replica + "/log_pointer");
		if (pointer.empty())
			return;
		min_pointer = std::min(min_pointer, parse<UInt64>(pointer));
	}

	Strings entries = zookeeper->getChildren(storage.zookeeper_path + "/log");
	std::sort(entries.begin(), entries.end());

	/// Не будем трогать последние replicated_logs_to_keep записей.
	entries.erase(entries.end() - std::min(entries.size(), storage.data.settings.replicated_logs_to_keep), entries.end());
	/// Не будем трогать записи, не меньшие min_pointer.
	entries.erase(std::lower_bound(entries.begin(), entries.end(), "log-" + storage.padIndex(min_pointer)), entries.end());

	if (entries.empty())
		return;

	zkutil::Ops ops;
	for (size_t i = 0; i < entries.size(); ++i)
	{
		ops.push_back(new zkutil::Op::Remove(storage.zookeeper_path + "/log/" + entries[i], -1));

		if (ops.size() > 400 || i + 1 == entries.size())
		{
			/// Одновременно с очисткой лога проверим, не добавилась ли реплика с тех пор, как мы получили список реплик.
			ops.push_back(new zkutil::Op::Check(storage.zookeeper_path + "/replicas", stat.version));
			zookeeper->multi(ops);
			ops.clear();
		}
	}

	LOG_DEBUG(log, "Removed " << entries.size() << " old log entries: " << entries.front() << " - " << entries.back());
}


void ReplicatedMergeTreeCleanupThread::clearOldBlocks()
{
	auto zookeeper = storage.getZooKeeper();

	zkutil::Stat stat;
	if (!zookeeper->exists(storage.zookeeper_path + "/blocks", &stat))
		throw Exception(storage.zookeeper_path + "/blocks doesn't exist", ErrorCodes::NOT_FOUND_NODE);

	int children_count = stat.numChildren;

	/// Чтобы делать "асимптотически" меньше запросов exists, будем ждать, пока накопятся в 1.1 раза больше блоков, чем нужно.
	if (static_cast<double>(children_count) < storage.data.settings.replicated_deduplication_window * 1.1)
		return;

	LOG_TRACE(log, "Clearing about " << static_cast<size_t>(children_count) - storage.data.settings.replicated_deduplication_window
		<< " old blocks from ZooKeeper. This might take several minutes.");

	Strings blocks = zookeeper->getChildren(storage.zookeeper_path + "/blocks");

	std::vector<std::pair<Int64, String> > timed_blocks;

	for (const String & block : blocks)
	{
		zkutil::Stat stat;
		zookeeper->exists(storage.zookeeper_path + "/blocks/" + block, &stat);
		timed_blocks.push_back(std::make_pair(stat.czxid, block));
	}

	//zkutil::Ops ops;
	std::sort(timed_blocks.begin(), timed_blocks.end(), std::greater<std::pair<Int64, String>>());
	for (size_t i = storage.data.settings.replicated_deduplication_window; i < timed_blocks.size(); ++i)
	{
		/// Устаревшие ноды. Этот код можно будет убрать через пол года.
		zookeeper->tryRemove(storage.zookeeper_path + "/blocks/" + timed_blocks[i].second + "/columns");
		zookeeper->tryRemove(storage.zookeeper_path + "/blocks/" + timed_blocks[i].second + "/checksums");
		zookeeper->tryRemove(storage.zookeeper_path + "/blocks/" + timed_blocks[i].second + "/checksum");
		zookeeper->tryRemove(storage.zookeeper_path + "/blocks/" + timed_blocks[i].second + "/number");
		zookeeper->tryRemove(storage.zookeeper_path + "/blocks/" + timed_blocks[i].second);

		/*ops.push_back(new zkutil::Op::Remove(storage.zookeeper_path + "/blocks/" + timed_blocks[i].second + "/number", -1));
		ops.push_back(new zkutil::Op::Remove(storage.zookeeper_path + "/blocks/" + timed_blocks[i].second + "/checksum", -1));
		ops.push_back(new zkutil::Op::Remove(storage.zookeeper_path + "/blocks/" + timed_blocks[i].second, -1));

		if (ops.size() > 400 || i + 1 == timed_blocks.size())
		{
			zookeeper->multi(ops);
			ops.clear();
		}*/
	}

	LOG_TRACE(log, "Cleared " << blocks.size() - storage.data.settings.replicated_deduplication_window << " old blocks from ZooKeeper");
}

}
