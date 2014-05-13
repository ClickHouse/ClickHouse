#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreePartsExchange.h>
#include <DB/Parsers/formatAST.h>
#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/IO/ReadBufferFromString.h>
#include <time.h>

namespace DB
{


const auto QUEUE_UPDATE_SLEEP = std::chrono::seconds(5);
const auto QUEUE_NO_WORK_SLEEP = std::chrono::seconds(5);
const auto QUEUE_ERROR_SLEEP = std::chrono::seconds(1);
const auto QUEUE_AFTER_WORK_SLEEP = std::chrono::seconds(0);
const auto MERGE_SELECTING_SLEEP = std::chrono::seconds(5);


StorageReplicatedMergeTree::StorageReplicatedMergeTree(
	const String & zookeeper_path_,
	const String & replica_name_,
	bool attach,
	const String & path_, const String & database_name_, const String & name_,
	NamesAndTypesListPtr columns_,
	Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_,
	const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	MergeTreeData::Mode mode_,
	const String & sign_column_,
	const MergeTreeSettings & settings_)
	:
	context(context_), zookeeper(context.getZooKeeper()),
	table_name(name_), full_path(path_ + escapeForFileName(table_name) + '/'), zookeeper_path(zookeeper_path_),
	replica_name(replica_name_),
	data(	full_path, columns_, context_, primary_expr_ast_, date_column_name_, sampling_expression_,
			index_granularity_, mode_, sign_column_, settings_, database_name_ + "." + table_name),
	reader(data), writer(data), merger(data), fetcher(data),
	log(&Logger::get(database_name_ + "." + table_name + " (StorageReplicatedMergeTree)"))
{
	if (!zookeeper_path.empty() && *zookeeper_path.rbegin() == '/')
		zookeeper_path.erase(zookeeper_path.end() - 1);
	replica_path = zookeeper_path + "/replicas/" + replica_name;

	if (!attach)
	{
		if (!zookeeper->exists(zookeeper_path))
			createTable();

		if (!isTableEmpty())
			throw Exception("Can't add new replica to non-empty table", ErrorCodes::ADDING_REPLICA_TO_NON_EMPTY_TABLE);

		checkTableStructure();
		createReplica();
	}
	else
	{
		checkTableStructure();
		checkParts();
	}

	loadQueue();

	String unreplicated_path = full_path + "unreplicated/";
	if (Poco::File(unreplicated_path).exists())
	{
		LOG_INFO(log, "Have unreplicated data");
		unreplicated_data.reset(new MergeTreeData(unreplicated_path, columns_, context_, primary_expr_ast_,
			date_column_name_, sampling_expression_, index_granularity_, mode_, sign_column_, settings_,
			database_name_ + "." + table_name + "[unreplicated]"));
		unreplicated_reader.reset(new MergeTreeDataSelectExecutor(*unreplicated_data));
		unreplicated_merger.reset(new MergeTreeDataMerger(*unreplicated_data));
	}

	/// Сгенерируем этому экземпляру случайный идентификатор.
	struct timespec times;
	if (clock_gettime(CLOCK_THREAD_CPUTIME_ID, &times))
		throwFromErrno("Cannot clock_gettime.", ErrorCodes::CANNOT_CLOCK_GETTIME);
	active_node_identifier = toString(times.tv_nsec);

	restarting_thread = std::thread(&StorageReplicatedMergeTree::restartingThread, this);
}

StoragePtr StorageReplicatedMergeTree::create(
	const String & zookeeper_path_,
	const String & replica_name_,
	bool attach,
	const String & path_, const String & database_name_, const String & name_,
	NamesAndTypesListPtr columns_,
	Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_,
	const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	MergeTreeData::Mode mode_,
	const String & sign_column_,
	const MergeTreeSettings & settings_)
{
	StorageReplicatedMergeTree * res = new StorageReplicatedMergeTree(zookeeper_path_, replica_name_, attach,
		path_, database_name_, name_, columns_, context_, primary_expr_ast_, date_column_name_, sampling_expression_,
		index_granularity_, mode_, sign_column_, settings_);
	StoragePtr res_ptr = res->thisPtr();
	String endpoint_name = "ReplicatedMergeTree:" + res->replica_path;
	InterserverIOEndpointPtr endpoint = new ReplicatedMergeTreePartsServer(res->data, res_ptr);
	res->endpoint_holder = new InterserverIOEndpointHolder(endpoint_name, endpoint, res->context.getInterserverIOHandler());
	return res_ptr;
}

static String formattedAST(const ASTPtr & ast)
{
	if (!ast)
		return "";
	std::stringstream ss;
	formatAST(*ast, ss, 0, false, true);
	return ss.str();
}

void StorageReplicatedMergeTree::createTable()
{
	zookeeper->create(zookeeper_path, "", zkutil::CreateMode::Persistent);

	/// Запишем метаданные таблицы, чтобы реплики могли сверять с ними свою локальную структуру таблицы.
	std::stringstream metadata;
	metadata << "metadata format version: 1" << std::endl;
	metadata << "date column: " << data.date_column_name << std::endl;
	metadata << "sampling expression: " << formattedAST(data.sampling_expression) << std::endl;
	metadata << "index granularity: " << data.index_granularity << std::endl;
	metadata << "mode: " << static_cast<int>(data.mode) << std::endl;
	metadata << "sign column: " << data.sign_column << std::endl;
	metadata << "primary key: " << formattedAST(data.primary_expr_ast) << std::endl;
	metadata << "columns:" << std::endl;
	WriteBufferFromOStream buf(metadata);
	for (auto & it : data.getColumnsList())
	{
		writeBackQuotedString(it.first, buf);
		writeChar(' ', buf);
		writeString(it.second->getName(), buf);
		writeChar('\n', buf);
	}
	buf.next();

	zookeeper->create(zookeeper_path + "/metadata", metadata.str(), zkutil::CreateMode::Persistent);

	zookeeper->create(zookeeper_path + "/replicas", "", zkutil::CreateMode::Persistent);
	zookeeper->create(zookeeper_path + "/blocks", "", zkutil::CreateMode::Persistent);
	zookeeper->create(zookeeper_path + "/block_numbers", "", zkutil::CreateMode::Persistent);
	zookeeper->create(zookeeper_path + "/leader_election", "", zkutil::CreateMode::Persistent);
	zookeeper->create(zookeeper_path + "/temp", "", zkutil::CreateMode::Persistent);
}

/** Проверить, что список столбцов и настройки таблицы совпадают с указанными в ZK (/metadata).
	* Если нет - бросить исключение.
	*/
void StorageReplicatedMergeTree::checkTableStructure()
{
	String metadata_str = zookeeper->get(zookeeper_path + "/metadata");
	ReadBufferFromString buf(metadata_str);
	assertString("metadata format version: 1", buf);
	assertString("\ndate column: ", buf);
	assertString(data.date_column_name, buf);
	assertString("\nsampling expression: ", buf);
	assertString(formattedAST(data.sampling_expression), buf);
	assertString("\nindex granularity: ", buf);
	assertString(toString(data.index_granularity), buf);
	assertString("\nmode: ", buf);
	assertString(toString(static_cast<int>(data.mode)), buf);
	assertString("\nsign column: ", buf);
	assertString(data.sign_column, buf);
	assertString("\nprimary key: ", buf);
	assertString(formattedAST(data.primary_expr_ast), buf);
	assertString("\ncolumns:\n", buf);
	for (auto & it : data.getColumnsList())
	{
		String name;
		readBackQuotedString(name, buf);
		if (name != it.first)
			throw Exception("Unexpected column name in ZooKeeper: expected " + it.first + ", found " + name,
				ErrorCodes::UNKNOWN_IDENTIFIER);
		assertString(" ", buf);
		assertString(it.second->getName(), buf);
		assertString("\n", buf);
	}
	assertEOF(buf);
}

void StorageReplicatedMergeTree::createReplica()
{
	zookeeper->create(replica_path, "", zkutil::CreateMode::Persistent);
	zookeeper->create(replica_path + "/host", "", zkutil::CreateMode::Persistent);
	zookeeper->create(replica_path + "/log", "", zkutil::CreateMode::Persistent);
	zookeeper->create(replica_path + "/log_pointers", "", zkutil::CreateMode::Persistent);
	zookeeper->create(replica_path + "/queue", "", zkutil::CreateMode::Persistent);
	zookeeper->create(replica_path + "/parts", "", zkutil::CreateMode::Persistent);
}

void StorageReplicatedMergeTree::activateReplica()
{
	std::stringstream host;
	host << "host: " << context.getInterserverIOHost() << std::endl;
	host << "port: " << context.getInterserverIOPort() << std::endl;

	/** Если нода отмечена как активная, но отметка сделана в этом же экземпляре, удалим ее.
	  * Такое возможно только при истечении сессии в ZooKeeper.
	  * Здесь есть небольшой race condition (можем удалить не ту ноду, для которой сделали tryGet),
	  *  но он крайне маловероятен при нормальном использовании.
	  */
	String data;
	if (zookeeper->tryGet(replica_path + "/is_active", data) && data == active_node_identifier)
		zookeeper->tryRemove(replica_path + "/is_active");

	/// Одновременно объявим, что эта реплика активна, и обновим хост.
	zkutil::Ops ops;
	ops.push_back(new zkutil::Op::Create(replica_path + "/is_active", "", zookeeper->getDefaultACL(), zkutil::CreateMode::Ephemeral));
	ops.push_back(new zkutil::Op::SetData(replica_path + "/host", host.str(), -1));

	try
	{
		zookeeper->multi(ops);
	}
	catch (zkutil::KeeperException & e)
	{
		if (e.code == zkutil::ReturnCode::NodeExists)
			throw Exception("Replica " + replica_path + " appears to be already active. If you're sure it's not, "
				"try again in a minute or remove znode " + replica_path + "/is_active manually", ErrorCodes::REPLICA_IS_ALREADY_ACTIVE);

		throw;
	}

	replica_is_active_node = zkutil::EphemeralNodeHolder::existing(replica_path + "/is_active", *zookeeper);
}

bool StorageReplicatedMergeTree::isTableEmpty()
{
	Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
	for (const auto & replica : replicas)
	{
		if (!zookeeper->getChildren(zookeeper_path + "/replicas/" + replica + "/parts").empty())
			return false;
	}
	return true;
}

void StorageReplicatedMergeTree::checkParts()
{
	Strings expected_parts_vec = zookeeper->getChildren(replica_path + "/parts");
	NameSet expected_parts(expected_parts_vec.begin(), expected_parts_vec.end());

	MergeTreeData::DataParts parts = data.getAllDataParts();

	MergeTreeData::DataParts unexpected_parts;
	for (const auto & part : parts)
	{
		if (expected_parts.count(part->name))
		{
			expected_parts.erase(part->name);
		}
		else
		{
			unexpected_parts.insert(part);
		}
	}

	/// Если локально не хватает какого-то куска, но есть покрывающий его кусок, можно заменить в ZK недостающий покрывающим.
	MergeTreeData::DataPartsVector parts_to_add;
	for (const String & missing_name : expected_parts)
	{
		auto containing = data.getContainingPart(missing_name);
		if (!containing)
			throw Exception("Not found " + toString(expected_parts.size())
				+ " parts (including " + missing_name + ") in table " + data.getTableName(),
				ErrorCodes::NOT_FOUND_EXPECTED_DATA_PART);
		LOG_ERROR(log, "Ignoring missing local part " << missing_name << " because part " << containing->name << " exists");
		if (unexpected_parts.count(containing))
		{
			parts_to_add.push_back(containing);
			unexpected_parts.erase(containing);
		}
	}

	if (parts_to_add.size() > 2 ||
		unexpected_parts.size() > 2 ||
		expected_parts.size() > 20)
	{
		throw Exception("The local set of parts of table " + data.getTableName() + " doesn't look like the set of parts in ZooKeeper."
			" There are " + toString(unexpected_parts.size()) + " unexpected parts, "
			+ toString(parts_to_add.size()) + " unexpectedly merged parts, "
			+ toString(expected_parts.size()) + " unexpectedly obsolete parts",
			ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS);
	}

	/// Добавим в ZK информацию о кусках, покрывающих недостающие куски.
	for (MergeTreeData::DataPartPtr part : parts_to_add)
	{
		LOG_ERROR(log, "Adding unexpected local part to ZooKeeper: " << part->name);
		zkutil::Ops ops;
		checkPartAndAddToZooKeeper(part, ops);
		zookeeper->multi(ops);
	}

	/// Удалим из ZK информацию о кусках, покрытых только что добавленными.
	for (const String & name : expected_parts)
	{
		zkutil::Ops ops;
		ops.push_back(new zkutil::Op::Remove(replica_path + "/parts/" + name + "/checksums", -1));
		ops.push_back(new zkutil::Op::Remove(replica_path + "/parts/" + name, -1));
		zookeeper->multi(ops);
	}

	/// Удалим лишние локальные куски.
	for (MergeTreeData::DataPartPtr part : unexpected_parts)
	{
		LOG_ERROR(log, "Unexpected part " << part->name << ". Renaming it to ignored_" + part->name);
		data.renameAndDetachPart(part, "ignored_");
	}
}

void StorageReplicatedMergeTree::checkPartAndAddToZooKeeper(MergeTreeData::DataPartPtr part, zkutil::Ops & ops)
{
	String another_replica = findReplicaHavingPart(part->name, false);
	if (!another_replica.empty())
	{
		String checksums_str;
		if (zookeeper->tryGet(zookeeper_path + "/replicas/" + another_replica + "/parts/" + part->name + "/checksums", checksums_str))
		{
			auto checksums = MergeTreeData::DataPart::Checksums::parse(checksums_str);
			checksums.checkEqual(part->checksums, true);
		}
	}

	ops.push_back(new zkutil::Op::Create(
		replica_path + "/parts/" + part->name,
		"",
		zookeeper->getDefaultACL(),
		zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(
		replica_path + "/parts/" + part->name + "/checksums",
		part->checksums.toString(),
		zookeeper->getDefaultACL(),
		zkutil::CreateMode::Persistent));
}

void StorageReplicatedMergeTree::clearOldParts()
{
	Strings parts = data.clearOldParts();

	for (const String & name : parts)
	{
		zkutil::Ops ops;
		ops.push_back(new zkutil::Op::Remove(replica_path + "/parts/" + name + "/checksums", -1));
		ops.push_back(new zkutil::Op::Remove(replica_path + "/parts/" + name, -1));
		zkutil::ReturnCode::type code = zookeeper->tryMulti(ops);
		if (code != zkutil::ReturnCode::Ok)
			LOG_WARNING(log, "Couldn't remove part " << name << " from ZooKeeper: " << zkutil::ReturnCode::toString(code));
	}

	if (!parts.empty())
		LOG_DEBUG(log, "Removed " << parts.size() << " old parts");
}

void StorageReplicatedMergeTree::clearOldLogs()
{
	Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
	UInt64 min_pointer = std::numeric_limits<UInt64>::max();
	for (const String & replica : replicas)
	{
		String pointer;
		if (!zookeeper->tryGet(zookeeper_path + "/replicas/" + replica + "/log_pointers/" + replica_name, pointer))
			return;
		min_pointer = std::min(min_pointer, parse<UInt64>(pointer));
	}

	Strings entries = zookeeper->getChildren(replica_path + "/log");
	std::sort(entries.begin(), entries.end());
	size_t removed = 0;

	for (const String & entry : entries)
	{
		UInt64 index = parse<UInt64>(entry.substr(strlen("log-")));
		if (index >= min_pointer)
			break;
		zookeeper->remove(replica_path + "/log/" + entry);
		++removed;
	}

	if (removed > 0)
		LOG_DEBUG(log, "Removed " << removed << " old log entries");
}

void StorageReplicatedMergeTree::clearOldBlocks()
{
	zkutil::Stat stat;
	if (!zookeeper->exists(zookeeper_path + "/blocks", &stat))
		throw Exception(zookeeper_path + "/blocks doesn't exist", ErrorCodes::NOT_FOUND_NODE);

	int children_count = stat.getnumChildren();

	/// Чтобы делать "асимптотически" меньше запросов exists, будем ждать, пока накопятся в 1.1 раза больше блоков, чем нужно.
	if (static_cast<double>(children_count) < data.settings.replicated_deduplication_window * 1.1)
		return;

	LOG_TRACE(log, "Clearing about " << static_cast<size_t>(children_count) - data.settings.replicated_deduplication_window
		<< " old blocks from ZooKeeper");

	Strings blocks = zookeeper->getChildren(zookeeper_path + "/blocks");

	std::vector<std::pair<Int64, String> > timed_blocks;

	for (const String & block : blocks)
	{
		zkutil::Stat stat;
		zookeeper->exists(zookeeper_path + "/blocks/" + block, &stat);
		timed_blocks.push_back(std::make_pair(stat.getczxid(), block));
	}

	std::sort(timed_blocks.begin(), timed_blocks.end(), std::greater<std::pair<Int64, String>>());
	for (size_t i = data.settings.replicated_deduplication_window; i <  timed_blocks.size(); ++i)
	{
		zkutil::Ops ops;
		ops.push_back(new zkutil::Op::Remove(zookeeper_path + "/blocks/" + timed_blocks[i].second + "/number", -1));
		ops.push_back(new zkutil::Op::Remove(zookeeper_path + "/blocks/" + timed_blocks[i].second + "/checksums", -1));
		ops.push_back(new zkutil::Op::Remove(zookeeper_path + "/blocks/" + timed_blocks[i].second, -1));
		zookeeper->multi(ops);
	}

	LOG_TRACE(log, "Cleared " << blocks.size() - data.settings.replicated_deduplication_window << " old blocks from ZooKeeper");
}

void StorageReplicatedMergeTree::loadQueue()
{
	Poco::ScopedLock<Poco::FastMutex> lock(queue_mutex);

	Strings children = zookeeper->getChildren(replica_path + "/queue");
	std::sort(children.begin(), children.end());
	for (const String & child : children)
	{
		String s = zookeeper->get(replica_path + "/queue/" + child);
		LogEntry entry = LogEntry::parse(s);
		entry.znode_name = child;
		entry.tagPartsAsCurrentlyMerging(*this);
		queue.push_back(entry);
	}
}

void StorageReplicatedMergeTree::pullLogsToQueue()
{
	Poco::ScopedLock<Poco::FastMutex> lock(queue_mutex);

	/// Сольем все логи в хронологическом порядке.

	struct LogIterator
	{
		String replica;		/// Имя реплики.
		UInt64 index;		/// Номер записи в логе (суффикс имени ноды).

		Int64 timestamp;	/// Время (czxid) создания записи в логе.
		String entry_str;	/// Сама запись.

		bool operator<(const LogIterator & rhs) const
		{
			return timestamp < rhs.timestamp;
		}

		bool readEntry(zkutil::ZooKeeper & zookeeper, const String & zookeeper_path)
		{
			String index_str = toString(index);
			while (index_str.size() < 10)
				index_str = '0' + index_str;
			zkutil::Stat stat;
			if (!zookeeper.tryGet(zookeeper_path + "/replicas/" + replica + "/log/log-" + index_str, entry_str, &stat))
				return false;
			timestamp = stat.getczxid();
			return true;
		}
	};

	typedef std::priority_queue<LogIterator> PriorityQueue;
	PriorityQueue priority_queue;

	Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");

	for (const String & replica : replicas)
	{
		String index_str;
		UInt64 index;

		if (zookeeper->tryGet(replica_path + "/log_pointers/" + replica, index_str))
		{
			index = parse<UInt64>(index_str);
		}
		else
		{
			/// Если у нас еще нет указателя на лог этой реплики, поставим указатель на первую запись в нем.
			Strings entries = zookeeper->getChildren(zookeeper_path + "/replicas/" + replica + "/log");
			std::sort(entries.begin(), entries.end());
			index = entries.empty() ? 0 : parse<UInt64>(entries[0].substr(strlen("log-")));

			zookeeper->create(replica_path + "/log_pointers/" + replica, toString(index), zkutil::CreateMode::Persistent);
		}

		LogIterator iterator;
		iterator.replica = replica;
		iterator.index = index;

		if (iterator.readEntry(*zookeeper, zookeeper_path))
			priority_queue.push(iterator);
	}

	size_t count = 0;

	while (!priority_queue.empty())
	{
		LogIterator iterator = priority_queue.top();
		priority_queue.pop();
		++count;

		LogEntry entry = LogEntry::parse(iterator.entry_str);

		/// Одновременно добавим запись в очередь и продвинем указатель на лог.
		zkutil::Ops ops;
		ops.push_back(new zkutil::Op::Create(
			replica_path + "/queue/queue-", iterator.entry_str, zookeeper->getDefaultACL(), zkutil::CreateMode::PersistentSequential));
		ops.push_back(new zkutil::Op::SetData(
			replica_path + "/log_pointers/" + iterator.replica, toString(iterator.index + 1), -1));
		auto results = zookeeper->multi(ops);

		String path_created = dynamic_cast<zkutil::OpResult::Create &>((*results)[0]).getPathCreated();
		entry.znode_name = path_created.substr(path_created.find_last_of('/') + 1);
		entry.tagPartsAsCurrentlyMerging(*this);
		queue.push_back(entry);

		++iterator.index;
		if (iterator.readEntry(*zookeeper, zookeeper_path))
			priority_queue.push(iterator);
	}

	if (count > 0)
		LOG_DEBUG(log, "Pulled " << count << " entries to queue");
}

bool StorageReplicatedMergeTree::shouldExecuteLogEntry(const LogEntry & entry)
{
	if (entry.type == LogEntry::MERGE_PARTS)
	{
		/** Если какая-то из нужных частей сейчас передается или мерджится, подождем окончания этой операции.
		  * Иначе, даже если всех нужных частей для мерджа нет, нужно попытаться сделать мердж.
		  * Если каких-то частей не хватает, вместо мерджа будет попытка скачать кусок.
		  * Такая ситуация возможна, если получение какого-то куска пофейлилось, и его переместили в конец очереди.
		  */
		for (const auto & name : entry.parts_to_merge)
		{
			if (future_parts.count(name))
			{
				LOG_TRACE(log, "Not merging into part " << entry.new_part_name << " because part " << name << " is not ready yet.");
				return false;
			}
		}
	}

	return true;
}

void StorageReplicatedMergeTree::executeLogEntry(const LogEntry & entry)
{
	if (entry.type == LogEntry::GET_PART ||
		entry.type == LogEntry::MERGE_PARTS)
	{
		/// Если у нас уже есть этот кусок или покрывающий его кусок, ничего делать не нужно.
		MergeTreeData::DataPartPtr containing_part = data.getContainingPart(entry.new_part_name);

		/// Даже если кусок есть локально, его (в исключительных случаях) может не быть в zookeeper.
		if (containing_part && zookeeper->exists(replica_path + "/parts/" + containing_part->name))
		{
			if (!(entry.type == LogEntry::GET_PART && entry.source_replica == replica_name))
				LOG_DEBUG(log, "Skipping action for part " + entry.new_part_name + " - part already exists");
			return;
		}
	}

	if (entry.type == LogEntry::GET_PART && entry.source_replica == replica_name)
		LOG_ERROR(log, "Part " << entry.new_part_name << " from own log doesn't exist. This is a bug.");

	bool do_fetch = false;

	if (entry.type == LogEntry::GET_PART)
	{
		do_fetch = true;
	}
	else if (entry.type == LogEntry::MERGE_PARTS)
	{
		MergeTreeData::DataPartsVector parts;
		bool have_all_parts = true;;
		for (const String & name : entry.parts_to_merge)
		{
			MergeTreeData::DataPartPtr part = data.getContainingPart(name);
			if (!part)
			{
				have_all_parts = false;
				break;
			}
			if (part->name != name)
			{
				LOG_ERROR(log, "Log and parts set look inconsistent: " << name << " is covered by " << part->name
					<< " but should be merged into " << entry.new_part_name);
				have_all_parts = false;
				break;
			}
			parts.push_back(part);
		}

		if (!have_all_parts)
		{
			/// Если нет всех нужных кусков, попробуем взять у кого-нибудь уже помердженный кусок.
			do_fetch = true;
			LOG_DEBUG(log, "Don't have all parts for merge " << entry.new_part_name << "; will try to fetch it instead");
		}
		else
		{
			MergeTreeData::DataPartPtr part = merger.mergeParts(parts, entry.new_part_name);

			zkutil::Ops ops;
			checkPartAndAddToZooKeeper(part, ops);

			zookeeper->multi(ops);

			ProfileEvents::increment(ProfileEvents::ReplicatedPartMerges);
		}
	}
	else
	{
		throw Exception("Unexpected log entry type: " + toString(static_cast<int>(entry.type)));
	}

	if (do_fetch)
	{
		try
		{
			String replica = findReplicaHavingPart(entry.new_part_name, true);
			if (replica.empty())
			{
				ProfileEvents::increment(ProfileEvents::ReplicatedPartFailedFetches);
				throw Exception("No active replica has part " + entry.new_part_name, ErrorCodes::NO_REPLICA_HAS_PART);
			}
			fetchPart(entry.new_part_name, replica);

			if (entry.type == LogEntry::MERGE_PARTS)
				ProfileEvents::increment(ProfileEvents::ReplicatedPartFetchesOfMerged);
		}
		catch (...)
		{
			/** Если не получилось скачать кусок, нужный для какого-то мерджа, лучше не пытаться получить другие куски для этого мерджа,
			  * а попытаться сразу получить помердженный кусок. Чтобы так получилось, переместим действия для получения остальных кусков
			  * для этого мерджа в конец очереди.
			  *
			  */
			try
			{
				Poco::ScopedLock<Poco::FastMutex> lock(queue_mutex);

				/// Найдем действие по объединению этого куска с другими. Запомним других.
				StringSet parts_for_merge;
				LogEntries::iterator merge_entry;
				for (LogEntries::iterator it = queue.begin(); it != queue.end(); ++it)
				{
					if (it->type == LogEntry::MERGE_PARTS)
					{
						if (std::find(it->parts_to_merge.begin(), it->parts_to_merge.end(), entry.new_part_name)
							!= it->parts_to_merge.end())
						{
							parts_for_merge = StringSet(it->parts_to_merge.begin(), it->parts_to_merge.end());
							merge_entry = it;
							break;
						}
					}
				}

				if (!parts_for_merge.empty())
				{
					/// Переместим в конец очереди действия, получающие parts_for_merge.
					for (LogEntries::iterator it = queue.begin(); it != queue.end();)
					{
						auto it0 = it;
						++it;

						if (it0 == merge_entry)
							break;

						if ((it0->type == LogEntry::MERGE_PARTS || it0->type == LogEntry::GET_PART)
							&& parts_for_merge.count(it0->new_part_name))
						{
							queue.splice(queue.end(), queue, it0, it);
						}
					}
				}
			}
			catch (...)
			{
				tryLogCurrentException(__PRETTY_FUNCTION__);
			}

			throw;
		}
	}
}

void StorageReplicatedMergeTree::queueUpdatingThread()
{
	while (!shutdown_called)
	{
		try
		{
			pullLogsToQueue();

			clearOldParts();

			/// Каждую минуту выбрасываем ненужные записи из лога.
			if (time(0) - clear_old_logs_time > 60)
			{
				clear_old_logs_time = time(0);
				clearOldLogs();
			}
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}

		std::this_thread::sleep_for(QUEUE_UPDATE_SLEEP);
	}
}

void StorageReplicatedMergeTree::queueThread()
{
	while (!shutdown_called)
	{
		LogEntry entry;
		bool have_work = false;

		try
		{
			Poco::ScopedLock<Poco::FastMutex> lock(queue_mutex);
			bool empty = queue.empty();
			if (!empty)
			{
				for (LogEntries::iterator it = queue.begin(); it != queue.end(); ++it)
				{
					if (shouldExecuteLogEntry(*it))
					{
						entry = *it;
						entry.tagPartAsFuture(*this);
						queue.erase(it);
						have_work = true;
						break;
					}
				}
			}
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}

		if (!have_work)
		{
			std::this_thread::sleep_for(QUEUE_NO_WORK_SLEEP);
			continue;
		}

		bool success = false;

		try
		{
			executeLogEntry(entry);

			auto code = zookeeper->tryRemove(replica_path + "/queue/" + entry.znode_name);
			if (code != zkutil::ReturnCode::Ok)
				LOG_ERROR(log, "Couldn't remove " << replica_path + "/queue/" + entry.znode_name << ": "
					<< zkutil::ReturnCode::toString(code) + ". There must be a bug somewhere. Ignoring it.");

			success = true;
		}
		catch (Exception & e)
		{
			if (e.code() == ErrorCodes::NO_REPLICA_HAS_PART)
				/// Если ни у кого нет нужного куска, это нормальная ситуация; не будем писать в лог с уровнем Error.
				LOG_INFO(log, e.displayText());
			else
				tryLogCurrentException(__PRETTY_FUNCTION__);
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}

		if (shutdown_called)
			break;

		if (success)
		{
			entry.currently_merging_tagger = nullptr;
			std::this_thread::sleep_for(QUEUE_AFTER_WORK_SLEEP);
		}
		else
		{
			{
				/// Добавим действие, которое не получилось выполнить, в конец очереди.
				entry.future_part_tagger = nullptr;
				Poco::ScopedLock<Poco::FastMutex> lock(queue_mutex);
				queue.push_back(entry);
			}
			entry.currently_merging_tagger = nullptr;
			std::this_thread::sleep_for(QUEUE_ERROR_SLEEP);
		}
	}
}

void StorageReplicatedMergeTree::mergeSelectingThread()
{
	pullLogsToQueue();

	while (!shutdown_called && is_leader_node)
	{
		bool success = false;

		try
		{
			size_t merges_queued = 0;

			{
				Poco::ScopedLock<Poco::FastMutex> lock(queue_mutex);

				for (const auto & entry : queue)
					if (entry.type == LogEntry::MERGE_PARTS)
						++merges_queued;
			}

			if (merges_queued >= data.settings.merging_threads)
			{
				std::this_thread::sleep_for(MERGE_SELECTING_SLEEP);
				continue;
			}

			/// Есть ли активный мердж крупных кусков.
			bool has_big_merge = false;

			{
				Poco::ScopedLock<Poco::FastMutex> lock(currently_merging_mutex);

				for (const auto & name : currently_merging)
				{
					MergeTreeData::DataPartPtr part = data.getContainingPart(name);
					if (!part)
						continue;
					if (part->name != name)
					{
						LOG_INFO(log, "currently_merging contains obsolete part " << name << " contained in" << part->name);
						continue;
					}
					if (part->size * data.index_granularity > 25 * 1024 * 1024)
					{
						has_big_merge = true;
						break;
					}
				}
			}

			MergeTreeData::DataPartsVector parts;

			{
				Poco::ScopedLock<Poco::FastMutex> lock(currently_merging_mutex);

				String merged_name;
				auto can_merge = std::bind(
					&StorageReplicatedMergeTree::canMergeParts, this, std::placeholders::_1, std::placeholders::_2);

				if (merger.selectPartsToMerge(parts, merged_name, 0, false, false, has_big_merge, can_merge) ||
					merger.selectPartsToMerge(parts, merged_name, 0,  true, false, has_big_merge, can_merge))
				{
					LogEntry entry;
					entry.type = LogEntry::MERGE_PARTS;
					entry.source_replica = replica_name;
					entry.new_part_name = merged_name;

					for (const auto & part : parts)
					{
						entry.parts_to_merge.push_back(part->name);
					}

					zookeeper->create(replica_path + "/log/log-", entry.toString(), zkutil::CreateMode::PersistentSequential);

					success = true;
				}
			}

			if (success)
			{
				/// Нужно загрузить новую запись в очередь перед тем, как в следующий раз выбирать куски для слияния.
				///  (чтобы куски пометились как currently_merging).
				pullLogsToQueue();

				String month_name = parts[0]->name.substr(0, 6);
				for (size_t i = 0; i + 1 < parts.size(); ++i)
				{
					/// Уберем больше не нужные отметки о несуществующих блоках.
					for (UInt64 number = parts[i]->right + 1; number <= parts[i + 1]->left - 1; ++number)
					{
						String number_str = toString(number);
						while (number_str.size() < 10)
							number_str = '0' + number_str;
						String path = zookeeper_path + "/block_numbers/" + month_name + "/block-" + number_str;

						zookeeper->tryRemove(path);
					}
				}
			}
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}

		if (shutdown_called || !is_leader_node)
			break;

		if (!success)
			std::this_thread::sleep_for(MERGE_SELECTING_SLEEP);
	}
}

void StorageReplicatedMergeTree::clearOldBlocksThread()
{
	while (!shutdown_called && is_leader_node)
	{
		try
		{
			clearOldBlocks();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}

		/// Спим минуту, но проверяем, нужно ли завершиться, каждую секунду.
		/// TODO: Лучше во всех подобных местах использовать condition variable.
		for (size_t i = 0; i < 60; ++i)
		{
			if (shutdown_called || !is_leader_node)
				break;
			std::this_thread::sleep_for(std::chrono::seconds(1));
		}
	}
}

bool StorageReplicatedMergeTree::canMergeParts(const MergeTreeData::DataPartPtr & left, const MergeTreeData::DataPartPtr & right)
{
	if (currently_merging.count(left->name) || currently_merging.count(right->name))
		return false;

	String month_name = left->name.substr(0, 6);

	/// Можно слить куски, если все номера между ними заброшены - не соответствуют никаким блокам.
	for (UInt64 number = left->right + 1; number <= right->left - 1; ++number)
	{
		String number_str = toString(number);
		while (number_str.size() < 10)
			number_str = '0' + number_str;
		String path = zookeeper_path + "/block_numbers/" + month_name + "/block-" + number_str;

		if (AbandonableLockInZooKeeper::check(path, *zookeeper) != AbandonableLockInZooKeeper::ABANDONED)
		{
			LOG_DEBUG(log, "Can't merge parts " << left->name << " and " << right->name << " because block " << path << " exists");
			return false;
		}
	}

	return true;
}

void StorageReplicatedMergeTree::becomeLeader()
{
	LOG_INFO(log, "Became leader");
	is_leader_node = true;
	merge_selecting_thread = std::thread(&StorageReplicatedMergeTree::mergeSelectingThread, this);
	clear_old_blocks_thread = std::thread(&StorageReplicatedMergeTree::clearOldBlocksThread, this);
}

String StorageReplicatedMergeTree::findReplicaHavingPart(const String & part_name, bool active)
{
	Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");

	/// Из реплик, у которых есть кусок, выберем одну равновероятно.
	std::random_shuffle(replicas.begin(), replicas.end());

	for (const String & replica : replicas)
	{
		if (zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/parts/" + part_name) &&
			(!active || zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/is_active")))
			return replica;
	}

	return "";
}

void StorageReplicatedMergeTree::fetchPart(const String & part_name, const String & replica_name)
{
	LOG_DEBUG(log, "Fetching part " << part_name << " from " << replica_name);

	auto table_lock = lockStructure(true);

	String host;
	int port;

	String host_port_str = zookeeper->get(zookeeper_path + "/replicas/" + replica_name + "/host");
	ReadBufferFromString buf(host_port_str);
	assertString("host: ", buf);
	readString(host, buf);
	assertString("\nport: ", buf);
	readText(port, buf);
	assertString("\n", buf);
	assertEOF(buf);

	MergeTreeData::MutableDataPartPtr part = fetcher.fetchPart(part_name, zookeeper_path + "/replicas/" + replica_name, host, port);
	auto removed_parts = data.renameTempPartAndReplace(part);

	zkutil::Ops ops;
	checkPartAndAddToZooKeeper(part, ops);

	zookeeper->multi(ops);

	for (const auto & removed_part : removed_parts)
	{
		LOG_DEBUG(log, "Part " << removed_part->name << " is rendered obsolete by fetching part " << part_name);
		ProfileEvents::increment(ProfileEvents::ObsoleteReplicatedParts);
	}

	ProfileEvents::increment(ProfileEvents::ReplicatedPartFetches);

	LOG_DEBUG(log, "Fetched part");
}

void StorageReplicatedMergeTree::shutdown()
{
	if (permanent_shutdown_called)
		return;
	permanent_shutdown_called = true;
	restarting_thread.join();
}

void StorageReplicatedMergeTree::partialShutdown()
{
	leader_election = nullptr;
	shutdown_called = true;
	replica_is_active_node = nullptr;

	merger.cancelAll();
	if (unreplicated_merger)
		unreplicated_merger->cancelAll();

	LOG_TRACE(log, "Waiting for threads to finish");
	if (is_leader_node)
	{
		is_leader_node = false;
		merge_selecting_thread.join();
		clear_old_blocks_thread.join();
	}
	queue_updating_thread.join();
	for (auto & thread : queue_threads)
		thread.join();
	queue_threads.clear();
	LOG_TRACE(log, "Threads finished");
}

void StorageReplicatedMergeTree::startup()
{
	shutdown_called = false;

	merger.uncancelAll();
	if (unreplicated_merger)
		unreplicated_merger->uncancelAll();

	activateReplica();

	leader_election = new zkutil::LeaderElection(zookeeper_path + "/leader_election", *zookeeper,
		std::bind(&StorageReplicatedMergeTree::becomeLeader, this), replica_name);

	queue_updating_thread = std::thread(&StorageReplicatedMergeTree::queueUpdatingThread, this);
	for (size_t i = 0; i < data.settings.replication_threads; ++i)
		queue_threads.push_back(std::thread(&StorageReplicatedMergeTree::queueThread, this));
}

void StorageReplicatedMergeTree::restartingThread()
{
	try
	{
		startup();

		while (!permanent_shutdown_called)
		{
			if (zookeeper->expired())
			{
				LOG_WARNING(log, "ZooKeeper session has expired. Switching to a new session.");

				/// Запретим писать в таблицу, пока подменяем zookeeper.
				LOG_TRACE(log, "Locking all operations");
				auto structure_lock = lockDataForAlter();
				LOG_TRACE(log, "Locked all operations");

				partialShutdown();

				zookeeper = context.getZooKeeper();

				startup();
			}

			std::this_thread::sleep_for(std::chrono::seconds(2));
		}
	}
	catch (...)
	{
		tryLogCurrentException("StorageReplicatedMergeTree::restartingThread");
		LOG_ERROR(log, "Exception in restartingThread. Calling std::terminate just in case.");
		std::terminate();
		return;
	}

	try
	{
		endpoint_holder = nullptr;
		partialShutdown();
	}
	catch (...)
	{
		tryLogCurrentException("StorageReplicatedMergeTree::restartingThread");
	}
}

StorageReplicatedMergeTree::~StorageReplicatedMergeTree()
{
	try
	{
		shutdown();
	}
	catch(...)
	{
		tryLogCurrentException("~StorageReplicatedMergeTree");
	}
}

BlockInputStreams StorageReplicatedMergeTree::read(
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size,
		unsigned threads)
{
	BlockInputStreams res = reader.read(column_names, query, settings, processed_stage, max_block_size, threads);

	if (unreplicated_reader)
	{
		BlockInputStreams res2 = unreplicated_reader->read(column_names, query, settings, processed_stage, max_block_size, threads);
		res.insert(res.begin(), res2.begin(), res2.end());
	}

	return res;
}

BlockOutputStreamPtr StorageReplicatedMergeTree::write(ASTPtr query)
{
	String insert_id;
	if (ASTInsertQuery * insert = dynamic_cast<ASTInsertQuery *>(&*query))
		insert_id = insert->insert_id;

	return new ReplicatedMergeTreeBlockOutputStream(*this, insert_id);
}

bool StorageReplicatedMergeTree::optimize()
{
	/// Померджим какие-нибудь куски из директории unreplicated. TODO: Мерджить реплицируемые куски тоже.

	if (!unreplicated_data)
		return false;

	unreplicated_data->clearOldParts();

	MergeTreeData::DataPartsVector parts;
	String merged_name;
	auto always_can_merge = [](const MergeTreeData::DataPartPtr &a, const MergeTreeData::DataPartPtr &b) { return true; };
	if (!unreplicated_merger->selectPartsToMerge(parts, merged_name, 0, true, true, false, always_can_merge))
		return false;

	unreplicated_merger->mergeParts(parts, merged_name);
	return true;
}

void StorageReplicatedMergeTree::drop()
{
	shutdown();

	replica_is_active_node = nullptr;
	zookeeper->removeRecursive(replica_path);
	if (zookeeper->getChildren(zookeeper_path + "/replicas").empty())
		zookeeper->removeRecursive(zookeeper_path);
}

void StorageReplicatedMergeTree::LogEntry::writeText(WriteBuffer & out) const
{
	writeString("format version: 1\n", out);
	writeString("source replica: ", out);
	writeString(source_replica, out);
	writeString("\n", out);
	switch (type)
	{
		case GET_PART:
			writeString("get\n", out);
			writeString(new_part_name, out);
			break;
		case MERGE_PARTS:
			writeString("merge\n", out);
			for (const String & s : parts_to_merge)
			{
				writeString(s, out);
				writeString("\n", out);
			}
			writeString("into\n", out);
			writeString(new_part_name, out);
			break;
	}
	writeString("\n", out);
}

void StorageReplicatedMergeTree::LogEntry::readText(ReadBuffer & in)
{
	String type_str;

	assertString("format version: 1\n", in);
	assertString("source replica: ", in);
	readString(source_replica, in);
	assertString("\n", in);
	readString(type_str, in);
	assertString("\n", in);

	if (type_str == "get")
	{
		type = GET_PART;
		readString(new_part_name, in);
	}
	else if (type_str == "merge")
	{
		type = MERGE_PARTS;
		while (true)
		{
			String s;
			readString(s, in);
			assertString("\n", in);
			if (s == "into")
				break;
			parts_to_merge.push_back(s);
		}
		readString(new_part_name, in);
	}
	assertString("\n", in);
}

}
