#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreePartsExchange.h>
#include <DB/Storages/MergeTree/MergeTreePartChecker.h>
#include <DB/Parsers/formatAST.h>
#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <DB/Common/VirtualColumnUtils.h>
#include <time.h>

namespace DB
{


const auto ERROR_SLEEP_MS = 1000;
const auto MERGE_SELECTING_SLEEP_MS = 5 * 1000;
const auto CLEANUP_SLEEP_MS = 30 * 1000;

/// Преобразовать число в строку формате суффиксов автоинкрементных нод в ZooKeeper.
static String padIndex(UInt64 index)
{
	String index_str = toString(index);
	while (index_str.size() < 10)
		index_str = '0' + index_str;
	return index_str;
}


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
	context(context_), zookeeper(context.getZooKeeper()), database_name(database_name_),
	table_name(name_), full_path(path_ + escapeForFileName(table_name) + '/'), zookeeper_path(zookeeper_path_),
	replica_name(replica_name_),
	data(	full_path, columns_, context_, primary_expr_ast_, date_column_name_, sampling_expression_,
			index_granularity_, mode_, sign_column_, settings_, database_name_ + "." + table_name, true,
			std::bind(&StorageReplicatedMergeTree::enqueuePartForCheck, this, std::placeholders::_1)),
	reader(data), writer(data), merger(data), fetcher(data),
	log(&Logger::get(database_name_ + "." + table_name + " (StorageReplicatedMergeTree)")),
	shutdown_event(false), permanent_shutdown_event(false)
{
	if (!zookeeper)
	{
		if (!attach)
			throw Exception("Can't create replicated table without ZooKeeper", ErrorCodes::NO_ZOOKEEPER);

		goReadOnly();
		return;
	}

	if (!zookeeper_path.empty() && *zookeeper_path.rbegin() == '/')
		zookeeper_path.erase(zookeeper_path.end() - 1);
	replica_path = zookeeper_path + "/replicas/" + replica_name;

	if (!attach)
	{
		if (!zookeeper->exists(zookeeper_path))
			createTable();

		checkTableStructure(false);
		createReplica();
	}
	else
	{
		bool skip_sanity_checks = false;
		if (zookeeper->exists(replica_path + "/flags/force_restore_data"))
		{
			skip_sanity_checks = true;
			zookeeper->remove(replica_path + "/flags/force_restore_data");

			LOG_WARNING(log, "Skipping the limits on severity of changes to data parts and columns (flag "
				<< replica_path << "/flags/force_restore_data).");
		}

		checkTableStructure(skip_sanity_checks);
		checkParts(skip_sanity_checks);
	}

	initVirtualParts();
	loadQueue();

	String unreplicated_path = full_path + "unreplicated/";
	if (Poco::File(unreplicated_path).exists())
	{
		LOG_INFO(log, "Have unreplicated data");
		unreplicated_data.reset(new MergeTreeData(unreplicated_path, columns_, context_, primary_expr_ast_,
			date_column_name_, sampling_expression_, index_granularity_, mode_, sign_column_, settings_,
			database_name_ + "." + table_name + "[unreplicated]", false));
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
	if (!res->is_read_only)
	{
		String endpoint_name = "ReplicatedMergeTree:" + res->replica_path;
		InterserverIOEndpointPtr endpoint = new ReplicatedMergeTreePartsServer(res->data, *res);
		res->endpoint_holder = new InterserverIOEndpointHolder(endpoint_name, endpoint, res->context.getInterserverIOHandler());
	}
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
	LOG_DEBUG(log, "Creating table " << zookeeper_path);

	zookeeper->create(zookeeper_path, "", zkutil::CreateMode::Persistent);

	/// Запишем метаданные таблицы, чтобы реплики могли сверять с ними параметры таблицы.
	std::stringstream metadata;
	metadata << "metadata format version: 1" << std::endl;
	metadata << "date column: " << data.date_column_name << std::endl;
	metadata << "sampling expression: " << formattedAST(data.sampling_expression) << std::endl;
	metadata << "index granularity: " << data.index_granularity << std::endl;
	metadata << "mode: " << static_cast<int>(data.mode) << std::endl;
	metadata << "sign column: " << data.sign_column << std::endl;
	metadata << "primary key: " << formattedAST(data.primary_expr_ast) << std::endl;

	zookeeper->create(zookeeper_path + "/metadata", metadata.str(), zkutil::CreateMode::Persistent);
	zookeeper->create(zookeeper_path + "/columns", data.getColumnsList().toString(), zkutil::CreateMode::Persistent);

	zookeeper->create(zookeeper_path + "/log", "", zkutil::CreateMode::Persistent);
	zookeeper->create(zookeeper_path + "/blocks", "", zkutil::CreateMode::Persistent);
	zookeeper->create(zookeeper_path + "/block_numbers", "", zkutil::CreateMode::Persistent);
	zookeeper->create(zookeeper_path + "/nonincrement_block_numbers", "", zkutil::CreateMode::Persistent);
	zookeeper->create(zookeeper_path + "/leader_election", "", zkutil::CreateMode::Persistent);
	zookeeper->create(zookeeper_path + "/temp", "", zkutil::CreateMode::Persistent);
	zookeeper->create(zookeeper_path + "/flags", "", zkutil::CreateMode::Persistent);
	/// Создадим replicas в последнюю очередь, чтобы нельзя было добавить реплику, пока все остальные ноды не созданы.
	zookeeper->create(zookeeper_path + "/replicas", "", zkutil::CreateMode::Persistent);
}

/** Проверить, что список столбцов и настройки таблицы совпадают с указанными в ZK (/metadata).
	* Если нет - бросить исключение.
	*/
void StorageReplicatedMergeTree::checkTableStructure(bool skip_sanity_checks)
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
	/// NOTE: Можно сделать менее строгую проверку совпадения выражений, чтобы таблицы не ломались от небольших изменений
	///       в коде formatAST.
	assertString(formattedAST(data.primary_expr_ast), buf);
	assertString("\n", buf);
	assertEOF(buf);

	zkutil::Stat stat;
	auto columns = NamesAndTypesList::parse(zookeeper->get(zookeeper_path + "/columns", &stat), context.getDataTypeFactory());
	columns_version = stat.version;
	if (columns != data.getColumnsList())
	{
		if (data.getColumnsList().sizeOfDifference(columns) <= 2 || skip_sanity_checks)
		{
			LOG_WARNING(log, "Table structure in ZooKeeper is a little different from local table structure. Assuming ALTER.");

			/// Без всяких блокировок, потому что таблица еще не создана.
			InterpreterAlterQuery::updateMetadata(database_name, table_name, columns, context);
			data.setColumnsList(columns);
		}
		else
		{
			throw Exception("Table structure in ZooKeeper is very different from local table structure.",
							ErrorCodes::INCOMPATIBLE_COLUMNS);
		}
	}
}

void StorageReplicatedMergeTree::createReplica()
{
	LOG_DEBUG(log, "Creating replica " << replica_path);

	/** Запомним список других реплик.
	  * NOTE: Здесь есть race condition. Если почти одновременно добавить нескольких реплик, сразу же начиная в них писать,
	  *       небольшая часть данных может не реплицироваться.
	  */
	Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");

	/// Создадим пустую реплику.
	zookeeper->create(replica_path, "", zkutil::CreateMode::Persistent);
	zookeeper->create(replica_path + "/columns", data.getColumnsList().toString(), zkutil::CreateMode::Persistent);
	zookeeper->create(replica_path + "/host", "", zkutil::CreateMode::Persistent);
	zookeeper->create(replica_path + "/log_pointer", "", zkutil::CreateMode::Persistent);
	zookeeper->create(replica_path + "/queue", "", zkutil::CreateMode::Persistent);
	zookeeper->create(replica_path + "/parts", "", zkutil::CreateMode::Persistent);

	/** Нужно изменить данные ноды /replicas на что угодно, чтобы поток, удаляющий старые записи в логе,
	  *  споткнулся об это изменение и не удалил записи, которые мы еще не прочитали.
	  */
	zookeeper->set(zookeeper_path + "/replicas", "last added replica: " + replica_name);

	if (replicas.empty())
	{
		LOG_DEBUG(log, "No other replicas");
		return;
	}

	/// "Эталонная" реплика, у которой мы возьмем информацию о множестве кусков, очередь и указатель на лог.
	String source_replica = replicas[rand() % replicas.size()];

	LOG_INFO(log, "Will mimic " << source_replica);

	String source_path = zookeeper_path + "/replicas/" + source_replica;

	/// Порядок следующих трех действий важен. Записи в логе могут продублироваться, но не могут потеряться.

	/// Скопируем у эталонной реплики ссылку на лог.
	zookeeper->set(replica_path + "/log_pointer", zookeeper->get(source_path + "/log_pointer"));

	/// Запомним очередь эталонной реплики.
	Strings source_queue_names = zookeeper->getChildren(source_path + "/queue");
	std::sort(source_queue_names.begin(), source_queue_names.end());
	Strings source_queue;
	for (const String & entry_name : source_queue_names)
	{
		String entry;
		if (!zookeeper->tryGet(source_path + "/queue/" + entry_name, entry))
			continue;
		source_queue.push_back(entry);
	}

	/// Добавим в очередь задания на получение всех активных кусков, которые есть у эталонной реплики.
	Strings parts = zookeeper->getChildren(source_path + "/parts");
	ActiveDataPartSet active_parts_set;
	for (const String & part : parts)
	{
		active_parts_set.add(part);
	}
	Strings active_parts = active_parts_set.getParts();
	for (const String & name : active_parts)
	{
		LogEntry log_entry;
		log_entry.type = LogEntry::GET_PART;
		log_entry.source_replica = "";
		log_entry.new_part_name = name;

		zookeeper->create(replica_path + "/queue/queue-", log_entry.toString(), zkutil::CreateMode::PersistentSequential);
	}
	LOG_DEBUG(log, "Queued " << active_parts.size() << " parts to be fetched");

	/// Добавим в очередь содержимое очереди эталонной реплики.
	for (const String & entry : source_queue)
	{
		zookeeper->create(replica_path + "/queue/queue-", entry, zkutil::CreateMode::PersistentSequential);
	}
	LOG_DEBUG(log, "Copied " << source_queue.size() << " queue entries");
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
		if (e.code == ZNODEEXISTS)
			throw Exception("Replica " + replica_path + " appears to be already active. If you're sure it's not, "
				"try again in a minute or remove znode " + replica_path + "/is_active manually", ErrorCodes::REPLICA_IS_ALREADY_ACTIVE);

		throw;
	}

	replica_is_active_node = zkutil::EphemeralNodeHolder::existing(replica_path + "/is_active", *zookeeper);
}

void StorageReplicatedMergeTree::checkParts(bool skip_sanity_checks)
{
	Strings expected_parts_vec = zookeeper->getChildren(replica_path + "/parts");

	/// Куски в ZK.
	NameSet expected_parts(expected_parts_vec.begin(), expected_parts_vec.end());

	MergeTreeData::DataParts parts = data.getAllDataParts();

	/// Локальные куски, которых нет в ZK.
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

	/// Какие локальные куски добавить в ZK.
	MergeTreeData::DataPartsVector parts_to_add;

	/// Какие куски нужно забрать с других реплик.
	Strings parts_to_fetch;

	for (const String & missing_name : expected_parts)
	{
		/// Если локально не хватает какого-то куска, но есть покрывающий его кусок, можно заменить в ZK недостающий покрывающим.
		auto containing = data.getActiveContainingPart(missing_name);
		if (containing)
		{
			LOG_ERROR(log, "Ignoring missing local part " << missing_name << " because part " << containing->name << " exists");
			if (unexpected_parts.count(containing))
			{
				parts_to_add.push_back(containing);
				unexpected_parts.erase(containing);
			}
		}
		else
		{
			parts_to_fetch.push_back(missing_name);
		}
	}

	for (const String & name : parts_to_fetch)
		expected_parts.erase(name);

	String sanity_report =
		"There are " + toString(unexpected_parts.size()) + " unexpected parts, "
					 + toString(parts_to_add.size()) + " unexpectedly merged parts, "
					 + toString(expected_parts.size()) + " missing obsolete parts, "
					 + toString(parts_to_fetch.size()) + " missing parts";
	bool insane =
		parts_to_add.size() > 2 ||
		unexpected_parts.size() > 2 ||
		expected_parts.size() > 20 ||
		parts_to_fetch.size() > 2;

	if (insane && !skip_sanity_checks)
	{
		throw Exception("The local set of parts of table " + getTableName() + " doesn't look like the set of parts in ZooKeeper. "
			+ sanity_report,
			ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS);
	}

	if (insane)
	{
		LOG_WARNING(log, sanity_report);
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
		LOG_ERROR(log, "Removing unexpectedly merged local part from ZooKeeper: " << name);

		zkutil::Ops ops;
		ops.push_back(new zkutil::Op::Remove(replica_path + "/parts/" + name + "/columns", -1));
		ops.push_back(new zkutil::Op::Remove(replica_path + "/parts/" + name + "/checksums", -1));
		ops.push_back(new zkutil::Op::Remove(replica_path + "/parts/" + name, -1));
		zookeeper->multi(ops);
	}

	/// Добавим в очередь задание забрать недостающие куски с других реплик и уберем из ZK информацию, что они у нас есть.
	for (const String & name : parts_to_fetch)
	{
		LOG_ERROR(log, "Removing missing part from ZooKeeper and queueing a fetch: " << name);

		LogEntry log_entry;
		log_entry.type = LogEntry::GET_PART;
		log_entry.source_replica = "";
		log_entry.new_part_name = name;

		/// Полагаемся, что это происходит до загрузки очереди (loadQueue).
		zkutil::Ops ops;
		ops.push_back(new zkutil::Op::Remove(replica_path + "/parts/" + name + "/columns", -1));
		ops.push_back(new zkutil::Op::Remove(replica_path + "/parts/" + name + "/checksums", -1));
		ops.push_back(new zkutil::Op::Remove(replica_path + "/parts/" + name, -1));
		ops.push_back(new zkutil::Op::Create(
			replica_path + "/queue/queue-", log_entry.toString(), zookeeper->getDefaultACL(), zkutil::CreateMode::PersistentSequential));
		zookeeper->multi(ops);
	}

	/// Удалим лишние локальные куски.
	for (MergeTreeData::DataPartPtr part : unexpected_parts)
	{
		LOG_ERROR(log, "Renaming unexpected part " << part->name << " to ignored_" + part->name);
		data.renameAndDetachPart(part, "ignored_", true);
	}
}

void StorageReplicatedMergeTree::initVirtualParts()
{
	auto parts = data.getDataParts();
	for (const auto & part : parts)
	{
		virtual_parts.add(part->name);
	}
}

void StorageReplicatedMergeTree::checkPartAndAddToZooKeeper(MergeTreeData::DataPartPtr part, zkutil::Ops & ops)
{
	check(part->columns);
	int expected_columns_version = columns_version;

	Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
	std::random_shuffle(replicas.begin(), replicas.end());
	String expected_columns_str = part->columns.toString();

	for (const String & replica : replicas)
	{
		zkutil::Stat stat_before, stat_after;
		String columns_str;
		if (!zookeeper->tryGet(zookeeper_path + "/replicas/" + replica + "/parts/" + part->name + "/columns", columns_str, &stat_before))
			continue;
		if (columns_str != expected_columns_str)
		{
			LOG_INFO(log, "Not checking checksums of part " << part->name << " with replica " << replica
				<< " because columns are different");
			continue;
		}
		String checksums_str;
		/// Проверим, что версия ноды со столбцами не изменилась, пока мы читали checksums.
		/// Это гарантирует, что столбцы и чексуммы относятся к одним и тем же данным.
		if (!zookeeper->tryGet(zookeeper_path + "/replicas/" + replica + "/parts/" + part->name + "/checksums", checksums_str) ||
			!zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/parts/" + part->name + "/columns", &stat_after) ||
			stat_before.version != stat_after.version)
		{
			LOG_INFO(log, "Not checking checksums of part " << part->name << " with replica " << replica
				<< " because part changed while we were reading its checksums");
			continue;
		}

		auto checksums = MergeTreeData::DataPart::Checksums::parse(checksums_str);
		checksums.checkEqual(part->checksums, true);
	}

	if (zookeeper->exists(replica_path + "/parts/" + part->name))
	{
		LOG_ERROR(log, "checkPartAndAddToZooKeeper: node " << replica_path + "/parts/" + part->name << " already exists");
		return;
	}

	ops.push_back(new zkutil::Op::Check(
		zookeeper_path + "/columns",
		expected_columns_version));
	ops.push_back(new zkutil::Op::Create(
		replica_path + "/parts/" + part->name,
		"",
		zookeeper->getDefaultACL(),
		zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(
		replica_path + "/parts/" + part->name + "/columns",
		part->columns.toString(),
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
	auto table_lock = lockStructure(false);

	MergeTreeData::DataPartsVector parts = data.grabOldParts();
	size_t count = parts.size();

	if (!count)
		return;

	try
	{
		while (!parts.empty())
		{
			MergeTreeData::DataPartPtr part = parts.back();

			LOG_DEBUG(log, "Removing " << part->name);

			zkutil::Ops ops;
			ops.push_back(new zkutil::Op::Remove(replica_path + "/parts/" + part->name + "/columns", -1));
			ops.push_back(new zkutil::Op::Remove(replica_path + "/parts/" + part->name + "/checksums", -1));
			ops.push_back(new zkutil::Op::Remove(replica_path + "/parts/" + part->name, -1));
			auto code = zookeeper->tryMulti(ops);
			if (code != ZOK)
				LOG_WARNING(log, "Couldn't remove " << part->name << " from ZooKeeper: " << zkutil::ZooKeeper::error2string(code));

			part->remove();
			parts.pop_back();
		}
	}
	catch (...)
	{
		data.addOldParts(parts);
		throw;
	}

	LOG_DEBUG(log, "Removed " << count << " old parts");
}

void StorageReplicatedMergeTree::clearOldLogs()
{
	zkutil::Stat stat;
	if (!zookeeper->exists(zookeeper_path + "/log", &stat))
		throw Exception(zookeeper_path + "/log doesn't exist", ErrorCodes::NOT_FOUND_NODE);

	int children_count = stat.numChildren;

	/// Будем ждать, пока накопятся в 1.1 раза больше записей, чем нужно.
	if (static_cast<double>(children_count) < data.settings.replicated_logs_to_keep * 1.1)
		return;

	Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas", &stat);
	UInt64 min_pointer = std::numeric_limits<UInt64>::max();
	for (const String & replica : replicas)
	{
		String pointer = zookeeper->get(zookeeper_path + "/replicas/" + replica + "/log_pointer");
		if (pointer.empty())
			return;
		min_pointer = std::min(min_pointer, parse<UInt64>(pointer));
	}

	Strings entries = zookeeper->getChildren(zookeeper_path + "/log");
	std::sort(entries.begin(), entries.end());

	/// Не будем трогать последние replicated_logs_to_keep записей.
	entries.erase(entries.end() - std::min(entries.size(), data.settings.replicated_logs_to_keep), entries.end());
	/// Не будем трогать записи, не меньшие min_pointer.
	entries.erase(std::lower_bound(entries.begin(), entries.end(), "log-" + padIndex(min_pointer)), entries.end());

	if (entries.empty())
		return;

	zkutil::Ops ops;
	for (size_t i = 0; i < entries.size(); ++i)
	{
		ops.push_back(new zkutil::Op::Remove(zookeeper_path + "/log/" + entries[i], -1));

		if (ops.size() > 400 || i + 1 == entries.size())
		{
			/// Одновременно с очисткой лога проверим, не добавилась ли реплика с тех пор, как мы получили список реплик.
			ops.push_back(new zkutil::Op::Check(zookeeper_path + "/replicas", stat.version));
			zookeeper->multi(ops);
			ops.clear();
		}
	}

	LOG_DEBUG(log, "Removed " << entries.size() << " old log entries: " << entries.front() << " - " << entries.back());
}

void StorageReplicatedMergeTree::clearOldBlocks()
{
	zkutil::Stat stat;
	if (!zookeeper->exists(zookeeper_path + "/blocks", &stat))
		throw Exception(zookeeper_path + "/blocks doesn't exist", ErrorCodes::NOT_FOUND_NODE);

	int children_count = stat.numChildren;

	/// Чтобы делать "асимптотически" меньше запросов exists, будем ждать, пока накопятся в 1.1 раза больше блоков, чем нужно.
	if (static_cast<double>(children_count) < data.settings.replicated_deduplication_window * 1.1)
		return;

	LOG_TRACE(log, "Clearing about " << static_cast<size_t>(children_count) - data.settings.replicated_deduplication_window
		<< " old blocks from ZooKeeper. This might take several minutes.");

	Strings blocks = zookeeper->getChildren(zookeeper_path + "/blocks");

	std::vector<std::pair<Int64, String> > timed_blocks;

	for (const String & block : blocks)
	{
		zkutil::Stat stat;
		zookeeper->exists(zookeeper_path + "/blocks/" + block, &stat);
		timed_blocks.push_back(std::make_pair(stat.czxid, block));
	}

	zkutil::Ops ops;
	std::sort(timed_blocks.begin(), timed_blocks.end(), std::greater<std::pair<Int64, String>>());
	for (size_t i = data.settings.replicated_deduplication_window; i <  timed_blocks.size(); ++i)
	{
		ops.push_back(new zkutil::Op::Remove(zookeeper_path + "/blocks/" + timed_blocks[i].second + "/number", -1));
		ops.push_back(new zkutil::Op::Remove(zookeeper_path + "/blocks/" + timed_blocks[i].second + "/columns", -1));
		ops.push_back(new zkutil::Op::Remove(zookeeper_path + "/blocks/" + timed_blocks[i].second + "/checksums", -1));
		ops.push_back(new zkutil::Op::Remove(zookeeper_path + "/blocks/" + timed_blocks[i].second, -1));
		if (ops.size() > 400 || i + 1 == timed_blocks.size())
		{
			zookeeper->multi(ops);
			ops.clear();
		}
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
		entry.addResultToVirtualParts(*this);
		queue.push_back(entry);
	}
}

void StorageReplicatedMergeTree::pullLogsToQueue(zkutil::EventPtr next_update_event)
{
	Poco::ScopedLock<Poco::FastMutex> lock(queue_mutex);

	String index_str = zookeeper->get(replica_path + "/log_pointer");
	UInt64 index;

	if (index_str.empty())
	{
		/// Если у нас еще нет указателя на лог, поставим указатель на первую запись в нем.
		Strings entries = zookeeper->getChildren(zookeeper_path + "/log");
		index = entries.empty() ? 0 : parse<UInt64>(std::min_element(entries.begin(), entries.end())->substr(strlen("log-")));

		zookeeper->set(replica_path + "/log_pointer", toString(index));
	}
	else
	{
		index = parse<UInt64>(index_str);
	}

	UInt64 first_index = index;

	size_t count = 0;
	String entry_str;
	while (zookeeper->tryGet(zookeeper_path + "/log/log-" + padIndex(index), entry_str))
	{
		++count;
		++index;

		LogEntry entry = LogEntry::parse(entry_str);

		/// Одновременно добавим запись в очередь и продвинем указатель на лог.
		zkutil::Ops ops;
		ops.push_back(new zkutil::Op::Create(
			replica_path + "/queue/queue-", entry_str, zookeeper->getDefaultACL(), zkutil::CreateMode::PersistentSequential));
		ops.push_back(new zkutil::Op::SetData(
			replica_path + "/log_pointer", toString(index), -1));
		auto results = zookeeper->multi(ops);

		String path_created = dynamic_cast<zkutil::Op::Create &>(ops[0]).getPathCreated();
		entry.znode_name = path_created.substr(path_created.find_last_of('/') + 1);
		entry.addResultToVirtualParts(*this);
		queue.push_back(entry);
	}

	if (next_update_event)
	{
		if (zookeeper->exists(zookeeper_path + "/log/log-" + padIndex(index), nullptr, next_update_event))
			next_update_event->set();
	}

	if (!count)
		return;

	if (queue_task_handle)
		queue_task_handle->wake();

	LOG_DEBUG(log, "Pulled " << count << " entries to queue: log-" << padIndex(first_index) << " - log-" << padIndex(index - 1));
}

bool StorageReplicatedMergeTree::shouldExecuteLogEntry(const LogEntry & entry)
{
	if ((entry.type == LogEntry::MERGE_PARTS || entry.type == LogEntry::GET_PART) && future_parts.count(entry.new_part_name))
	{
		LOG_DEBUG(log, "Not executing log entry for part " << entry.new_part_name <<
			" because another log entry for the same part is being processed. This shouldn't happen often.");
		return false;
	}

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

bool StorageReplicatedMergeTree::executeLogEntry(const LogEntry & entry, BackgroundProcessingPool::Context & pool_context)
{
	if (entry.type == LogEntry::GET_PART ||
		entry.type == LogEntry::MERGE_PARTS)
	{
		/// Если у нас уже есть этот кусок или покрывающий его кусок, ничего делать не нужно.
		MergeTreeData::DataPartPtr containing_part = data.getActiveContainingPart(entry.new_part_name);

		/// Даже если кусок есть локально, его (в исключительных случаях) может не быть в zookeeper.
		if (containing_part && zookeeper->exists(replica_path + "/parts/" + containing_part->name))
		{
			if (!(entry.type == LogEntry::GET_PART && entry.source_replica == replica_name))
				LOG_DEBUG(log, "Skipping action for part " + entry.new_part_name + " - part already exists");
			return true;
		}
	}

	if (entry.type == LogEntry::GET_PART && entry.source_replica == replica_name)
		LOG_WARNING(log, "Part " << entry.new_part_name << " from own log doesn't exist.");

	bool do_fetch = false;

	if (entry.type == LogEntry::GET_PART)
	{
		do_fetch = true;
	}
	else if (entry.type == LogEntry::MERGE_PARTS)
	{
		MergeTreeData::DataPartsVector parts;
		bool have_all_parts = true;
		for (const String & name : entry.parts_to_merge)
		{
			MergeTreeData::DataPartPtr part = data.getActiveContainingPart(name);
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
			/// Если собираемся сливать большие куски, увеличим счетчик потоков, сливающих большие куски.
			for (const auto & part : parts)
			{
				if (part->size_in_bytes > data.settings.max_bytes_to_merge_parts_small)
				{
					pool_context.incrementCounter("big merges");
					pool_context.incrementCounter("replicated big merges");
					break;
				}
			}

			auto table_lock = lockStructure(false);

			MergeTreeData::Transaction transaction;
			MergeTreeData::DataPartPtr part = merger.mergeParts(parts, entry.new_part_name, &transaction);

			zkutil::Ops ops;
			checkPartAndAddToZooKeeper(part, ops);

			zookeeper->multi(ops);

			/** При ZCONNECTIONLOSS или ZOPERATIONTIMEOUT можем зря откатить локальные изменения кусков.
			  * Это не проблема, потому что в таком случае слияние останется в очереди, и мы попробуем снова.
			  */
			transaction.commit();
			merge_selecting_event.set();

			ProfileEvents::increment(ProfileEvents::ReplicatedPartMerges);
		}
	}
	else
	{
		throw Exception("Unexpected log entry type: " + toString(static_cast<int>(entry.type)));
	}

	if (do_fetch)
	{
		String replica;

		try
		{
			replica = findReplicaHavingPart(entry.new_part_name, true);
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

					/** Если этого куска ни у кого нет, но в очереди упоминается мердж с его участием, то наверно этот кусок такой старый,
					  *  что его все померджили и удалили. Не будем бросать исключение, чтобы queueTask лишний раз не спала.
					  */
					if (replica.empty())
					{
						LOG_INFO(log, "No replica has part " << entry.new_part_name << ". Will fetch merged part instead.");
						return false;
					}
				}

				/// Если ни у кого нет куска, и в очереди нет слияний с его участием, проверим, есть ли у кого-то покрывающий его.
				if (replica.empty())
					enqueuePartForCheck(entry.new_part_name);
			}
			catch (...)
			{
				tryLogCurrentException(__PRETTY_FUNCTION__);
			}

			throw;
		}
	}

	return true;
}

void StorageReplicatedMergeTree::queueUpdatingThread()
{
	while (!shutdown_called)
	{
		try
		{
			pullLogsToQueue(queue_updating_event);

			queue_updating_event->wait();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);

			queue_updating_event->tryWait(ERROR_SLEEP_MS);
		}
	}

	LOG_DEBUG(log, "queue updating thread finished");
}

bool StorageReplicatedMergeTree::queueTask(BackgroundProcessingPool::Context & pool_context)
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
		return false;

	bool exception = true;
	bool success = false;

	try
	{
		success = executeLogEntry(entry, pool_context);

		if (success)
		{
			auto code = zookeeper->tryRemove(replica_path + "/queue/" + entry.znode_name);
			if (code != ZOK)
				LOG_ERROR(log, "Couldn't remove " << replica_path + "/queue/" + entry.znode_name << ": "
					<< zkutil::ZooKeeper::error2string(code) + ". There must be a bug somewhere. Ignoring it.");
		}

		exception = false;
	}
	catch (Exception & e)
	{
		if (e.code() == ErrorCodes::NO_REPLICA_HAS_PART)
			/// Если ни у кого нет нужного куска, наверно, просто не все реплики работают; не будем писать в лог с уровнем Error.
			LOG_INFO(log, e.displayText());
		else
			tryLogCurrentException(__PRETTY_FUNCTION__);
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}

	if (!success)
	{
		/// Добавим действие, которое не получилось выполнить, в конец очереди.
		entry.future_part_tagger = nullptr;
		Poco::ScopedLock<Poco::FastMutex> lock(queue_mutex);
		queue.push_back(entry);
	}

	/// Если не было исключения, не нужно спать.
	return !exception;
}

void StorageReplicatedMergeTree::mergeSelectingThread()
{
	bool need_pull = true;

	while (!shutdown_called && is_leader_node)
	{
		bool success = false;

		try
		{
			if (need_pull)
			{
				/// Нужно загрузить новую запись в очередь перед тем, как выбирать куски для слияния.
				///  (чтобы кусок добавился в virtual_parts).
				pullLogsToQueue();
				need_pull = false;
			}

			size_t merges_queued = 0;
			/// Есть ли в очереди или в фоновом потоке мердж крупных кусков.
			bool has_big_merge = context.getBackgroundPool().getCounter("replicated big merges") > 0;

			if (!has_big_merge)
			{
				Poco::ScopedLock<Poco::FastMutex> lock(queue_mutex);

				for (const auto & entry : queue)
				{
					if (entry.type == LogEntry::MERGE_PARTS)
					{
						++merges_queued;

						if (!has_big_merge)
						{
							for (const String & name : entry.parts_to_merge)
							{
								MergeTreeData::DataPartPtr part = data.getActiveContainingPart(name);
								if (!part || part->name != name)
									continue;
								if (part->size_in_bytes > data.settings.max_bytes_to_merge_parts_small)
								{
									has_big_merge = true;
									break;
								}
							}
						}
					}
				}
			}

			do
			{
				if (merges_queued >= data.settings.max_replicated_merges_in_queue)
					break;

				MergeTreeData::DataPartsVector parts;

				String merged_name;
				auto can_merge = std::bind(
					&StorageReplicatedMergeTree::canMergeParts, this, std::placeholders::_1, std::placeholders::_2);

				if (!merger.selectPartsToMerge(parts, merged_name, MergeTreeDataMerger::NO_LIMIT,
												false, false, has_big_merge, can_merge) &&
					!merger.selectPartsToMerge(parts, merged_name, MergeTreeDataMerger::NO_LIMIT,
												true, false, has_big_merge, can_merge))
					break;

				LogEntry entry;
				entry.type = LogEntry::MERGE_PARTS;
				entry.source_replica = replica_name;
				entry.new_part_name = merged_name;

				for (const auto & part : parts)
				{
					entry.parts_to_merge.push_back(part->name);
				}

				need_pull = true;

				zookeeper->create(zookeeper_path + "/log/log-", entry.toString(), zkutil::CreateMode::PersistentSequential);

				String month_name = parts[0]->name.substr(0, 6);
				for (size_t i = 0; i + 1 < parts.size(); ++i)
				{
					/// Уберем больше не нужные отметки о несуществующих блоках.
					for (UInt64 number = parts[i]->right + 1; number <= parts[i + 1]->left - 1; ++number)
					{
						zookeeper->tryRemove(zookeeper_path +              "/block_numbers/" + month_name + "/block-" + padIndex(number));
						zookeeper->tryRemove(zookeeper_path + "/nonincrement_block_numbers/" + month_name + "/block-" + padIndex(number));
					}
				}

				success = true;
			}
			while(false);
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}

		if (shutdown_called || !is_leader_node)
			break;

		if (!success)
			merge_selecting_event.tryWait(MERGE_SELECTING_SLEEP_MS);
	}

	LOG_DEBUG(log, "merge selecting thread finished");
}

void StorageReplicatedMergeTree::cleanupThread()
{
	while (!shutdown_called)
	{
		try
		{
			clearOldParts();

			if (is_leader_node)
			{
				clearOldLogs();
				clearOldBlocks();
			}
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
		}

		shutdown_event.tryWait(CLEANUP_SLEEP_MS);
	}

	LOG_DEBUG(log, "cleanup thread finished");
}

void StorageReplicatedMergeTree::alterThread()
{
	bool force_recheck_parts = true;

	while (!shutdown_called)
	{
		try
		{
			zkutil::Stat stat;
			String columns_str = zookeeper->get(zookeeper_path + "/columns", &stat, alter_thread_event);
			NamesAndTypesList columns = NamesAndTypesList::parse(columns_str, context.getDataTypeFactory());

			bool changed = false;

			/// Проверим, что описание столбцов изменилось.
			/// Чтобы не останавливать лишний раз все запросы в таблицу, проверим сначала под локом на чтение.
			{
				auto table_lock = lockStructure(false);
				if (columns != data.getColumnsList())
					changed = true;
			}

			MergeTreeData::DataParts parts;

			/// Если описание столбцов изменилось, обновим структуру таблицы локально.
			if (changed)
			{
				auto table_lock = lockStructureForAlter();
				if (columns != data.getColumnsList())
				{
					LOG_INFO(log, "Columns list changed in ZooKeeper. Applying changes locally.");
					InterpreterAlterQuery::updateMetadata(database_name, table_name, columns, context);
					data.setColumnsList(columns);
					if (unreplicated_data)
						unreplicated_data->setColumnsList(columns);
					columns_version = stat.version;
					LOG_INFO(log, "Applied changes to table.");

					/// Нужно получить список кусков под блокировкой таблицы, чтобы избежать race condition с мерджем.
					parts = data.getDataParts();
				}
				else
				{
					changed = false;
				}
			}

			/// Обновим куски.
			if (changed || force_recheck_parts)
			{
				if (changed)
					LOG_INFO(log, "ALTER-ing parts");

				int changed_parts = 0;

				if (!changed)
					parts = data.getDataParts();

				auto table_lock = lockStructure(false);

				for (const MergeTreeData::DataPartPtr & part : parts)
				{
					/// Обновим кусок и запишем результат во временные файлы.
					/// TODO: Можно пропускать проверку на слишком большие изменения, если в ZooKeeper есть, например,
					///  нода /flags/force_alter.
					auto transaction = data.alterDataPart(part, columns);

					if (!transaction)
						continue;

					++changed_parts;

					/// Обновим метаданные куска в ZooKeeper.
					zkutil::Ops ops;
					ops.push_back(new zkutil::Op::SetData(replica_path + "/parts/" + part->name + "/columns", part->columns.toString(), -1));
					ops.push_back(new zkutil::Op::SetData(replica_path + "/parts/" + part->name + "/checksums", part->checksums.toString(), -1));
					zookeeper->multi(ops);

					/// Применим изменения файлов.
					transaction->commit();
				}

				/// То же самое для нереплицируемых данных.
				if (unreplicated_data)
				{
					parts = unreplicated_data->getDataParts();

					for (const MergeTreeData::DataPartPtr & part : parts)
					{
						auto transaction = unreplicated_data->alterDataPart(part, columns);

						if (!transaction)
							continue;

						++changed_parts;

						transaction->commit();
					}
				}

				zookeeper->set(replica_path + "/columns", columns.toString());

				if (changed || changed_parts != 0)
					LOG_INFO(log, "ALTER-ed " << changed_parts << " parts");
				force_recheck_parts = false;
			}

			alter_thread_event->wait();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);

			force_recheck_parts = true;

			alter_thread_event->tryWait(ERROR_SLEEP_MS);
		}
	}

	LOG_DEBUG(log, "alter thread finished");
}

void StorageReplicatedMergeTree::removePartAndEnqueueFetch(const String & part_name)
{
	String part_path = replica_path + "/parts/" + part_name;

	LogEntry log_entry;
	log_entry.type = LogEntry::GET_PART;
	log_entry.source_replica = "";
	log_entry.new_part_name = part_name;

	zkutil::Ops ops;
	ops.push_back(new zkutil::Op::Create(
		replica_path + "/queue/queue-", log_entry.toString(), zookeeper->getDefaultACL(),
		zkutil::CreateMode::PersistentSequential));
	ops.push_back(new zkutil::Op::Remove(part_path + "/checksums", -1));
	ops.push_back(new zkutil::Op::Remove(part_path + "/columns", -1));
	ops.push_back(new zkutil::Op::Remove(part_path, -1));
	auto results = zookeeper->multi(ops);

	{
		Poco::ScopedLock<Poco::FastMutex> lock(queue_mutex);

		String path_created = dynamic_cast<zkutil::Op::Create &>(ops[0]).getPathCreated();
		log_entry.znode_name = path_created.substr(path_created.find_last_of('/') + 1);
		log_entry.addResultToVirtualParts(*this);
		queue.push_back(log_entry);
	}
}

void StorageReplicatedMergeTree::enqueuePartForCheck(const String & name)
{
	Poco::ScopedLock<Poco::FastMutex> lock(parts_to_check_mutex);

	if (parts_to_check_set.count(name))
		return;
	parts_to_check_queue.push_back(name);
	parts_to_check_set.insert(name);
	parts_to_check_event.set();
}

void StorageReplicatedMergeTree::partCheckThread()
{
	while (!shutdown_called)
	{
		try
		{
			/// Достанем из очереди кусок для проверки.
			String part_name;
			{
				Poco::ScopedLock<Poco::FastMutex> lock(parts_to_check_mutex);
				if (parts_to_check_queue.empty())
				{
					if (!parts_to_check_set.empty())
					{
						LOG_ERROR(log, "Non-empty parts_to_check_set with empty parts_to_check_queue. This is a bug.");
						parts_to_check_set.clear();
					}
				}
				else
				{
					part_name = parts_to_check_queue.front();
				}
			}
			if (part_name.empty())
			{
				parts_to_check_event.wait();
				continue;
			}

			LOG_WARNING(log, "Checking part " << part_name);
			ProfileEvents::increment(ProfileEvents::ReplicatedPartChecks);

			auto part = data.getActiveContainingPart(part_name);
			String part_path = replica_path + "/parts/" + part_name;

			/// Этого или покрывающего куска у нас нет.
			if (!part)
			{
				/// Если кусок есть в ZooKeeper, удалим его оттуда и добавим в очередь задание скачать его.
				if (zookeeper->exists(part_path))
				{
					LOG_WARNING(log, "Part " << part_name << " exists in ZooKeeper but not locally. "
						"Removing from ZooKeeper and queueing a fetch.");
					ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

					removePartAndEnqueueFetch(part_name);
				}
				/// Если куска нет в ZooKeeper, проверим есть ли он хоть у кого-то.
				else
				{
					ActiveDataPartSet::Part part_info;
					ActiveDataPartSet::parsePartName(part_name, part_info);

					/** Будем проверять только куски, не полученные в результате слияния.
					  * Для кусков, полученных в результате слияния, такая проверка была бы некорректной,
					  *  потому что слитого куска может еще ни у кого не быть.
					  */
					if (part_info.left == part_info.right)
					{
						LOG_WARNING(log, "Checking if anyone has part covering " << part_name << ".");

						bool found = false;
						Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
						for (const String & replica : replicas)
						{
							Strings parts = zookeeper->getChildren(zookeeper_path + "/replicas/" + replica + "/parts");
							for (const String & part_on_replica : parts)
							{
								if (part_on_replica == part_name || ActiveDataPartSet::contains(part_on_replica, part_name))
								{
									found = true;
									LOG_WARNING(log, "Found part " << part_on_replica << " on " << replica);
									break;
								}
							}
							if (found)
								break;
						}

						if (!found)
						{
							LOG_ERROR(log, "No replica has part covering " << part_name);
							ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

							/// Если ни у кого нет такого куска, удалим его из нашей очереди.

							bool was_in_queue = false;

							{
								Poco::ScopedLock<Poco::FastMutex> lock(queue_mutex);

								/** NOTE: Не удалятся записи в очереди, которые сейчас выполняются.
								* Они пофейлятся и положат кусок снова в очередь на проверку.
								* Расчитываем, что это редкая ситуация.
								*/
								for (LogEntries::iterator it = queue.begin(); it != queue.end(); )
								{
									if (it->new_part_name == part_name)
									{
										zookeeper->remove(replica_path + "/queue/" + it->znode_name);
										queue.erase(it++);
										was_in_queue = true;
									}
									else
									{
										++it;
									}
								}
							}

							if (was_in_queue)
							{
								/** Такая ситуация возможна, если на всех репликах, где был кусок, он испортился.
								  * Например, у реплики, которая только что его записала, отключили питание, и данные не записались из кеша на диск.
								  */
								LOG_ERROR(log, "Part " << part_name << " is lost forever. Say goodbye to a piece of data!");

								/** Нужно добавить отсутствующий кусок в block_numbers, чтобы он не мешал слияниям.
								  * Вот только в сам block_numbers мы его добавить не можем - если так сделать,
								  *  ZooKeeper зачем-то пропустит один номер для автоинкремента,
								  *  и в номерах блоков все равно останется дырка.
								  * Специально из-за этого приходится отдельно иметь nonincrement_block_numbers.
								  */
								zookeeper->createIfNotExists(zookeeper_path + "/nonincrement_block_numbers", "");
								zookeeper->createIfNotExists(zookeeper_path + "/nonincrement_block_numbers/" + part_name.substr(0, 6), "");
								AbandonableLockInZooKeeper::createAbandonedIfNotExists(
									zookeeper_path + "/nonincrement_block_numbers/" + part_name.substr(0, 6) + "/block-" + padIndex(part_info.left),
									*zookeeper);
							}
						}
					}
				}
			}
			/// У нас есть этот кусок, и он активен.
			else if (part->name == part_name)
			{
				auto table_lock = lockStructure(false);

				/// Если кусок есть в ZooKeeper, сверим его данные с его чексуммами, а их с ZooKeeper.
				if (zookeeper->exists(replica_path + "/parts/" + part_name))
				{
					LOG_WARNING(log, "Checking data of part " << part_name << ".");

					try
					{
						auto zk_checksums = MergeTreeData::DataPart::Checksums::parse(
							zookeeper->get(replica_path + "/parts/" + part_name + "/checksums"));
						zk_checksums.checkEqual(part->checksums, true);

						auto zk_columns = NamesAndTypesList::parse(
							zookeeper->get(replica_path + "/parts/" + part_name + "/columns"), context.getDataTypeFactory());
						if (part->columns != zk_columns)
							throw Exception("Columns of local part " + part_name + " are different from ZooKeeper");

						MergeTreePartChecker::checkDataPart(
							data.getFullPath() + part_name, data.index_granularity, true, context.getDataTypeFactory());

						LOG_INFO(log, "Part " << part_name << " looks good.");
					}
					catch (...)
					{
						tryLogCurrentException(__PRETTY_FUNCTION__);

						LOG_ERROR(log, "Part " << part_name << " looks broken. Removing it and queueing a fetch.");
						ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

						removePartAndEnqueueFetch(part_name);

						/// Удалим кусок локально.
						data.renameAndDetachPart(part, "broken_");
					}
				}
				/// Если куска нет в ZooKeeper, удалим его локально.
				else
				{
					ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

					LOG_ERROR(log, "Unexpected part " << part_name << ". Removing.");
					data.renameAndDetachPart(part, "unexpected_");
				}
			}
			else
			{
				/// Если у нас есть покрывающий кусок, игнорируем все проблемы с этим куском.
				/// В худшем случае в лог еще old_parts_lifetime секунд будут валиться ошибки, пока кусок не удалится как старый.
			}

			/// Удалим кусок из очереди.
			{
				Poco::ScopedLock<Poco::FastMutex> lock(parts_to_check_mutex);
				if (parts_to_check_queue.empty() || parts_to_check_queue.front() != part_name)
				{
					LOG_ERROR(log, "Someone changed parts_to_check_queue.front(). This is a bug.");
				}
				else
				{
					parts_to_check_queue.pop_front();
					parts_to_check_set.erase(part_name);
				}
			}
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
			parts_to_check_event.tryWait(ERROR_SLEEP_MS);
		}
	}
}


bool StorageReplicatedMergeTree::canMergeParts(const MergeTreeData::DataPartPtr & left, const MergeTreeData::DataPartPtr & right)
{
	/// Если какой-то из кусков уже собираются слить в больший, не соглашаемся его сливать.
	if (virtual_parts.getContainingPart(left->name) != left->name ||
		virtual_parts.getContainingPart(right->name) != right->name)
		return false;

	/// Если о каком-то из кусков нет информации в ZK, не будем сливать.
	if (!zookeeper->exists(replica_path + "/parts/" + left->name) ||
		!zookeeper->exists(replica_path + "/parts/" + right->name))
		return false;

	String month_name = left->name.substr(0, 6);

	/// Можно слить куски, если все номера между ними заброшены - не соответствуют никаким блокам.
	for (UInt64 number = left->right + 1; number <= right->left - 1; ++number)
	{
		String path1 = zookeeper_path +              "/block_numbers/" + month_name + "/block-" + padIndex(number);
		String path2 = zookeeper_path + "/nonincrement_block_numbers/" + month_name + "/block-" + padIndex(number);

		if (AbandonableLockInZooKeeper::check(path1, *zookeeper) != AbandonableLockInZooKeeper::ABANDONED &&
			AbandonableLockInZooKeeper::check(path2, *zookeeper) != AbandonableLockInZooKeeper::ABANDONED)
			return false;
	}

	return true;
}

void StorageReplicatedMergeTree::becomeLeader()
{
	LOG_INFO(log, "Became leader");
	is_leader_node = true;
	merge_selecting_thread = std::thread(&StorageReplicatedMergeTree::mergeSelectingThread, this);
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

	MergeTreeData::Transaction transaction;
	auto removed_parts = data.renameTempPartAndReplace(part, nullptr, &transaction);

	zkutil::Ops ops;
	checkPartAndAddToZooKeeper(part, ops);

	zookeeper->multi(ops);
	transaction.commit();
	merge_selecting_event.set();

	for (const auto & removed_part : removed_parts)
	{
		LOG_DEBUG(log, "Part " << removed_part->name << " is rendered obsolete by fetching part " << part_name);
		ProfileEvents::increment(ProfileEvents::ObsoleteReplicatedParts);
	}

	ProfileEvents::increment(ProfileEvents::ReplicatedPartFetches);

	LOG_DEBUG(log, "Fetched part " << part_name << " from " << replica_name);
}

void StorageReplicatedMergeTree::shutdown()
{
	if (permanent_shutdown_called)
	{
		if (restarting_thread.joinable())
			restarting_thread.join();
		return;
	}

	permanent_shutdown_called = true;
	permanent_shutdown_event.set();
	restarting_thread.join();

	endpoint_holder = nullptr;
}

void StorageReplicatedMergeTree::partialShutdown()
{
	leader_election = nullptr;
	shutdown_called = true;
	shutdown_event.set();
	merge_selecting_event.set();
	queue_updating_event->set();
	alter_thread_event->set();
	alter_query_event->set();
	parts_to_check_event.set();
	replica_is_active_node = nullptr;

	merger.cancelAll();
	if (unreplicated_merger)
		unreplicated_merger->cancelAll();

	LOG_TRACE(log, "Waiting for threads to finish");
	if (is_leader_node)
	{
		is_leader_node = false;
		if (merge_selecting_thread.joinable())
			merge_selecting_thread.join();
	}
	if (queue_updating_thread.joinable())
		queue_updating_thread.join();
	if (cleanup_thread.joinable())
		cleanup_thread.join();
	if (alter_thread.joinable())
		alter_thread.join();
	if (part_check_thread.joinable())
		part_check_thread.join();
	if (queue_task_handle)
		context.getBackgroundPool().removeTask(queue_task_handle);
	queue_task_handle.reset();
	LOG_TRACE(log, "Threads finished");
}

void StorageReplicatedMergeTree::goReadOnly()
{
	LOG_INFO(log, "Going to read-only mode");

	is_read_only = true;
	permanent_shutdown_called = true;
	permanent_shutdown_event.set();

	partialShutdown();
}

void StorageReplicatedMergeTree::startup()
{
	shutdown_called = false;
	shutdown_event.reset();

	merger.uncancelAll();
	if (unreplicated_merger)
		unreplicated_merger->uncancelAll();

	activateReplica();

	leader_election = new zkutil::LeaderElection(zookeeper_path + "/leader_election", *zookeeper,
		std::bind(&StorageReplicatedMergeTree::becomeLeader, this), replica_name);

	queue_updating_thread = std::thread(&StorageReplicatedMergeTree::queueUpdatingThread, this);
	cleanup_thread = std::thread(&StorageReplicatedMergeTree::cleanupThread, this);
	alter_thread = std::thread(&StorageReplicatedMergeTree::alterThread, this);
	part_check_thread = std::thread(&StorageReplicatedMergeTree::partCheckThread, this);
	queue_task_handle = context.getBackgroundPool().addTask(
		std::bind(&StorageReplicatedMergeTree::queueTask, this, std::placeholders::_1));
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
				LOG_TRACE(log, "Locking INSERTs");
				auto structure_lock = lockDataForAlter();
				LOG_TRACE(log, "Locked INSERTs");

				partialShutdown();

				zookeeper = context.getZooKeeper();

				startup();
			}

			permanent_shutdown_event.tryWait(60 * 1000);
		}
	}
	catch (...)
	{
		tryLogCurrentException("StorageReplicatedMergeTree::restartingThread");
		LOG_ERROR(log, "Exception in restartingThread. The storage will be read-only until server restart.");
		goReadOnly();
		LOG_DEBUG(log, "restarting thread finished");
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

	LOG_DEBUG(log, "restarting thread finished");
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
	Names virt_column_names;
	Names real_column_names;
	for (const auto & it : column_names)
		if (it == "_replicated")
			virt_column_names.push_back(it);
		else
			real_column_names.push_back(it);

	Block virtual_columns_block;
	ColumnUInt8 * column = new ColumnUInt8(2);
	ColumnPtr column_ptr = column;
	column->getData()[0] = 0;
	column->getData()[1] = 1;
	virtual_columns_block.insert(ColumnWithNameAndType(column_ptr, new DataTypeUInt8, "_replicated"));

	BlockInputStreamPtr virtual_columns;

	/// Если запрошен хотя бы один виртуальный столбец, пробуем индексировать
	if (!virt_column_names.empty())
		virtual_columns = VirtualColumnUtils::getVirtualColumnsBlocks(query->clone(), virtual_columns_block, context);
	else /// Иначе, считаем допустимыми все возможные значения
		virtual_columns = new OneBlockInputStream(virtual_columns_block);

	std::multiset<UInt8> values = VirtualColumnUtils::extractSingleValueFromBlocks<UInt8>(virtual_columns, "_replicated");

	BlockInputStreams res;

	if (values.count(1))
	{
		res = reader.read(real_column_names, query, settings, processed_stage, max_block_size, threads);

		for (auto & virtual_column : virt_column_names)
		{
			if (virtual_column == "_replicated")
			{
				for (auto & stream : res)
					stream = new AddingConstColumnBlockInputStream<UInt8>(stream, new DataTypeUInt8, 1, "_replicated");
			}
		}
	}

	if (unreplicated_reader && values.count(0))
	{
		BlockInputStreams res2 = unreplicated_reader->read(real_column_names, query, settings, processed_stage, max_block_size, threads);

		for (auto & virtual_column : virt_column_names)
		{
			if (virtual_column == "_replicated")
			{
				for (auto & stream : res2)
					stream = new AddingConstColumnBlockInputStream<UInt8>(stream, new DataTypeUInt8, 0, "_replicated");
			}
		}

		res.insert(res.begin(), res2.begin(), res2.end());
	}

	return res;
}

BlockOutputStreamPtr StorageReplicatedMergeTree::write(ASTPtr query)
{
	if (is_read_only)
		throw Exception("Table is in read only mode", ErrorCodes::TABLE_IS_READ_ONLY);

	String insert_id;
	if (ASTInsertQuery * insert = typeid_cast<ASTInsertQuery *>(&*query))
		insert_id = insert->insert_id;

	return new ReplicatedMergeTreeBlockOutputStream(*this, insert_id);
}

bool StorageReplicatedMergeTree::optimize()
{
	/// Померджим какие-нибудь куски из директории unreplicated.
	/// TODO: Мерджить реплицируемые куски тоже.
	/// TODO: Не давать вызывать это из нескольких потоков сразу: один кусок может принять участие в нескольких несовместимых слияниях.

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

void StorageReplicatedMergeTree::alter(const AlterCommands & params, const String & database_name, const String & table_name, Context & context)
{
	LOG_DEBUG(log, "Doing ALTER");

	NamesAndTypesList new_columns;
	String new_columns_str;
	int new_columns_version;
	zkutil::Stat stat;

	{
		auto table_lock = lockStructureForAlter();

		data.checkAlter(params);

		new_columns = data.getColumnsList();
		params.apply(new_columns);

		new_columns_str = new_columns.toString();

		/// Делаем ALTER.
		zookeeper->set(zookeeper_path + "/columns", new_columns_str, -1, &stat);

		new_columns_version = stat.version;
	}

	LOG_DEBUG(log, "Updated columns in ZooKeeper. Waiting for replicas to apply changes.");

	/// Ждем, пока все реплики обновят данные.

	/// Подпишемся на изменения столбцов, чтобы перестать ждать, если кто-то еще сделает ALTER.
	if (!zookeeper->exists(zookeeper_path + "/columns", &stat, alter_query_event))
		throw Exception(zookeeper_path + "/columns doesn't exist", ErrorCodes::NOT_FOUND_NODE);
	if (stat.version != new_columns_version)
	{
		LOG_WARNING(log, zookeeper_path + "/columns changed before this ALTER finished; "
			"overlapping ALTER-s are fine but use caution with nontransitive changes");
		return;
	}

	Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
	for (const String & replica : replicas)
	{
		LOG_DEBUG(log, "Waiting for " << replica << " to apply changes");

		while (!shutdown_called)
		{
			String replica_columns_str;

			/// Реплику могли успеть удалить.
			if (!zookeeper->tryGet(zookeeper_path + "/replicas/" + replica + "/columns", replica_columns_str, &stat))
			{
				LOG_WARNING(log, replica << " was removed");
				break;
			}

			int replica_columns_version = stat.version;

			if (replica_columns_str == new_columns_str)
				break;

			if (!zookeeper->exists(zookeeper_path + "/columns", &stat))
				throw Exception(zookeeper_path + "/columns doesn't exist", ErrorCodes::NOT_FOUND_NODE);
			if (stat.version != new_columns_version)
			{
				LOG_WARNING(log, zookeeper_path + "/columns changed before ALTER finished; "
					"overlapping ALTER-s are fine but use caution with nontransitive changes");
				return;
			}

			if (!zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/columns", &stat, alter_query_event))
			{
				LOG_WARNING(log, replica << " was removed");
				break;
			}

			if (stat.version != replica_columns_version)
				continue;

			alter_query_event->wait();
		}

		if (shutdown_called)
			break;
	}

	LOG_DEBUG(log, "ALTER finished");
}

void StorageReplicatedMergeTree::drop()
{
	if (!zookeeper)
		throw Exception("Can't drop replicated table without ZooKeeper", ErrorCodes::NO_ZOOKEEPER);

	shutdown();

	LOG_INFO(log, "Removing replica " << replica_path);
	replica_is_active_node = nullptr;
	zookeeper->tryRemoveRecursive(replica_path);

	/// Проверяем, что zookeeper_path существует: его могла удалить другая реплика после выполнения предыдущей строки.
	Strings replicas;
	if (zookeeper->tryGetChildren(zookeeper_path + "/replicas", replicas) == ZOK && replicas.empty())
	{
		LOG_INFO(log, "Removing table " << zookeeper_path << " (this might take several minutes)");
		zookeeper->tryRemoveRecursive(zookeeper_path);
	}

	data.dropAllData();
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
