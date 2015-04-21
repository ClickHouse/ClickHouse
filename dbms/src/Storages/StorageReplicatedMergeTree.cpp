#include <statdaemons/ext/range.hpp>
#include <DB/Storages/ColumnsDescription.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreePartsExchange.h>
#include <DB/Storages/MergeTree/MergeTreePartChecker.h>
#include <DB/Storages/MergeTree/MergeList.h>
#include <DB/Parsers/formatAST.h>
#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <DB/Common/VirtualColumnUtils.h>
#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/DataStreams/AddingConstColumnBlockInputStream.h>
#include <DB/Common/Macros.h>
#include <Poco/DirectoryIterator.h>
#include <time.h>


namespace DB
{


const auto ERROR_SLEEP_MS = 1000;
const auto MERGE_SELECTING_SLEEP_MS = 5 * 1000;

const auto RESERVED_BLOCK_NUMBERS = 200;


StorageReplicatedMergeTree::StorageReplicatedMergeTree(
	const String & zookeeper_path_,
	const String & replica_name_,
	bool attach,
	const String & path_, const String & database_name_, const String & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_,
	const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	MergeTreeData::Mode mode_,
	const String & sign_column_,
	const Names & columns_to_sum_,
	const MergeTreeSettings & settings_)
    : IStorage{materialized_columns_, alias_columns_, column_defaults_}, context(context_),
	current_zookeeper(context.getZooKeeper()), database_name(database_name_),
	table_name(name_), full_path(path_ + escapeForFileName(table_name) + '/'),
	zookeeper_path(context.getMacros().expand(zookeeper_path_)),
	replica_name(context.getMacros().expand(replica_name_)),
	data(full_path, columns_,
		 materialized_columns_, alias_columns_, column_defaults_,
		 context_, primary_expr_ast_, date_column_name_,
		 sampling_expression_, index_granularity_, mode_, sign_column_, columns_to_sum_,
		 settings_, database_name_ + "." + table_name, true,
		 std::bind(&StorageReplicatedMergeTree::enqueuePartForCheck, this, std::placeholders::_1)),
	reader(data), writer(data), merger(data), fetcher(data), shutdown_event(false),
	log(&Logger::get(database_name + "." + table_name + " (StorageReplicatedMergeTree)"))
{
	if (!zookeeper_path.empty() && zookeeper_path.back() == '/')
		zookeeper_path.resize(zookeeper_path.size() - 1);
	replica_path = zookeeper_path + "/replicas/" + replica_name;

	bool skip_sanity_checks = false;

	try
	{
		if (current_zookeeper && current_zookeeper->exists(replica_path + "/flags/force_restore_data"))
		{
			skip_sanity_checks = true;
			current_zookeeper->remove(replica_path + "/flags/force_restore_data");

			LOG_WARNING(log, "Skipping the limits on severity of changes to data parts and columns (flag "
				<< replica_path << "/flags/force_restore_data).");
		}
	}
	catch (const zkutil::KeeperException & e)
	{
		/// Не удалось соединиться с ZK (об этом стало известно при попытке выполнить первую операцию).
		if (e.code == ZCONNECTIONLOSS)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);
			current_zookeeper = nullptr;
		}
		else
			throw;
	}

	data.loadDataParts(skip_sanity_checks);

	if (!current_zookeeper)
	{
		if (!attach)
			throw Exception("Can't create replicated table without ZooKeeper", ErrorCodes::NO_ZOOKEEPER);

		/// Не активируем реплику. Она будет в режиме readonly.
		return;
	}

	if (!attach)
	{
		createTableIfNotExists();

		checkTableStructure(false, false);
		createReplica();
	}
	else
	{
		checkTableStructure(skip_sanity_checks, true);
		checkParts(skip_sanity_checks);
	}

	initVirtualParts();

	String unreplicated_path = full_path + "unreplicated/";
	if (Poco::File(unreplicated_path).exists())
	{
		LOG_INFO(log, "Have unreplicated data");

		unreplicated_data.reset(new MergeTreeData(unreplicated_path, columns_,
			materialized_columns_, alias_columns_, column_defaults_,
			context_, primary_expr_ast_,
			date_column_name_, sampling_expression_, index_granularity_, mode_, sign_column_, columns_to_sum_, settings_,
			database_name_ + "." + table_name + "[unreplicated]", false));

		unreplicated_data->loadDataParts(skip_sanity_checks);

		unreplicated_reader.reset(new MergeTreeDataSelectExecutor(*unreplicated_data));
		unreplicated_merger.reset(new MergeTreeDataMerger(*unreplicated_data));
	}

	loadQueue();

	/// В этом потоке реплика будет активирована.
	restarting_thread.reset(new ReplicatedMergeTreeRestartingThread(*this));
}


StoragePtr StorageReplicatedMergeTree::create(
	const String & zookeeper_path_,
	const String & replica_name_,
	bool attach,
	const String & path_, const String & database_name_, const String & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_,
	const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	MergeTreeData::Mode mode_,
	const String & sign_column_,
	const Names & columns_to_sum_ = Names(),
	const MergeTreeSettings & settings_)
{
	auto res = new StorageReplicatedMergeTree{
		zookeeper_path_, replica_name_, attach,
		path_, database_name_, name_,
		columns_, materialized_columns_, alias_columns_, column_defaults_,
		context_, primary_expr_ast_, date_column_name_,
		sampling_expression_, index_granularity_, mode_,
		sign_column_, columns_to_sum_, settings_};

	StoragePtr res_ptr = res->thisPtr();

	if (res->getZooKeeper())
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


void StorageReplicatedMergeTree::createTableIfNotExists()
{
	auto zookeeper = getZooKeeper();

	if (zookeeper->exists(zookeeper_path))
		return;

	LOG_DEBUG(log, "Creating table " << zookeeper_path);

	zookeeper->createAncestors(zookeeper_path);

	/// Запишем метаданные таблицы, чтобы реплики могли сверять с ними параметры таблицы.
	std::stringstream metadata;
	metadata << "metadata format version: 1" << std::endl;
	metadata << "date column: " << data.date_column_name << std::endl;
	metadata << "sampling expression: " << formattedAST(data.sampling_expression) << std::endl;
	metadata << "index granularity: " << data.index_granularity << std::endl;
	metadata << "mode: " << static_cast<int>(data.mode) << std::endl;
	metadata << "sign column: " << data.sign_column << std::endl;
	metadata << "primary key: " << formattedAST(data.primary_expr_ast) << std::endl;

	zkutil::Ops ops;
	ops.push_back(new zkutil::Op::Create(zookeeper_path, "",
										 zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/metadata", metadata.str(),
										 zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/columns", ColumnsDescription<false>{
				data.getColumnsListNonMaterialized(), data.materialized_columns,
				data.alias_columns, data.column_defaults}.toString(),
										 zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/log", "",
										 zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/blocks", "",
										 zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/block_numbers", "",
										 zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/nonincrement_block_numbers", "",
										 zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/leader_election", "",
										 zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/temp", "",
										 zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/replicas", "",
										 zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));

	auto code = zookeeper->tryMulti(ops);
	if (code != ZOK && code != ZNODEEXISTS)
		throw zkutil::KeeperException(code);
}


/** Проверить, что список столбцов и настройки таблицы совпадают с указанными в ZK (/metadata).
	* Если нет - бросить исключение.
	*/
void StorageReplicatedMergeTree::checkTableStructure(bool skip_sanity_checks, bool allow_alter)
{
	auto zookeeper = getZooKeeper();

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
	auto columns_desc = ColumnsDescription<true>::parse(
		zookeeper->get(zookeeper_path + "/columns", &stat), context.getDataTypeFactory());

	auto & columns = columns_desc.columns;
	auto & materialized_columns = columns_desc.materialized;
	auto & alias_columns = columns_desc.alias;
	auto & column_defaults = columns_desc.defaults;
	columns_version = stat.version;

	if (columns != data.getColumnsListNonMaterialized() ||
		materialized_columns != data.materialized_columns ||
		alias_columns != data.alias_columns ||
		column_defaults != data.column_defaults)
	{
		if (allow_alter &&
			(skip_sanity_checks ||
			 data.getColumnsListNonMaterialized().sizeOfDifference(columns) +
			 data.materialized_columns.sizeOfDifference(materialized_columns) <= 2))
		{
			LOG_WARNING(log, "Table structure in ZooKeeper is a little different from local table structure. Assuming ALTER.");

			/// Без всяких блокировок, потому что таблица еще не создана.
			InterpreterAlterQuery::updateMetadata(database_name, table_name, columns,
				materialized_columns, alias_columns, column_defaults, context);
			data.setColumnsList(columns);
			data.materialized_columns = std::move(materialized_columns);
			data.alias_columns = std::move(alias_columns);
			data.column_defaults = std::move(column_defaults);
		}
		else
		{
			throw Exception("Table structure in ZooKeeper is too different from local table structure.",
							ErrorCodes::INCOMPATIBLE_COLUMNS);
		}
	}
}


void StorageReplicatedMergeTree::createReplica()
{
	auto zookeeper = getZooKeeper();

	LOG_DEBUG(log, "Creating replica " << replica_path);

	/// Создадим пустую реплику. Ноду columns создадим в конце - будем использовать ее в качестве признака, что создание реплики завершено.
	zkutil::Ops ops;
	ops.push_back(new zkutil::Op::Create(replica_path, "", zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(replica_path + "/host", "", zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(replica_path + "/log_pointer", "", zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(replica_path + "/queue", "", zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(replica_path + "/parts", "", zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(replica_path + "/flags", "", zookeeper->getDefaultACL(), zkutil::CreateMode::Persistent));

	try
	{
		zookeeper->multi(ops);
	}
	catch (const zkutil::KeeperException & e)
	{
		if (e.code == ZNODEEXISTS)
			throw Exception("Replica " + replica_path + " is already exist.", ErrorCodes::REPLICA_IS_ALREADY_EXIST);

		throw;
	}

	/** Нужно изменить данные ноды /replicas на что угодно, чтобы поток, удаляющий старые записи в логе,
	  *  споткнулся об это изменение и не удалил записи, которые мы еще не прочитали.
	  */
	zookeeper->set(zookeeper_path + "/replicas", "last added replica: " + replica_name);

	Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");

	/** "Эталонная" реплика, у которой мы возьмем информацию о множестве кусков, очередь и указатель на лог.
	  * Возьмем случайную из реплик, созданных раньше этой.
	  */
	String source_replica;

	Stat stat;
	zookeeper->exists(replica_path, &stat);
	auto my_create_time = stat.czxid;

	std::random_shuffle(replicas.begin(), replicas.end());
	for (const String & replica : replicas)
	{
		if (!zookeeper->exists(zookeeper_path + "/replicas/" + replica, &stat))
			throw Exception("Replica " + zookeeper_path + "/replicas/" + replica + " was removed from right under our feet.",
							ErrorCodes::NO_SUCH_REPLICA);
		if (stat.czxid < my_create_time)
		{
			source_replica = replica;
			break;
		}
	}

	if (source_replica.empty())
	{
		LOG_INFO(log, "This is the first replica");
	}
	else
	{
		LOG_INFO(log, "Will mimic " << source_replica);

		String source_path = zookeeper_path + "/replicas/" + source_replica;

		/** Если эталонная реплика еще не до конца создана, подождем.
		  * NOTE: Если при ее создании что-то пошло не так, можем провисеть тут вечно.
		  *       Можно создавать на время создания эфемерную ноду, чтобы быть уверенным, что реплика создается, а не заброшена.
		  *       То же можно делать и для таблицы. Можно автоматически удалять ноду реплики/таблицы,
		  *        если видно, что она создана не до конца, а создающий ее умер.
		  */
		while (!zookeeper->exists(source_path + "/columns"))
		{
			LOG_INFO(log, "Waiting for replica " << source_path << " to be fully created");

			zkutil::EventPtr event = new Poco::Event;
			if (zookeeper->exists(source_path + "/columns", nullptr, event))
			{
				LOG_WARNING(log, "Oops, a watch has leaked");
				break;
			}

			event->wait();
		}

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
		ActiveDataPartSet active_parts_set(parts);

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

	zookeeper->create(replica_path + "/columns", ColumnsDescription<false>{
			data.getColumnsListNonMaterialized(),
			data.materialized_columns,
			data.alias_columns,
			data.column_defaults
		}.toString(), zkutil::CreateMode::Persistent);
}


void StorageReplicatedMergeTree::checkParts(bool skip_sanity_checks)
{
	auto zookeeper = getZooKeeper();

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
			LOG_ERROR(log, "Fetching missing part " << missing_name);
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

	/** Можно автоматически синхронизировать данные,
	  * если количество ошибок каждого из четырёх типов не больше соответствующих порогов,
	  * или если отношение общего количества ошибок к общему количеству кусков (минимальному - в локальной файловой системе или в ZK)
	  *  не больше некоторого отношения (например 5%).
	  */

	size_t min_parts_local_or_expected = std::min(expected_parts_vec.size(), parts.size());

	bool insane =
		(parts_to_add.size() > data.settings.replicated_max_unexpectedly_merged_parts
			|| unexpected_parts.size() > data.settings.replicated_max_unexpected_parts
			|| expected_parts.size() > data.settings.replicated_max_missing_obsolete_parts
			|| parts_to_fetch.size() > data.settings.replicated_max_missing_active_parts)
		&& ((parts_to_add.size() + unexpected_parts.size() + expected_parts.size() + parts_to_fetch.size())
			> min_parts_local_or_expected * data.settings.replicated_max_ratio_of_wrong_parts);

	if (insane)
	{
		if (skip_sanity_checks)
			LOG_WARNING(log, sanity_report);
		else
			throw Exception("The local set of parts of table " + getTableName() + " doesn't look like the set of parts in ZooKeeper. "
				+ sanity_report, ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS);
	}

	/// Добавим в ZK информацию о кусках, покрывающих недостающие куски.
	for (const MergeTreeData::DataPartPtr & part : parts_to_add)
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
	for (const MergeTreeData::DataPartPtr & part : unexpected_parts)
	{
		LOG_ERROR(log, "Renaming unexpected part " << part->name << " to ignored_" + part->name);
		data.renameAndDetachPart(part, "ignored_", true);
	}
}


void StorageReplicatedMergeTree::initVirtualParts()
{
	auto parts = data.getDataParts();
	for (const auto & part : parts)
		virtual_parts.add(part->name);
}


void StorageReplicatedMergeTree::checkPartAndAddToZooKeeper(const MergeTreeData::DataPartPtr & part, zkutil::Ops & ops, String part_name)
{
	auto zookeeper = getZooKeeper();

	if (part_name.empty())
		part_name = part->name;

	check(part->columns);
	int expected_columns_version = columns_version;

	Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
	std::random_shuffle(replicas.begin(), replicas.end());
	String expected_columns_str = part->columns.toString();

	for (const String & replica : replicas)
	{
		zkutil::Stat stat_before, stat_after;
		String columns_str;
		if (!zookeeper->tryGet(zookeeper_path + "/replicas/" + replica + "/parts/" + part_name + "/columns", columns_str, &stat_before))
			continue;
		if (columns_str != expected_columns_str)
		{
			LOG_INFO(log, "Not checking checksums of part " << part_name << " with replica " << replica
				<< " because columns are different");
			continue;
		}
		String checksums_str;
		/// Проверим, что версия ноды со столбцами не изменилась, пока мы читали checksums.
		/// Это гарантирует, что столбцы и чексуммы относятся к одним и тем же данным.
		if (!zookeeper->tryGet(zookeeper_path + "/replicas/" + replica + "/parts/" + part_name + "/checksums", checksums_str) ||
			!zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/parts/" + part_name + "/columns", &stat_after) ||
			stat_before.version != stat_after.version)
		{
			LOG_INFO(log, "Not checking checksums of part " << part_name << " with replica " << replica
				<< " because part changed while we were reading its checksums");
			continue;
		}

		auto checksums = MergeTreeData::DataPart::Checksums::parse(checksums_str);
		checksums.checkEqual(part->checksums, true);
	}

	if (zookeeper->exists(replica_path + "/parts/" + part_name))
	{
		LOG_ERROR(log, "checkPartAndAddToZooKeeper: node " << replica_path + "/parts/" + part_name << " already exists");
		return;
	}

	ops.push_back(new zkutil::Op::Check(
		zookeeper_path + "/columns",
		expected_columns_version));
	ops.push_back(new zkutil::Op::Create(
		replica_path + "/parts/" + part_name,
		"",
		zookeeper->getDefaultACL(),
		zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(
		replica_path + "/parts/" + part_name + "/columns",
		part->columns.toString(),
		zookeeper->getDefaultACL(),
		zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(
		replica_path + "/parts/" + part_name + "/checksums",
		part->checksums.toString(),
		zookeeper->getDefaultACL(),
		zkutil::CreateMode::Persistent));
}


void StorageReplicatedMergeTree::loadQueue()
{
	auto zookeeper = getZooKeeper();

	std::lock_guard<std::mutex> lock(queue_mutex);

	Strings children = zookeeper->getChildren(replica_path + "/queue");
	std::sort(children.begin(), children.end());
	for (const String & child : children)
	{
		zkutil::Stat stat;
		String s = zookeeper->get(replica_path + "/queue/" + child, &stat);
		LogEntryPtr entry = LogEntry::parse(s);
		entry->create_time = stat.ctime / 1000;
		entry->znode_name = child;
		entry->addResultToVirtualParts(*this);
		queue.push_back(entry);
	}
}


void StorageReplicatedMergeTree::pullLogsToQueue(zkutil::EventPtr next_update_event)
{
	auto zookeeper = getZooKeeper();

	std::lock_guard<std::mutex> lock(queue_mutex);

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
	zkutil::Stat stat;
	while (zookeeper->tryGet(zookeeper_path + "/log/log-" + padIndex(index), entry_str, &stat))
	{
		++count;
		++index;

		LogEntryPtr entry = LogEntry::parse(entry_str);
		entry->create_time = stat.ctime / 1000;

		/// Одновременно добавим запись в очередь и продвинем указатель на лог.
		zkutil::Ops ops;
		ops.push_back(new zkutil::Op::Create(
			replica_path + "/queue/queue-", entry_str, zookeeper->getDefaultACL(), zkutil::CreateMode::PersistentSequential));
		ops.push_back(new zkutil::Op::SetData(
			replica_path + "/log_pointer", toString(index), -1));
		auto results = zookeeper->multi(ops);

		String path_created = dynamic_cast<zkutil::Op::Create &>(ops[0]).getPathCreated();
		entry->znode_name = path_created.substr(path_created.find_last_of('/') + 1);
		entry->addResultToVirtualParts(*this);
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
	if ((entry.type == LogEntry::MERGE_PARTS || entry.type == LogEntry::GET_PART || entry.type == LogEntry::ATTACH_PART)
		&& future_parts.count(entry.new_part_name))
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
	auto zookeeper = getZooKeeper();

	if (entry.type == LogEntry::DROP_RANGE)
	{
		executeDropRange(entry);
		return true;
	}

	if (entry.type == LogEntry::GET_PART ||
		entry.type == LogEntry::MERGE_PARTS ||
		entry.type == LogEntry::ATTACH_PART)
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
	else if (entry.type == LogEntry::ATTACH_PART)
	{
		do_fetch = !executeAttachPart(entry);
	}
	else if (entry.type == LogEntry::MERGE_PARTS)
	{
		std::stringstream log_message;
		log_message << "Executing log entry to merge parts ";
		for (auto i : ext::range(0, entry.parts_to_merge.size()))
			log_message << (i != 0 ? ", " : "") << entry.parts_to_merge[i];
		log_message << " to " << entry.new_part_name;

		LOG_TRACE(log, log_message.rdbuf());

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
				LOG_WARNING(log, "Part " << name << " is covered by " << part->name
					<< " but should be merged into " << entry.new_part_name << ". This shouldn't happen often.");
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

			const auto & merge_entry = context.getMergeList().insert(database_name, table_name, entry.new_part_name);
			MergeTreeData::Transaction transaction;
			size_t aio_threshold = context.getSettings().min_bytes_to_use_direct_io;
			MergeTreeData::DataPartPtr part = merger.mergeParts(parts, entry.new_part_name, *merge_entry, aio_threshold, &transaction);

			zkutil::Ops ops;
			checkPartAndAddToZooKeeper(part, ops);

			/** TODO: Переименование нового куска лучше делать здесь, а не пятью строчками выше,
			  *  чтобы оно было как можно ближе к zookeeper->multi.
			  */

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

			if (replica.empty() && entry.type == LogEntry::ATTACH_PART)
			{
				/** Если ATTACH - куска может не быть, потому что реплика, на которой кусок есть, ещё сама не успела его прицепить.
				  * В таком случае, надо подождать этого.
				  */

				/// Кусок должен быть на реплике-инициаторе.
				if (entry.source_replica.empty() || entry.source_replica == replica_name)
					throw Exception("Logical error: no source replica specified for ATTACH_PART log entry;"
						" or trying to fetch part on source replica", ErrorCodes::LOGICAL_ERROR);

				/// Подождём, пока реплика-инициатор подцепит кусок.
				waitForReplicaToProcessLogEntry(entry.source_replica, entry);
				replica = findReplicaHavingPart(entry.new_part_name, true);
			}

			if (replica.empty())
			{
				ProfileEvents::increment(ProfileEvents::ReplicatedPartFailedFetches);
				throw Exception("No active replica has part " + entry.new_part_name, ErrorCodes::NO_REPLICA_HAS_PART);
			}

			fetchPart(entry.new_part_name, zookeeper_path + "/replicas/" + replica);

			if (entry.type == LogEntry::MERGE_PARTS)
				ProfileEvents::increment(ProfileEvents::ReplicatedPartFetchesOfMerged);
		}
		catch (...)
		{
			/** Если не получилось скачать кусок, нужный для какого-то мерджа, лучше не пытаться получить другие куски для этого мерджа,
			  * а попытаться сразу получить помердженный кусок. Чтобы так получилось, переместим действия для получения остальных кусков
			  * для этого мерджа в конец очереди.
			  */
			try
			{
				std::lock_guard<std::mutex> lock(queue_mutex);

				/// Найдем действие по объединению этого куска с другими. Запомним других.
				StringSet parts_for_merge;
				LogEntries::iterator merge_entry;
				for (LogEntries::iterator it = queue.begin(); it != queue.end(); ++it)
				{
					if ((*it)->type == LogEntry::MERGE_PARTS)
					{
						if (std::find((*it)->parts_to_merge.begin(), (*it)->parts_to_merge.end(), entry.new_part_name)
							!= (*it)->parts_to_merge.end())
						{
							parts_for_merge = StringSet((*it)->parts_to_merge.begin(), (*it)->parts_to_merge.end());
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

						if (((*it0)->type == LogEntry::MERGE_PARTS || (*it0)->type == LogEntry::GET_PART)
							&& parts_for_merge.count((*it0)->new_part_name))
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


void StorageReplicatedMergeTree::executeDropRange(const StorageReplicatedMergeTree::LogEntry & entry)
{
	auto zookeeper = getZooKeeper();

	LOG_INFO(log, (entry.detach ? "Detaching" : "Removing") << " parts inside " << entry.new_part_name << ".");

	{
		LogEntries to_wait;
		size_t removed_entries = 0;

		/// Удалим из очереди операции с кусками, содержащимися в удаляемом диапазоне.
		std::unique_lock<std::mutex> lock(queue_mutex);
		for (LogEntries::iterator it = queue.begin(); it != queue.end();)
		{
			if (((*it)->type == LogEntry::GET_PART || (*it)->type == LogEntry::MERGE_PARTS) &&
				ActiveDataPartSet::contains(entry.new_part_name, (*it)->new_part_name))
			{
				if ((*it)->currently_executing)
					to_wait.push_back(*it);
				auto code = zookeeper->tryRemove(replica_path + "/queue/" + (*it)->znode_name);
				if (code != ZOK)
					LOG_INFO(log, "Couldn't remove " << replica_path + "/queue/" + (*it)->znode_name << ": "
						<< zkutil::ZooKeeper::error2string(code));
				queue.erase(it++);
				++removed_entries;
			}
			else
				++it;
		}

		LOG_DEBUG(log, "Removed " << removed_entries << " entries from queue. "
			"Waiting for " << to_wait.size() << " entries that are currently executing.");

		/// Дождемся завершения операций с кусками, содержащимися в удаляемом диапазоне.
		for (LogEntryPtr & entry : to_wait)
			entry->execution_complete.wait(lock, [&entry] { return !entry->currently_executing; });
	}

	LOG_DEBUG(log, (entry.detach ? "Detaching" : "Removing") << " parts.");
	size_t removed_parts = 0;

	/// Удалим куски, содержащиеся в удаляемом диапазоне.
	auto parts = data.getDataParts();
	for (const auto & part : parts)
	{
		if (!ActiveDataPartSet::contains(entry.new_part_name, part->name))
			continue;
		LOG_DEBUG(log, "Removing part " << part->name);
		++removed_parts;

		/// Если кусок удалять не нужно, надежнее переместить директорию до изменений в ZooKeeper.
		if (entry.detach)
			data.renameAndDetachPart(part);

		zkutil::Ops ops;
		ops.push_back(new zkutil::Op::Remove(replica_path + "/parts/" + part->name + "/columns", -1));
		ops.push_back(new zkutil::Op::Remove(replica_path + "/parts/" + part->name + "/checksums", -1));
		ops.push_back(new zkutil::Op::Remove(replica_path + "/parts/" + part->name, -1));
		zookeeper->multi(ops);

		/// Если кусок нужно удалить, надежнее удалить директорию после изменений в ZooKeeper.
		if (!entry.detach)
			data.replaceParts({part}, {}, true);
	}

	LOG_INFO(log, (entry.detach ? "Detached " : "Removed ") << removed_parts << " parts inside " << entry.new_part_name << ".");
}


bool StorageReplicatedMergeTree::executeAttachPart(const StorageReplicatedMergeTree::LogEntry & entry)
{
	auto zookeeper = getZooKeeper();

	String source_path = (entry.attach_unreplicated ? "unreplicated/" : "detached/") + entry.source_part_name;

	LOG_INFO(log, "Attaching part " << entry.source_part_name << " from " << source_path << " as " << entry.new_part_name);

	if (!Poco::File(data.getFullPath() + source_path).exists())
	{
		LOG_INFO(log, "No part at " << source_path << ". Will fetch it instead");
		return false;
	}

	LOG_DEBUG(log, "Checking data");
	MergeTreeData::MutableDataPartPtr part = data.loadPartAndFixMetadata(source_path);

	zkutil::Ops ops;
	checkPartAndAddToZooKeeper(part, ops, entry.new_part_name);

	if (entry.attach_unreplicated && unreplicated_data)
	{
		MergeTreeData::DataPartPtr unreplicated_part = unreplicated_data->getPartIfExists(entry.source_part_name);
		if (unreplicated_part)
			unreplicated_data->detachPartInPlace(unreplicated_part);
		else
			LOG_WARNING(log, "Unreplicated part " << entry.source_part_name << " is already detached");
	}

	zookeeper->multi(ops);

	/// NOTE: Не можем использовать renameTempPartAndAdd, потому что кусок не временный - если что-то пойдет не так, его не нужно удалять.
	part->renameTo(entry.new_part_name);
	part->name = entry.new_part_name;
	ActiveDataPartSet::parsePartName(part->name, *part);

	data.attachPart(part);

	LOG_INFO(log, "Finished attaching part " << entry.new_part_name);

	/// На месте удаленных кусков могут появиться новые, с другими данными.
	context.resetCaches();

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
		catch (const zkutil::KeeperException & e)
		{
			if (e.code == ZINVALIDSTATE)
				restarting_thread->wakeup();

			tryLogCurrentException(__PRETTY_FUNCTION__);

			queue_updating_event->tryWait(ERROR_SLEEP_MS);
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);

			queue_updating_event->tryWait(ERROR_SLEEP_MS);
		}
	}

	LOG_DEBUG(log, "Queue updating thread finished");
}


bool StorageReplicatedMergeTree::queueTask(BackgroundProcessingPool::Context & pool_context)
{
	LogEntryPtr entry;

	try
	{
		std::lock_guard<std::mutex> lock(queue_mutex);
		bool empty = queue.empty();
		if (!empty)
		{
			for (LogEntries::iterator it = queue.begin(); it != queue.end(); ++it)
			{
				if (!(*it)->currently_executing && shouldExecuteLogEntry(**it))
				{
					entry = *it;
					entry->tagPartAsFuture(*this);
					queue.splice(queue.end(), queue, it);
					entry->currently_executing = true;
					break;
				}
			}
		}
	}
	catch (...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}

	if (!entry)
		return false;

	bool exception = true;
	bool success = false;

	try
	{
		if (executeLogEntry(*entry, pool_context))
		{
			auto zookeeper = getZooKeeper();
			auto code = zookeeper->tryRemove(replica_path + "/queue/" + entry->znode_name);

			if (code != ZOK)
				LOG_ERROR(log, "Couldn't remove " << replica_path + "/queue/" + entry->znode_name << ": "
					<< zkutil::ZooKeeper::error2string(code) + ". This shouldn't happen often.");

			success = true;
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

	entry->future_part_tagger = nullptr;

	std::lock_guard<std::mutex> lock(queue_mutex);

	entry->currently_executing = false;
	entry->execution_complete.notify_all();

	if (success)
	{
		/// Удалим задание из очереди.
		/// Нельзя просто обратиться по заранее сохраненному итератору, потому что задание мог успеть удалить кто-то другой.
		for (LogEntries::iterator it = queue.end(); it != queue.begin();)
		{
			--it;
			if (*it == entry)
			{
				queue.erase(it);
				break;
			}
		}
	}

	/// Если не было исключения, не нужно спать.
	return !exception;
}


void StorageReplicatedMergeTree::mergeSelectingThread()
{
	bool need_pull = true;

	/** Может много времени тратиться на определение, можно ли мерджить два рядом стоящих куска.
	  * Два рядом стоящих куска можно мерджить, если все номера блоков между их номерами не используются ("заброшены", abandoned).
	  * Это значит, что между этими кусками не может быть вставлен другой кусок.
	  *
	  * Но если номера соседних блоков отличаются достаточно сильно (обычно, если между ними много "заброшенных" блоков),
	  *  то делается слишком много чтений из ZooKeeper, чтобы узнать, можно ли их мерджить.
	  *
	  * Воспользуемся утверждением, что если пару кусков было можно мерджить, и их мердж ещё не запланирован,
	  *  то и сейчас их можно мерджить, и будем запоминать это состояние, чтобы не делать много раз одинаковые запросы в ZooKeeper.
	  *
	  * TODO Интересно, как это сочетается с DROP PARTITION и затем ATTACH PARTITION.
	  */
	std::set<std::pair<std::string, std::string>> memoized_parts_that_could_be_merged;

	auto can_merge = [&memoized_parts_that_could_be_merged, this]
		(const MergeTreeData::DataPartPtr & left, const MergeTreeData::DataPartPtr & right) -> bool
	{
		/// Если какой-то из кусков уже собираются слить в больший, не соглашаемся его сливать.
		if (virtual_parts.getContainingPart(left->name) != left->name ||
			virtual_parts.getContainingPart(right->name) != right->name)
			return false;

		auto key = std::make_pair(left->name, right->name);
		if (memoized_parts_that_could_be_merged.count(key))
			return true;

		String month_name = left->name.substr(0, 6);
		auto zookeeper = getZooKeeper();

		/// Можно слить куски, если все номера между ними заброшены - не соответствуют никаким блокам.
		for (UInt64 number = left->right + 1; number <= right->left - 1; ++number)	/// Номера блоков больше нуля.
		{
			String path1 = zookeeper_path +              "/block_numbers/" + month_name + "/block-" + padIndex(number);
			String path2 = zookeeper_path + "/nonincrement_block_numbers/" + month_name + "/block-" + padIndex(number);

			if (AbandonableLockInZooKeeper::check(path1, *zookeeper) != AbandonableLockInZooKeeper::ABANDONED &&
				AbandonableLockInZooKeeper::check(path2, *zookeeper) != AbandonableLockInZooKeeper::ABANDONED)
				return false;
		}

		memoized_parts_that_could_be_merged.insert(key);
		return true;
	};

	while (!shutdown_called && is_leader_node)
	{
		bool success = false;

		try
		{
			std::lock_guard<std::mutex> merge_selecting_lock(merge_selecting_mutex);

			if (need_pull)
			{
				/// Нужно загрузить новую запись в очередь перед тем, как выбирать куски для слияния.
				///  (чтобы кусок добавился в virtual_parts).
				pullLogsToQueue();
				need_pull = false;
			}

			/** Сколько в очереди или в фоновом потоке мерджей крупных кусков.
			  * Если их больше половины от размера пула потоков для мерджа, то можно мерджить только мелкие куски.
			  */
			auto & background_pool = context.getBackgroundPool();

			size_t big_merges_current = background_pool.getCounter("replicated big merges");
			size_t max_number_of_big_merges = background_pool.getNumberOfThreads() / 2;
			size_t merges_queued = 0;
			size_t big_merges_queued = 0;

			if (big_merges_current < max_number_of_big_merges)
			{
				std::lock_guard<std::mutex> lock(queue_mutex);

				for (const auto & entry : queue)
				{
					if (entry->type == LogEntry::MERGE_PARTS)
					{
						++merges_queued;

						if (big_merges_current + big_merges_queued < max_number_of_big_merges)
						{
							for (const String & name : entry->parts_to_merge)
							{
								MergeTreeData::DataPartPtr part = data.getActiveContainingPart(name);
								if (!part || part->name != name)
									continue;

								if (part->size_in_bytes > data.settings.max_bytes_to_merge_parts_small)
								{
									++big_merges_queued;
									break;
								}
							}
						}
					}
				}
			}

			bool only_small = big_merges_current + big_merges_queued >= max_number_of_big_merges;

			if (big_merges_current || merges_queued)
				LOG_TRACE(log, "Currently executing big merges: " << big_merges_current
					<< ". Queued big merges: " << big_merges_queued
					<< ". All merges in queue: " << merges_queued
					<< ". Max number of big merges: " << max_number_of_big_merges
					<< (only_small ? ". So, will select only small parts to merge." : "."));

			do
			{
				auto zookeeper = getZooKeeper();

				if (merges_queued >= data.settings.max_replicated_merges_in_queue)
				{
					LOG_TRACE(log, "Number of queued merges is greater than max_replicated_merges_in_queue, so won't select new parts to merge.");
					break;
				}

				MergeTreeData::DataPartsVector parts;

				String merged_name;

				if (   !merger.selectPartsToMerge(parts, merged_name, MergeTreeDataMerger::NO_LIMIT, false, false, only_small, can_merge)
					&& !merger.selectPartsToMerge(parts, merged_name, MergeTreeDataMerger::NO_LIMIT, true, false, only_small, can_merge))
				{
					break;
				}

				bool all_in_zk = true;
				for (const auto & part : parts)
				{
					/// Если о каком-то из кусков нет информации в ZK, не будем сливать.
					if (!zookeeper->exists(replica_path + "/parts/" + part->name))
					{
						LOG_WARNING(log, "Part " << part->name << " exists locally but not in ZooKeeper.");
						enqueuePartForCheck(part->name);
						all_in_zk = false;
					}
				}
				if (!all_in_zk)
					break;

				LogEntry entry;
				entry.type = LogEntry::MERGE_PARTS;
				entry.source_replica = replica_name;
				entry.new_part_name = merged_name;

				for (const auto & part : parts)
					entry.parts_to_merge.push_back(part->name);

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
			while (false);
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

	LOG_DEBUG(log, "Merge selecting thread finished");
}


void StorageReplicatedMergeTree::alterThread()
{
	bool force_recheck_parts = true;

	while (!shutdown_called)
	{
		try
		{
			/** Имеем описание столбцов в ZooKeeper, общее для всех реплик (Пример: /clickhouse/tables/02-06/visits/columns),
			  *  а также описание столбцов в локальном файле с метаданными (data.getColumnsList()).
			  *
			  * Если эти описания отличаются - нужно сделать ALTER.
			  *
			  * Если запомненная версия ноды (columns_version) отличается от версии в ZK,
			  *  то описание столбцов в ZK не обязательно отличается от локального
			  *  - такое может быть при цикле из ALTER-ов, который в целом, ничего не меняет.
			  * В этом случае, надо обновить запомненный номер версии,
			  *  а также всё-равно проверить структуру кусков, и, при необходимости, сделать ALTER.
			  *
			  * Запомненный номер версии нужно обновить после обновления метаданных, под блокировкой.
			  * Этот номер версии проверяется на соответствие актуальному при INSERT-е.
			  * То есть, так добиваемся, чтобы вставлялись блоки с правильной структурой.
			  *
			  * При старте сервера, мог быть не завершён предыдущий ALTER.
			  * Поэтому, в первый раз, независимо от изменений, проверяем структуру всех part-ов,
			  *  (Пример: /clickhouse/tables/02-06/visits/replicas/example02-06-1.yandex.ru/parts/20140806_20140831_131664_134988_3296/columns)
			  *  и делаем ALTER, если необходимо.
			  *
			  * TODO: Слишком сложно, всё переделать.
			  */

			auto zookeeper = getZooKeeper();

			zkutil::Stat stat;
			const String columns_str = zookeeper->get(zookeeper_path + "/columns", &stat, alter_thread_event);
			auto columns_desc = ColumnsDescription<true>::parse(columns_str, context.getDataTypeFactory());

			auto & columns = columns_desc.columns;
			auto & materialized_columns = columns_desc.materialized;
			auto & alias_columns = columns_desc.alias;
			auto & column_defaults = columns_desc.defaults;

			bool changed_version = (stat.version != columns_version);

			MergeTreeData::DataParts parts;

			/// Если описание столбцов изменилось, обновим структуру таблицы локально.
			if (changed_version)
			{
				LOG_INFO(log, "Changed version of 'columns' node in ZooKeeper. Waiting for structure write lock.");

				auto table_lock = lockStructureForAlter();

				const auto columns_changed = columns != data.getColumnsListNonMaterialized();
				const auto materialized_columns_changed = materialized_columns != data.materialized_columns;
				const auto alias_columns_changed = alias_columns != data.alias_columns;
				const auto column_defaults_changed = column_defaults != data.column_defaults;

				if (columns_changed || materialized_columns_changed || alias_columns_changed ||
					column_defaults_changed)
				{
					LOG_INFO(log, "Columns list changed in ZooKeeper. Applying changes locally.");

					InterpreterAlterQuery::updateMetadata(database_name, table_name, columns,
						materialized_columns, alias_columns, column_defaults, context);

					if (columns_changed)
					{
						data.setColumnsList(columns);

						if (unreplicated_data)
							unreplicated_data->setColumnsList(columns);
					}

					if (materialized_columns_changed)
					{
						this->materialized_columns = materialized_columns;
						data.materialized_columns = std::move(materialized_columns);
					}

					if (alias_columns_changed)
					{
						this->alias_columns = alias_columns;
						data.alias_columns = std::move(alias_columns);
					}

					if (column_defaults_changed)
					{
						this->column_defaults = column_defaults;
						data.column_defaults = std::move(column_defaults);
					}

					LOG_INFO(log, "Applied changes to table.");
				}
				else
				{
					LOG_INFO(log, "Columns version changed in ZooKeeper, but data wasn't changed. It's like cyclic ALTERs.");
				}

				/// Нужно получить список кусков под блокировкой таблицы, чтобы избежать race condition с мерджем.
				parts = data.getDataParts();

				columns_version = stat.version;
			}

			/// Обновим куски.
			if (changed_version || force_recheck_parts)
			{
				auto table_lock = lockStructure(false);

				if (changed_version)
					LOG_INFO(log, "ALTER-ing parts");

				int changed_parts = 0;

				if (!changed_version)
					parts = data.getDataParts();

				const auto columns_plus_materialized = data.getColumnsList();

				for (const MergeTreeData::DataPartPtr & part : parts)
				{
					/// Обновим кусок и запишем результат во временные файлы.
					/// TODO: Можно пропускать проверку на слишком большие изменения, если в ZooKeeper есть, например,
					///  нода /flags/force_alter.
					auto transaction = data.alterDataPart(part, columns_plus_materialized);

					if (!transaction)
						continue;

					++changed_parts;

					/// Обновим метаданные куска в ZooKeeper.
					zkutil::Ops ops;
					ops.push_back(new zkutil::Op::SetData(
						replica_path + "/parts/" + part->name + "/columns", transaction->getNewColumns().toString(), -1));
					ops.push_back(new zkutil::Op::SetData(
						replica_path + "/parts/" + part->name + "/checksums", transaction->getNewChecksums().toString(), -1));
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
						auto transaction = unreplicated_data->alterDataPart(part, columns_plus_materialized);

						if (!transaction)
							continue;

						++changed_parts;

						transaction->commit();
					}
				}

				/// Список столбцов для конкретной реплики.
				zookeeper->set(replica_path + "/columns", columns_str);

				if (changed_version)
				{
					if (changed_parts != 0)
						LOG_INFO(log, "ALTER-ed " << changed_parts << " parts");
					else
						LOG_INFO(log, "No parts ALTER-ed");
				}

				force_recheck_parts = false;
			}

			parts.clear();
			alter_thread_event->wait();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);

			force_recheck_parts = true;

			alter_thread_event->tryWait(ERROR_SLEEP_MS);
		}
	}

	LOG_DEBUG(log, "Alter thread finished");
}


void StorageReplicatedMergeTree::removePartAndEnqueueFetch(const String & part_name)
{
	auto zookeeper = getZooKeeper();

	String part_path = replica_path + "/parts/" + part_name;

	LogEntryPtr log_entry = new LogEntry;
	log_entry->type = LogEntry::GET_PART;
	log_entry->create_time = time(0);
	log_entry->source_replica = "";
	log_entry->new_part_name = part_name;

	zkutil::Ops ops;
	ops.push_back(new zkutil::Op::Create(
		replica_path + "/queue/queue-", log_entry->toString(), zookeeper->getDefaultACL(),
		zkutil::CreateMode::PersistentSequential));
	ops.push_back(new zkutil::Op::Remove(part_path + "/checksums", -1));
	ops.push_back(new zkutil::Op::Remove(part_path + "/columns", -1));
	ops.push_back(new zkutil::Op::Remove(part_path, -1));
	auto results = zookeeper->multi(ops);

	{
		std::lock_guard<std::mutex> lock(queue_mutex);

		String path_created = dynamic_cast<zkutil::Op::Create &>(ops[0]).getPathCreated();
		log_entry->znode_name = path_created.substr(path_created.find_last_of('/') + 1);
		log_entry->addResultToVirtualParts(*this);
		queue.push_back(log_entry);
	}
}


void StorageReplicatedMergeTree::enqueuePartForCheck(const String & name)
{
	std::lock_guard<std::mutex> lock(parts_to_check_mutex);

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
			auto zookeeper = getZooKeeper();

			/// Достанем из очереди кусок для проверки.
			String part_name;
			{
				std::lock_guard<std::mutex> lock(parts_to_check_mutex);
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
			if (part_name.empty())	/// TODO Здесь race condition?
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
								std::lock_guard<std::mutex> lock(queue_mutex);

								for (LogEntries::iterator it = queue.begin(); it != queue.end(); )
								{
									if ((*it)->new_part_name == part_name)
									{
										zookeeper->tryRemove(replica_path + "/queue/" + (*it)->znode_name);
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
								LOG_ERROR(log, "Part " << part_name << " is lost forever.");
								ProfileEvents::increment(ProfileEvents::ReplicatedDataLoss);

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

						MergeTreePartChecker::Settings settings;
						settings.setIndexGranularity(data.index_granularity);
						settings.setRequireChecksums(true);
						settings.setRequireColumnFiles(true);
						MergeTreePartChecker::checkDataPart(
							data.getFullPath() + part_name, settings, context.getDataTypeFactory());

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
				/// Возможно, кусок кто-то только что записал, и еще не успел добавить в ZK.
				/// Поэтому удаляем только если кусок старый (не очень надежно).
				else if (part->modification_time + 5 * 60 < time(0))
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
				std::lock_guard<std::mutex> lock(parts_to_check_mutex);
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


void StorageReplicatedMergeTree::becomeLeader()
{
	LOG_INFO(log, "Became leader");
	is_leader_node = true;
	merge_selecting_thread = std::thread(&StorageReplicatedMergeTree::mergeSelectingThread, this);
}


String StorageReplicatedMergeTree::findReplicaHavingPart(const String & part_name, bool active)
{
	auto zookeeper = getZooKeeper();
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


void StorageReplicatedMergeTree::fetchPart(const String & part_name, const String & replica_path, bool to_detached)
{
	auto zookeeper = getZooKeeper();

	LOG_DEBUG(log, "Fetching part " << part_name << " from " << replica_path);

	TableStructureReadLockPtr table_lock;
	if (!to_detached)
		table_lock = lockStructure(true);

	String host;
	int port;

	String host_port_str = zookeeper->get(replica_path + "/host");
	ReadBufferFromString buf(host_port_str);
	assertString("host: ", buf);
	readString(host, buf);
	assertString("\nport: ", buf);
	readText(port, buf);
	assertString("\n", buf);
	assertEOF(buf);

	MergeTreeData::MutableDataPartPtr part = fetcher.fetchPart(part_name, replica_path, host, port, to_detached);

	if (!to_detached)
	{
		zkutil::Ops ops;
		checkPartAndAddToZooKeeper(part, ops, part_name);

		MergeTreeData::Transaction transaction;
		auto removed_parts = data.renameTempPartAndReplace(part, nullptr, &transaction);

		zookeeper->multi(ops);
		transaction.commit();
		merge_selecting_event.set();

		for (const auto & removed_part : removed_parts)
		{
			LOG_DEBUG(log, "Part " << removed_part->name << " is rendered obsolete by fetching part " << part_name);
			ProfileEvents::increment(ProfileEvents::ObsoleteReplicatedParts);
		}
	}
	else
	{
		Poco::File(data.getFullPath() + "detached/tmp_" + part_name).renameTo(data.getFullPath() + "detached/" + part_name);
	}

	ProfileEvents::increment(ProfileEvents::ReplicatedPartFetches);

	LOG_DEBUG(log, "Fetched part " << part_name << " from " << replica_name << (to_detached ? " (to 'detached' directory)" : ""));
}


void StorageReplicatedMergeTree::shutdown()
{
	if (restarting_thread)
	{
		restarting_thread->stop();
		restarting_thread.reset();
	}

	endpoint_holder = nullptr;
}


StorageReplicatedMergeTree::~StorageReplicatedMergeTree()
{
	try
	{
		shutdown();
	}
	catch(...)
	{
		tryLogCurrentException(__PRETTY_FUNCTION__);
	}
}


BlockInputStreams StorageReplicatedMergeTree::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned threads)
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

	/// Если запрошен хотя бы один виртуальный столбец, пробуем индексировать
	if (!virt_column_names.empty())
		VirtualColumnUtils::filterBlockWithQuery(query, virtual_columns_block, context);

	std::multiset<UInt8> values = VirtualColumnUtils::extractSingleValueFromBlock<UInt8>(virtual_columns_block, "_replicated");

	BlockInputStreams res;

	size_t part_index = 0;

	if ((settings.parallel_replica_offset == 0) && unreplicated_reader && values.count(0))
	{
		res = unreplicated_reader->read(real_column_names, query,
										context, settings, processed_stage,
										max_block_size, threads, &part_index);

		for (auto & virtual_column : virt_column_names)
		{
			if (virtual_column == "_replicated")
			{
				for (auto & stream : res)
					stream = new AddingConstColumnBlockInputStream<UInt8>(stream, new DataTypeUInt8, 0, "_replicated");
			}
		}
	}

	if (values.count(1))
	{
		auto res2 = reader.read(real_column_names, query, context, settings, processed_stage, max_block_size, threads, &part_index);

		for (auto & virtual_column : virt_column_names)
		{
			if (virtual_column == "_replicated")
			{
				for (auto & stream : res2)
					stream = new AddingConstColumnBlockInputStream<UInt8>(stream, new DataTypeUInt8, 1, "_replicated");
			}
		}

		res.insert(res.end(), res2.begin(), res2.end());
	}

	return res;
}


BlockOutputStreamPtr StorageReplicatedMergeTree::write(ASTPtr query)
{
	if (is_readonly)
		throw Exception("Table is in readonly mode", ErrorCodes::TABLE_IS_READ_ONLY);

	String insert_id;
	if (query)
		if (ASTInsertQuery * insert = typeid_cast<ASTInsertQuery *>(&*query))
			insert_id = insert->insert_id;

	return new ReplicatedMergeTreeBlockOutputStream(*this, insert_id);
}


bool StorageReplicatedMergeTree::optimize(const Settings & settings)
{
	/// Померджим какие-нибудь куски из директории unreplicated.
	/// TODO: Мерджить реплицируемые куски тоже.

	if (!unreplicated_data)
		return false;

	std::lock_guard<std::mutex> lock(unreplicated_mutex);

	unreplicated_data->clearOldParts();

	MergeTreeData::DataPartsVector parts;
	String merged_name;
	auto always_can_merge = [](const MergeTreeData::DataPartPtr & a, const MergeTreeData::DataPartPtr & b) { return true; };
	if (!unreplicated_merger->selectPartsToMerge(parts, merged_name, 0, true, true, false, always_can_merge))
		return false;

	const auto & merge_entry = context.getMergeList().insert(database_name, table_name, merged_name);
	unreplicated_merger->mergeParts(parts, merged_name, *merge_entry, settings.min_bytes_to_use_direct_io);

	return true;
}


void StorageReplicatedMergeTree::alter(const AlterCommands & params,
	const String & database_name, const String & table_name, Context & context)
{
	auto zookeeper = getZooKeeper();
	const MergeTreeMergeBlocker merge_blocker{merger};
	const auto unreplicated_merge_blocker = unreplicated_merger ?
		std::make_unique<MergeTreeMergeBlocker>(*unreplicated_merger) : nullptr;

	LOG_DEBUG(log, "Doing ALTER");

	NamesAndTypesList new_columns;
	NamesAndTypesList new_materialized_columns;
	NamesAndTypesList new_alias_columns;
	ColumnDefaults new_column_defaults;
	String new_columns_str;
	int new_columns_version;
	zkutil::Stat stat;

	{
		auto table_lock = lockStructureForAlter();

		if (is_readonly)
			throw Exception("Can't ALTER readonly table", ErrorCodes::TABLE_IS_READ_ONLY);

		data.checkAlter(params);

		new_columns = data.getColumnsListNonMaterialized();
		new_materialized_columns = data.materialized_columns;
		new_alias_columns = data.alias_columns;
		new_column_defaults = data.column_defaults;
		params.apply(new_columns, new_materialized_columns, new_alias_columns, new_column_defaults);

		new_columns_str = ColumnsDescription<false>{
			new_columns, new_materialized_columns,
			new_alias_columns, new_column_defaults
		}.toString();

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


/// Название воображаемого куска, покрывающего все возможные куски в указанном месяце с номерами в указанном диапазоне.
static String getFakePartNameForDrop(const String & month_name, UInt64 left, UInt64 right)
{
	/// Диапазон дат - весь месяц.
	DateLUT & lut = DateLUT::instance();
	time_t start_time = DateLUT::instance().YYYYMMDDToDate(parse<UInt32>(month_name + "01"));
	DayNum_t left_date = lut.toDayNum(start_time);
	DayNum_t right_date = DayNum_t(static_cast<size_t>(left_date) + lut.daysInMonth(start_time) - 1);

	/// Уровень - right-left+1: кусок не мог образоваться в результате такого или большего количества слияний.
	return ActiveDataPartSet::getPartName(left_date, right_date, left, right, right - left + 1);
}


void StorageReplicatedMergeTree::dropUnreplicatedPartition(const Field & partition, const Settings & settings)
{
	if (!unreplicated_data)
		return;

	/// Просит завершить мерджи и не позволяет им начаться.
	/// Это защищает от "оживания" данных за удалённую партицию после завершения мерджа.
	const MergeTreeMergeBlocker merge_blocker{*unreplicated_merger};
	auto structure_lock = lockStructure(true);

	const DayNum_t month = MergeTreeData::getMonthDayNum(partition);

	size_t removed_parts = 0;
	MergeTreeData::DataParts parts = unreplicated_data->getDataParts();

	for (const auto & part : parts)
	{
		if (!(part->left_month == part->right_month && part->left_month == month))
			continue;

		LOG_DEBUG(log, "Removing unreplicated part " << part->name);
		++removed_parts;

		unreplicated_data->replaceParts({part}, {}, false);
	}

	LOG_INFO(log, "Removed " << removed_parts << " unreplicated parts inside " << apply_visitor(FieldVisitorToString(), partition) << ".");
}


void StorageReplicatedMergeTree::dropPartition(const Field & field, bool detach, bool unreplicated, const Settings & settings)
{
	if (unreplicated)
	{
		if (detach)
			throw Exception{
				"DETACH UNREPLICATED PATITION not supported",
				ErrorCodes::LOGICAL_ERROR
			};

		dropUnreplicatedPartition(field, settings);

		return;
	}

	auto zookeeper = getZooKeeper();
	String month_name = MergeTreeData::getMonthName(field);

	/// TODO: Делать запрос в лидера по TCP.
	if (!is_leader_node)
		throw Exception("DROP PARTITION can only be done on leader replica.", ErrorCodes::NOT_LEADER);

	/** Пропустим один номер в block_numbers для удаляемого месяца, и будем удалять только куски до этого номера.
	  * Это запретит мерджи удаляемых кусков с новыми вставляемыми данными.
	  * Инвариант: в логе не появятся слияния удаляемых кусков с другими кусками.
	  * NOTE: Если понадобится аналогично поддержать запрос DROP PART, для него придется придумать какой-нибудь новый механизм,
	  *        чтобы гарантировать этот инвариант.
	  */
	UInt64 right;

	{
		AbandonableLockInZooKeeper block_number_lock = allocateBlockNumber(month_name);
		right = block_number_lock.getNumber();
		block_number_lock.unlock();
	}

	/// Такого никогда не должно происходить.
	if (right == 0)
		return;
	--right;

	String fake_part_name = getFakePartNameForDrop(month_name, 0, right);

	/** Запретим выбирать для слияния удаляемые куски - сделаем вид, что их всех уже собираются слить в fake_part_name.
	  * Инвариант: после появления в логе записи DROP_RANGE, в логе не появятся слияния удаляемых кусков.
	  */
	{
		std::lock_guard<std::mutex> merge_selecting_lock(merge_selecting_mutex);

		virtual_parts.add(fake_part_name);
	}

	/// Наконец, добившись нужных инвариантов, можно положить запись в лог.
	LogEntry entry;
	entry.type = LogEntry::DROP_RANGE;
	entry.source_replica = replica_name;
	entry.new_part_name = fake_part_name;
	entry.detach = detach;
	String log_znode_path = zookeeper->create(zookeeper_path + "/log/log-", entry.toString(), zkutil::CreateMode::PersistentSequential);
	entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

	/// Если надо - дожидаемся выполнения операции на себе или на всех репликах.
	if (settings.replication_alter_partitions_sync != 0)
	{
		if (settings.replication_alter_partitions_sync == 1)
			waitForReplicaToProcessLogEntry(replica_name, entry);
		else
			waitForAllReplicasToProcessLogEntry(entry);
	}
}


void StorageReplicatedMergeTree::attachPartition(const Field & field, bool unreplicated, bool attach_part, const Settings & settings)
{
	auto zookeeper = getZooKeeper();
	String partition;

	if (attach_part)
		partition = field.getType() == Field::Types::UInt64 ? toString(field.get<UInt64>()) : field.safeGet<String>();
	else
		partition = MergeTreeData::getMonthName(field);

	String source_dir = (unreplicated ? "unreplicated/" : "detached/");

	/// Составим список кусков, которые нужно добавить.
	Strings parts;
	if (attach_part)
	{
		parts.push_back(partition);
	}
	else
	{
		LOG_DEBUG(log, "Looking for parts for partition " << partition << " in " << source_dir);
		ActiveDataPartSet active_parts;
		for (Poco::DirectoryIterator it = Poco::DirectoryIterator(full_path + source_dir); it != Poco::DirectoryIterator(); ++it)
		{
			String name = it.name();
			if (!ActiveDataPartSet::isPartDirectory(name))
				continue;
			if (0 != name.compare(0, partition.size(), partition))
				continue;
			LOG_DEBUG(log, "Found part " << name);
			active_parts.add(name);
		}
		LOG_DEBUG(log, active_parts.size() << " of them are active");
		parts = active_parts.getParts();
	}

	/// Синхронно проверим, что добавляемые куски существуют и не испорчены хотя бы на этой реплике. Запишем checksums.txt, если его нет.
	LOG_DEBUG(log, "Checking parts");
	for (const String & part : parts)
	{
		LOG_DEBUG(log, "Checking part " << part);
		data.loadPartAndFixMetadata(source_dir + part);
	}

	/// Выделим добавляемым кускам максимальные свободные номера, меньшие RESERVED_BLOCK_NUMBERS.
	/// NOTE: Проверка свободности номеров никак не синхронизируется. Выполнять несколько запросов ATTACH/DETACH/DROP одновременно нельзя.
	UInt64 min_used_number = RESERVED_BLOCK_NUMBERS;

	{
		/// TODO Это необходимо лишь в пределах одного месяца.
		auto existing_parts = data.getDataParts();
		for (const auto & part : existing_parts)
			min_used_number = std::min(min_used_number, part->left);
	}

	if (parts.size() > min_used_number)
		throw Exception("Not enough free small block numbers for attaching parts: "
			+ toString(parts.size()) + " needed, " + toString(min_used_number) + " available", ErrorCodes::NOT_ENOUGH_BLOCK_NUMBERS);

	/// Добавим записи в лог.
	std::reverse(parts.begin(), parts.end());
	std::list<LogEntry> entries;
	zkutil::Ops ops;
	for (const String & part_name : parts)
	{
		ActiveDataPartSet::Part part;
		ActiveDataPartSet::parsePartName(part_name, part);
		part.left = part.right = --min_used_number;
		String new_part_name = ActiveDataPartSet::getPartName(part.left_date, part.right_date, part.left, part.right, part.level);

		LOG_INFO(log, "Will attach " << part_name << " as " << new_part_name);

		entries.emplace_back();
		LogEntry & entry = entries.back();
		entry.type = LogEntry::ATTACH_PART;
		entry.source_replica = replica_name;
		entry.source_part_name = part_name;
		entry.new_part_name = new_part_name;
		entry.attach_unreplicated = unreplicated;
		ops.push_back(new zkutil::Op::Create(
			zookeeper_path + "/log/log-", entry.toString(), zookeeper->getDefaultACL(), zkutil::CreateMode::PersistentSequential));
	}

	LOG_DEBUG(log, "Adding attaches to log");
	zookeeper->multi(ops);

	/// Если надо - дожидаемся выполнения операции на себе или на всех репликах.
	if (settings.replication_alter_partitions_sync != 0)
	{
		size_t i = 0;
		for (LogEntry & entry : entries)
		{
			String log_znode_path = dynamic_cast<zkutil::Op::Create &>(ops[i]).getPathCreated();
			entry.znode_name = log_znode_path.substr(log_znode_path.find_last_of('/') + 1);

			if (settings.replication_alter_partitions_sync == 1)
				waitForReplicaToProcessLogEntry(replica_name, entry);
			else
				waitForAllReplicasToProcessLogEntry(entry);

			++i;
		}
	}
}


void StorageReplicatedMergeTree::drop()
{
	if (is_readonly)
		throw Exception("Can't drop readonly replicated table (need to drop data in ZooKeeper as well)", ErrorCodes::TABLE_IS_READ_ONLY);

	auto zookeeper = getZooKeeper();

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


void StorageReplicatedMergeTree::rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name)
{
	std::string new_full_path = new_path_to_db + escapeForFileName(new_table_name) + '/';

	data.setPath(new_full_path, true);
	if (unreplicated_data)
		unreplicated_data->setPath(new_full_path + "unreplicated/", false);

	database_name = new_database_name;
	table_name = new_table_name;
	full_path = new_full_path;

	/// TODO: Можно обновить названия логгеров.
}


AbandonableLockInZooKeeper StorageReplicatedMergeTree::allocateBlockNumber(const String & month_name)
{
	auto zookeeper = getZooKeeper();

	String month_path = zookeeper_path + "/block_numbers/" + month_name;
	if (!zookeeper->exists(month_path))
	{
		/// Создадим в block_numbers ноду для месяца и пропустим в ней 200 значений инкремента.
		/// Нужно, чтобы в будущем при необходимости можно было добавить данные в начало.
		zkutil::Ops ops;
		auto acl = zookeeper->getDefaultACL();
		ops.push_back(new zkutil::Op::Create(month_path, "", acl, zkutil::CreateMode::Persistent));
		for (size_t i = 0; i < RESERVED_BLOCK_NUMBERS; ++i)
		{
			ops.push_back(new zkutil::Op::Create(month_path + "/skip_increment", "", acl, zkutil::CreateMode::Persistent));
			ops.push_back(new zkutil::Op::Remove(month_path + "/skip_increment", -1));
		}
		/// Игнорируем ошибки - не получиться могло только если кто-то еще выполнил эту строчку раньше нас.
		zookeeper->tryMulti(ops);
	}

	return AbandonableLockInZooKeeper(
		zookeeper_path + "/block_numbers/" + month_name + "/block-",
		zookeeper_path + "/temp", *zookeeper);
}


void StorageReplicatedMergeTree::waitForAllReplicasToProcessLogEntry(const LogEntry & entry)
{
	auto zookeeper = getZooKeeper();
	LOG_DEBUG(log, "Waiting for all replicas to process " << entry.znode_name);

	Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
	for (const String & replica : replicas)
		waitForReplicaToProcessLogEntry(replica, entry);

	LOG_DEBUG(log, "Finished waiting for all replicas to process " << entry.znode_name);
}


void StorageReplicatedMergeTree::waitForReplicaToProcessLogEntry(const String & replica, const LogEntry & entry)
{
	auto zookeeper = getZooKeeper();

	UInt64 log_index = parse<UInt64>(entry.znode_name.substr(entry.znode_name.size() - 10));
	String log_entry_str = entry.toString();

	LOG_DEBUG(log, "Waiting for " << replica << " to pull " << entry.znode_name << " to queue");

	/// Дождемся, пока запись попадет в очередь реплики.
	while (true)
	{
		zkutil::EventPtr event = new Poco::Event;

		String pointer = zookeeper->get(zookeeper_path + "/replicas/" + replica + "/log_pointer", nullptr, event);
		if (!pointer.empty() && parse<UInt64>(pointer) > log_index)
			break;

		event->wait();
	}

	LOG_DEBUG(log, "Looking for " << entry.znode_name << " in " << replica << " queue");

	/// Найдем запись в очереди реплики.
	Strings queue_entries = zookeeper->getChildren(zookeeper_path + "/replicas/" + replica + "/queue");
	String entry_to_wait_for;

	for (const String & entry_name : queue_entries)
	{
		String queue_entry_str;
		bool exists = zookeeper->tryGet(zookeeper_path + "/replicas/" + replica + "/queue/" + entry_name, queue_entry_str);
		if (exists && queue_entry_str == log_entry_str)
		{
			entry_to_wait_for = entry_name;
			break;
		}
	}

	/// Пока искали запись, ее уже выполнили и удалили.
	if (entry_to_wait_for.empty())
		return;

	LOG_DEBUG(log, "Waiting for " << entry_to_wait_for << " to disappear from " << replica << " queue");

	/// Дождемся, пока запись исчезнет из очереди реплики.
	while (true)
	{
		zkutil::EventPtr event = new Poco::Event;

		String unused;
		/// get вместо exists, чтобы не утек watch, если ноды уже нет.
		if (!zookeeper->tryGet(zookeeper_path + "/replicas/" + replica + "/queue/" + entry_to_wait_for, unused, nullptr, event))
			break;

		event->wait();
	}
}


void StorageReplicatedMergeTree::getStatus(Status & res, bool with_zk_fields)
{
	auto zookeeper = getZooKeeper();

	res.is_leader = is_leader_node;
	res.is_readonly = is_readonly;
	res.is_session_expired = !zookeeper || zookeeper->expired();

	{
		std::lock_guard<std::mutex> lock(queue_mutex);
		res.future_parts = future_parts.size();
		res.queue_size = queue.size();

		res.inserts_in_queue = 0;
		res.merges_in_queue = 0;
		res.queue_oldest_time = 0;

		for (const LogEntryPtr & entry : queue)
		{
			if (entry->type == LogEntry::GET_PART)
				++res.inserts_in_queue;
			if (entry->type == LogEntry::MERGE_PARTS)
				++res.merges_in_queue;

			if (entry->create_time && (!res.queue_oldest_time || entry->create_time < res.queue_oldest_time))
				res.queue_oldest_time = entry->create_time;
		}
	}

	{
		std::lock_guard<std::mutex> lock(parts_to_check_mutex);
		res.parts_to_check = parts_to_check_set.size();
	}

	res.zookeeper_path = zookeeper_path;
	res.replica_name = replica_name;
	res.replica_path = replica_path;
	res.columns_version = columns_version;

	if (res.is_session_expired || !with_zk_fields)
	{
		res.log_max_index = 0;
		res.log_pointer = 0;
		res.total_replicas = 0;
		res.active_replicas = 0;
	}
	else
	{
		auto log_entries = zookeeper->getChildren(zookeeper_path + "/log");

		if (log_entries.empty())
		{
			res.log_max_index = 0;
		}
		else
		{
			const String & last_log_entry = *std::max_element(log_entries.begin(), log_entries.end());
			res.log_max_index = parse<UInt64>(last_log_entry.substr(strlen("log-")));
		}

		String log_pointer_str = zookeeper->get(replica_path + "/log_pointer");
		res.log_pointer = log_pointer_str.empty() ? 0 : parse<UInt64>(log_pointer_str);

		auto all_replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
		res.total_replicas = all_replicas.size();

		res.active_replicas = 0;
		for (const String & replica : all_replicas)
			if (zookeeper->exists(zookeeper_path + "/replicas/" + replica + "/is_active"))
				++res.active_replicas;
	}
}


void StorageReplicatedMergeTree::fetchPartition(const Field & partition, const String & from_, const Settings & settings)
{
	auto zookeeper = getZooKeeper();

	String partition_str = MergeTreeData::getMonthName(partition);

	String from = from_;
	if (from.back() == '/')
		from.resize(from.size() - 1);

	LOG_INFO(log, "Will fetch partition " << partition_str << " from shard " << from_);

	/** Проверим, что в директории detached (куда мы будем записывать скаченные куски) ещё нет такой партиции.
	  * Ненадёжно (есть race condition) - такая партиция может появиться чуть позже.
	  */
	Poco::DirectoryIterator dir_end;
	for (Poco::DirectoryIterator dir_it{data.getFullPath() + "detached/"}; dir_it != dir_end; ++dir_it)
		if (0 == dir_it.name().compare(0, partition_str.size(), partition_str))
			throw Exception("Detached partition " + partition_str + " is already exists.", ErrorCodes::PARTITION_ALREADY_EXISTS);

	/// Список реплик шарда-источника.
	zkutil::Strings replicas = zookeeper->getChildren(from + "/replicas");

	/// Оставим только активные реплики.
	zkutil::Strings active_replicas;
	active_replicas.reserve(replicas.size());

	for (const String & replica : replicas)
		if (zookeeper->exists(from + "/replicas/" + replica + "/is_active"))
			active_replicas.push_back(replica);

	if (active_replicas.empty())
		throw Exception("No active replicas for shard " + from, ErrorCodes::NO_ACTIVE_REPLICAS);

	/** Надо выбрать лучшую (наиболее актуальную) реплику.
	  * Это реплика с максимальным log_pointer, затем с минимальным размером queue.
	  * NOTE Это не совсем лучший критерий. Для скачивания старых партиций это не имеет смысла,
	  *  и было бы неплохо уметь выбирать реплику, ближайшую по сети.
	  * NOTE Разумеется, здесь есть data race-ы. Можно решить ретраями.
	  */
	Int64 max_log_pointer = -1;
	UInt64 min_queue_size = std::numeric_limits<UInt64>::max();
	String best_replica;

	for (const String & replica : active_replicas)
	{
		String current_replica_path = from + "/replicas/" + replica;

		String log_pointer_str = zookeeper->get(current_replica_path + "/log_pointer");
		Int64 log_pointer = log_pointer_str.empty() ? 0 : parse<UInt64>(log_pointer_str);

		zkutil::Stat stat;
		zookeeper->get(current_replica_path + "/queue", &stat);
		size_t queue_size = stat.numChildren;

		if (log_pointer > max_log_pointer
			|| (log_pointer == max_log_pointer && queue_size < min_queue_size))
		{
			max_log_pointer = log_pointer;
			min_queue_size = queue_size;
			best_replica = replica;
		}
	}

	if (best_replica.empty())
		throw Exception("Logical error: cannot choose best replica.", ErrorCodes::LOGICAL_ERROR);

	LOG_INFO(log, "Found " << replicas.size() << " replicas, " << active_replicas.size() << " of them are active."
		<< " Selected " << best_replica << " to fetch from.");

	String best_replica_path = from + "/replicas/" + best_replica;

	/// Выясним, какие куски есть на лучшей реплике.

	/** Пытаемся скачать эти куски.
	  * Часть из них могла удалиться из-за мерджа.
	  * В этом случае, обновляем информацию о доступных кусках и пробуем снова.
	  */

	unsigned try_no = 0;
	Strings missing_parts;
	do
	{
		if (try_no)
			LOG_INFO(log, "Some of parts (" << missing_parts.size() << ") are missing. Will try to fetch covering parts.");

		if (try_no >= 5)
			throw Exception("Too much retries to fetch parts from " + best_replica_path, ErrorCodes::TOO_MUCH_RETRIES_TO_FETCH_PARTS);

		Strings parts = zookeeper->getChildren(best_replica_path + "/parts");
		ActiveDataPartSet active_parts_set(parts);
		Strings parts_to_fetch;

		if (missing_parts.empty())
		{
			parts_to_fetch = active_parts_set.getParts();

			/// Оставляем только куски нужной партиции.
			Strings parts_to_fetch_partition;
			for (const String & part : parts_to_fetch)
				if (0 == part.compare(0, partition_str.size(), partition_str))
					parts_to_fetch_partition.push_back(part);

			parts_to_fetch = std::move(parts_to_fetch_partition);

			if (parts_to_fetch.empty())
				throw Exception("Partition " + partition_str + " on " + best_replica_path + " doesn't exist", ErrorCodes::PARTITION_DOESNT_EXIST);
		}
		else
		{
			for (const String & missing_part : missing_parts)
			{
				String containing_part = active_parts_set.getContainingPart(missing_part);
				if (!containing_part.empty())
					parts_to_fetch.push_back(containing_part);
				else
					LOG_WARNING(log, "Part " << missing_part << " on replica " << best_replica_path << " has been vanished.");
			}
		}

		LOG_INFO(log, "Parts to fetch: " << parts_to_fetch.size());

		missing_parts.clear();
		for (const String & part : parts_to_fetch)
		{
			try
			{
				fetchPart(part, best_replica_path, true);
			}
			catch (const DB::Exception & e)
			{
				if (e.code() != ErrorCodes::RECEIVED_ERROR_FROM_REMOTE_IO_SERVER)
					throw;

				LOG_INFO(log, e.displayText());
				missing_parts.push_back(part);
			}
		}

		++try_no;
	} while (!missing_parts.empty());
}


void StorageReplicatedMergeTree::freezePartition(const Field & partition, const Settings & settings)
{
	/// Префикс может быть произвольным. Не обязательно месяц - можно указать лишь год.
	String prefix = partition.getType() == Field::Types::UInt64
		? toString(partition.get<UInt64>())
		: partition.safeGet<String>();

	data.freezePartition(prefix);
	if (unreplicated_data)
		unreplicated_data->freezePartition(prefix);
}


}
