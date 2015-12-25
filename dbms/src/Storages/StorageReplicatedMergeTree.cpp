#include <ext/range.hpp>
#include <DB/Core/FieldVisitors.h>
#include <DB/Storages/ColumnsDescription.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeBlockOutputStream.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreePartsExchange.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeQuorumEntry.h>
#include <DB/Storages/MergeTree/MergeTreePartChecker.h>
#include <DB/Storages/MergeTree/MergeList.h>
#include <DB/Storages/MergeTree/MergeTreeWhereOptimizer.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeAddress.h>
#include <DB/Parsers/formatAST.h>
#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/Operators.h>
#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <DB/Common/VirtualColumnUtils.h>
#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/DataStreams/AddingConstColumnBlockInputStream.h>
#include <DB/DataStreams/RemoteBlockInputStream.h>
#include <DB/DataStreams/NullBlockOutputStream.h>
#include <DB/DataStreams/copyData.h>
#include <DB/Common/Macros.h>
#include <DB/Common/formatReadable.h>
#include <DB/Common/setThreadName.h>
#include <Poco/DirectoryIterator.h>
#include <time.h>


namespace DB
{


const auto ERROR_SLEEP_MS = 1000;
const auto MERGE_SELECTING_SLEEP_MS = 5 * 1000;

const Int64 RESERVED_BLOCK_NUMBERS = 200;


/** Для каждого куска есть сразу три места, где он должен быть:
  * 1. В оперативке (RAM), MergeTreeData::data_parts, all_data_parts.
  * 2. В файловой системе (FS), директория с данными таблицы.
  * 3. В ZooKeeper (ZK).
  *
  * При добавлении куска, его надо добавить сразу в эти три места.
  * Это делается так:
  * - [FS] сначала записываем кусок во временную директорию на файловой системе;
  * - [FS] переименовываем временный кусок в результирующий на файловой системе;
  * - [RAM] сразу же после этого добавляем его в data_parts, и удаляем из data_parts покрываемые им куски;
  * - [RAM] также устанавливаем объект Transaction, который в случае исключения (в следующем пункте),
  *   откатит изменения в data_parts (из предыдущего пункта) назад;
  * - [ZK] затем отправляем транзакцию (multi) на добавление куска в ZooKeeper (и ещё некоторых действий);
  * - [FS, ZK] кстати, удаление покрываемых (старых) кусков из файловой системы, из ZooKeeper и из all_data_parts
  *   делается отложенно, через несколько минут.
  *
  * Здесь нет никакой атомарности.
  * Можно было бы добиться атомарности с помощью undo/redo логов и флага в DataPart, когда он полностью готов.
  * Но это было бы неудобно - пришлось бы писать undo/redo логи для каждого Part-а в ZK, а это увеличило бы и без того большое количество взаимодействий.
  *
  * Вместо этого, мы вынуждены работать в ситуации, когда в любой момент времени
  *  (из другого потока, или после рестарта сервера) может наблюдаться недоделанная до конца транзакция.
  *  (заметим - для этого кусок должен быть в RAM)
  * Из этих случаев наиболее частый - когда кусок уже есть в data_parts, но его ещё нет в ZooKeeper.
  * Этот случай надо отличить от случая, когда такая ситуация достигается вследствие какого-то повреждения состояния.
  *
  * Делаем это с помощью порога на время.
  * Если кусок достаточно молодой, то его отсутствие в ZooKeeper будем воспринимать оптимистично - как будто он просто не успел ещё туда добавиться
  *  - как будто транзакция ещё не выполнена, но скоро выполнится.
  * А если кусок старый, то его отсутствие в ZooKeeper будем воспринимать как недоделанную транзакцию, которую нужно откатить.
  *
  * PS. Возможно, было бы лучше добавить в DataPart флаг о том, что кусок вставлен в ZK.
  * Но здесь уже слишком легко запутаться с консистентностью этого флага.
  */
const auto MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER = 5 * 60;


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
		if (!data.getDataParts().empty())
			throw Exception("Data directory for table already containing data parts - probably it was unclean DROP table or manual intervention. You must either clear directory by hand or use ATTACH TABLE instead of CREATE TABLE if you need to use that parts.", ErrorCodes::INCORRECT_DATA);

		createTableIfNotExists();

		checkTableStructure(false, false);
		createReplica();
	}
	else
	{
		checkTableStructure(skip_sanity_checks, true);
		checkParts(skip_sanity_checks);
	}

	createNewZooKeeperNodes();

	initVirtualParts();

	String unreplicated_path = full_path + "unreplicated/";
	if (Poco::File(unreplicated_path).exists())
	{
		unreplicated_data.reset(new MergeTreeData(unreplicated_path, columns_,
			materialized_columns_, alias_columns_, column_defaults_,
			context_, primary_expr_ast_,
			date_column_name_, sampling_expression_, index_granularity_, mode_, sign_column_, columns_to_sum_, settings_,
			database_name_ + "." + table_name + "[unreplicated]", false));

		unreplicated_data->loadDataParts(skip_sanity_checks);

		if (unreplicated_data->getDataPartsVector().empty())
		{
			unreplicated_data.reset();
		}
		else
		{
			LOG_INFO(log, "Have unreplicated data");
			unreplicated_reader.reset(new MergeTreeDataSelectExecutor(*unreplicated_data));
			unreplicated_merger.reset(new MergeTreeDataMerger(*unreplicated_data));
		}
	}

	loadQueue();

	/// В этом потоке реплика будет активирована.
	restarting_thread.reset(new ReplicatedMergeTreeRestartingThread(*this));
}


void StorageReplicatedMergeTree::createNewZooKeeperNodes()
{
	auto zookeeper = getZooKeeper();

	/// Работа с кворумом.
	zookeeper->createIfNotExists(zookeeper_path + "/quorum", "");
	zookeeper->createIfNotExists(zookeeper_path + "/quorum/last_part", "");
	zookeeper->createIfNotExists(zookeeper_path + "/quorum/failed_parts", "");

	/// Отслеживание отставания реплик.
	zookeeper->createIfNotExists(replica_path + "/min_unprocessed_insert_time", "");
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
	const Names & columns_to_sum_,
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
	std::string metadata;
	{
		WriteBufferFromString out(metadata);
		out << "metadata format version: 1" << "\n"
			<< "date column: " << data.date_column_name << "\n"
			<< "sampling expression: " << formattedAST(data.sampling_expression) << "\n"
			<< "index granularity: " << data.index_granularity << "\n"
			<< "mode: " << static_cast<int>(data.mode) << "\n"
			<< "sign column: " << data.sign_column << "\n"
			<< "primary key: " << formattedAST(data.primary_expr_ast) << "\n";
	}

	auto acl = zookeeper->getDefaultACL();

	zkutil::Ops ops;
	ops.push_back(new zkutil::Op::Create(zookeeper_path, "",
										 acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/metadata", metadata,
										 acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/columns", ColumnsDescription<false>{
				data.getColumnsListNonMaterialized(), data.materialized_columns,
				data.alias_columns, data.column_defaults}.toString(),
										 acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/log", "",
										 acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/blocks", "",
										 acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/block_numbers", "",
										 acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/nonincrement_block_numbers", "",
										 acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/leader_election", "",
										 acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/temp", "",
										 acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/replicas", "",
										 acl, zkutil::CreateMode::Persistent));

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
	auto columns_desc = ColumnsDescription<true>::parse(zookeeper->get(zookeeper_path + "/columns", &stat));

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


/** При необходимости восстановить кусок, реплика сама добавляет в свою очередь запись на его получение.
  * Какое поставить время для этой записи в очереди? Время учитывается при расчёте отставания реплики.
  * Для этих целей имеет смысл использовать время создания недостающего куска
  *  (то есть, при расчёте отставания будет учитано, насколько старый кусок нам нужно восстановить).
  */
static time_t tryGetPartCreateTime(zkutil::ZooKeeperPtr & zookeeper, const String & replica_path, const String & part_name)
{
	time_t res = 0;

	/// Узнаем время создания part-а, если он ещё существует (не был, например, смерджен).
	zkutil::Stat stat;
	String unused;
	if (zookeeper->tryGet(replica_path + "/parts/" + part_name, unused, &stat))
		res = stat.ctime / 1000;

	return res;
}


void StorageReplicatedMergeTree::createReplica()
{
	auto zookeeper = getZooKeeper();

	LOG_DEBUG(log, "Creating replica " << replica_path);

	/// Создадим пустую реплику. Ноду columns создадим в конце - будем использовать ее в качестве признака, что создание реплики завершено.
	auto acl = zookeeper->getDefaultACL();
	zkutil::Ops ops;
	ops.push_back(new zkutil::Op::Create(replica_path, "", acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(replica_path + "/host", "", acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(replica_path + "/log_pointer", "", acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(replica_path + "/queue", "", acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(replica_path + "/parts", "", acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(replica_path + "/flags", "", acl, zkutil::CreateMode::Persistent));

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
			log_entry.create_time = tryGetPartCreateTime(zookeeper, source_path, name);

			zookeeper->create(replica_path + "/queue/queue-", log_entry.toString(), zkutil::CreateMode::PersistentSequential);
		}
		LOG_DEBUG(log, "Queued " << active_parts.size() << " parts to be fetched");

		/// Добавим в очередь содержимое очереди эталонной реплики.
		for (const String & entry : source_queue)
		{
			zookeeper->create(replica_path + "/queue/queue-", entry, zkutil::CreateMode::PersistentSequential);
		}

		/// Далее оно будет загружено в переменную queue в методе loadQueue.

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
			expected_parts.erase(part->name);
		else
			unexpected_parts.insert(part);
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

	/** Для проверки адекватности, для кусков, которые есть в ФС, но нет в ZK, будем учитывать только не самые новые куски.
	  * Потому что неожиданные новые куски обычно возникают лишь оттого, что они не успели записаться в ZK при грубом перезапуске сервера.
	  * Также это возникает от дедуплицированных кусков, которые не успели удалиться.
	  */
	size_t unexpected_parts_nonnew = 0;
	for (const auto & part : unexpected_parts)
		if (part->level > 0 || part->right < RESERVED_BLOCK_NUMBERS)
			++unexpected_parts_nonnew;

	String sanity_report = "There are "
			+ toString(unexpected_parts.size()) + " unexpected parts ("
			+ toString(unexpected_parts_nonnew) + " of them is not just-written), "
			+ toString(parts_to_add.size()) + " unexpectedly merged parts, "
			+ toString(expected_parts.size()) + " missing obsolete parts, "
			+ toString(parts_to_fetch.size()) + " missing parts";

	/** Можно автоматически синхронизировать данные,
	  *  если количество ошибок каждого из четырёх типов не больше соответствующих порогов,
	  *  или если отношение общего количества ошибок к общему количеству кусков (минимальному - в локальной файловой системе или в ZK)
	  *  не больше некоторого отношения (например 5%).
	  *
	  * Большое количество несовпадений в данных на файловой системе и ожидаемых данных
	  *  может свидетельствовать об ошибке конфигурации (сервер случайно подключили как реплику не от того шарда).
	  * В этом случае, защитный механизм не даёт стартовать серверу.
	  */

	size_t min_parts_local_or_expected = std::min(expected_parts_vec.size(), parts.size());
	size_t total_difference = parts_to_add.size() + unexpected_parts_nonnew + expected_parts.size() + parts_to_fetch.size();

	bool insane =
		(parts_to_add.size() > data.settings.replicated_max_unexpectedly_merged_parts
			|| unexpected_parts_nonnew > data.settings.replicated_max_unexpected_parts
			|| expected_parts.size() > data.settings.replicated_max_missing_obsolete_parts
			|| parts_to_fetch.size() > data.settings.replicated_max_missing_active_parts)
		&& (total_difference > min_parts_local_or_expected * data.settings.replicated_max_ratio_of_wrong_parts);

	if (insane && !skip_sanity_checks)
		throw Exception("The local set of parts of table " + getTableName() + " doesn't look like the set of parts in ZooKeeper. "
			+ sanity_report, ErrorCodes::TOO_MANY_UNEXPECTED_DATA_PARTS);

	if (total_difference > 0)
		LOG_WARNING(log, sanity_report);

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
		removePartFromZooKeeper(name, ops);
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
		log_entry.create_time = tryGetPartCreateTime(zookeeper, replica_path, name);

		/// Полагаемся, что это происходит до загрузки очереди (loadQueue).
		zkutil::Ops ops;
		removePartFromZooKeeper(name, ops);
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

	auto acl = zookeeper->getDefaultACL();

	ops.push_back(new zkutil::Op::Check(
		zookeeper_path + "/columns",
		expected_columns_version));
	ops.push_back(new zkutil::Op::Create(
		replica_path + "/parts/" + part_name,
		"",
		acl,
		zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(
		replica_path + "/parts/" + part_name + "/columns",
		part->columns.toString(),
		acl,
		zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(
		replica_path + "/parts/" + part_name + "/checksums",
		part->checksums.toString(),
		acl,
		zkutil::CreateMode::Persistent));
}


void StorageReplicatedMergeTree::loadQueue()
{
	auto zookeeper = getZooKeeper();

	std::lock_guard<std::mutex> lock(queue_mutex);

	Strings children = zookeeper->getChildren(replica_path + "/queue");
	std::sort(children.begin(), children.end());

	std::vector<std::pair<String, zkutil::ZooKeeper::GetFuture>> futures;
	futures.reserve(children.size());

	for (const String & child : children)
		futures.emplace_back(child, zookeeper->asyncGet(replica_path + "/queue/" + child));

	for (auto & future : futures)
	{
		zkutil::ZooKeeper::ValueAndStat res = future.second.get();
		LogEntryPtr entry = LogEntry::parse(res.value, res.stat);

		entry->znode_name = future.first;
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

		LogEntryPtr entry = LogEntry::parse(entry_str, stat);

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

	last_queue_update = time(0);

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


bool StorageReplicatedMergeTree::shouldExecuteLogEntry(const LogEntry & entry, String & out_postpone_reason)
{
	/// queue_mutex уже захвачен. Функция вызывается только из queueTask.

	if (entry.type == LogEntry::MERGE_PARTS || entry.type == LogEntry::GET_PART || entry.type == LogEntry::ATTACH_PART)
	{
		/// Проверим, не создаётся ли сейчас этот же кусок другим действием.
		if (future_parts.count(entry.new_part_name))
		{
			String reason = "Not executing log entry for part " + entry.new_part_name
				+ " because another log entry for the same part is being processed. This shouldn't happen often.";
			LOG_DEBUG(log, reason);
			out_postpone_reason = reason;
			return false;

			/** Когда соответствующее действие завершится, то shouldExecuteLogEntry, в следующий раз, пройдёт успешно,
			  *  и элемент очереди будет обработан. Сразу же в функции executeLogEntry будет выяснено, что кусок у нас уже есть,
			  *  и элемент очереди будет сразу считаться обработанным.
			  */
		}

		/// Более сложная проверка - не создаётся ли сейчас другим действием кусок, который покроет этот кусок.
		/// NOTE То, что выше - избыточно, но оставлено ради более удобного сообщения в логе.
		ActiveDataPartSet::Part result_part;
		ActiveDataPartSet::parsePartName(entry.new_part_name, result_part);

		/// Оно может тормозить при большом размере future_parts. Но он не может быть большим, так как ограничен BackgroundProcessingPool.
		for (const auto & future_part_name : future_parts)
		{
			ActiveDataPartSet::Part future_part;
			ActiveDataPartSet::parsePartName(future_part_name, future_part);

			if (future_part.contains(result_part))
			{
				String reason = "Not executing log entry for part " + entry.new_part_name
					+ " because another log entry for covering part " + future_part_name + " is being processed.";
				LOG_DEBUG(log, reason);
				out_postpone_reason = reason;
				return false;
			}
		}
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
				String reason = "Not merging into part " + entry.new_part_name
					+ " because part " + name + " is not ready yet (log entry for that part is being processed).";
				LOG_TRACE(log, reason);
				out_postpone_reason = reason;
				return false;
			}
		}

		if (merger.isCancelled())
		{
			String reason = "Not executing log entry for part " + entry.new_part_name + " because merges are cancelled now.";
			LOG_DEBUG(log, reason);
			out_postpone_reason = reason;
			return false;
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

		/// Даже если кусок есть локально, его (в исключительных случаях) может не быть в zookeeper. Проверим, что он там есть.
		if (containing_part && zookeeper->exists(replica_path + "/parts/" + containing_part->name))
		{
			if (!(entry.type == LogEntry::GET_PART && entry.source_replica == replica_name))
				LOG_DEBUG(log, "Skipping action for part " << entry.new_part_name << " - part already exists.");
			return true;
		}
	}

	if (entry.type == LogEntry::GET_PART && entry.source_replica == replica_name)
		LOG_WARNING(log, "Part " << entry.new_part_name << " from own log doesn't exist.");

	/// Возможно, этот кусок нам не нужен, так как при записи с кворумом, кворум пофейлился (см. ниже про /quorum/failed_parts).
	if (entry.quorum && zookeeper->exists(zookeeper_path + "/quorum/failed_parts/" + entry.new_part_name))
	{
		LOG_DEBUG(log, "Skipping action for part " << entry.new_part_name << " because quorum for that part was failed.");
		return true;	/// NOTE Удаление из virtual_parts не делается, но оно нужно только для мерджей.
	}

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

			size_t sum_parts_size_in_bytes = MergeTreeDataMerger::estimateDiskSpaceForMerge(parts);

			/// Может бросить исключение.
			DiskSpaceMonitor::ReservationPtr reserved_space = DiskSpaceMonitor::reserve(full_path, sum_parts_size_in_bytes);

			auto table_lock = lockStructure(false);

			const auto & merge_entry = context.getMergeList().insert(database_name, table_name, entry.new_part_name);
			MergeTreeData::Transaction transaction;
			size_t aio_threshold = context.getSettings().min_bytes_to_use_direct_io;

			MergeTreeData::DataPartPtr part = merger.mergeParts(
				parts, entry.new_part_name, *merge_entry, aio_threshold, &transaction, reserved_space);

			zkutil::Ops ops;
			checkPartAndAddToZooKeeper(part, ops);

			/** TODO: Переименование нового куска лучше делать здесь, а не пятью строчками выше,
			  *  чтобы оно было как можно ближе к zookeeper->multi.
			  */

			zookeeper->multi(ops);

			/** Удаление старых кусков из ZK и с диска делается отложенно - см. ReplicatedMergeTreeCleanupThread, clearOldParts.
			  */

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
				/** Если кусок должен быть записан с кворумом, и кворум ещё недостигнут,
				  *  то (из-за того, что кусок невозможно прямо сейчас скачать),
				  *  кворумную запись следует считать безуспешной.
				  * TODO Сложный код, вынести отдельно.
				  */
				if (entry.quorum)
				{
					if (entry.type != LogEntry::GET_PART)
						throw Exception("Logical error: log entry with quorum but type is not GET_PART", ErrorCodes::LOGICAL_ERROR);

					if (entry.block_id.empty())
						throw Exception("Logical error: log entry with quorum have empty block_id", ErrorCodes::LOGICAL_ERROR);

					LOG_DEBUG(log, "No active replica has part " << entry.new_part_name << " which needs to be written with quorum."
						" Will try to mark that quorum as failed.");

					/** Атомарно:
					  * - если реплики не стали активными;
					  * - если существует узел quorum с этим куском;
					  * - удалим узел quorum;
					  * - установим nonincrement_block_numbers, чтобы разрешить мерджи через номер потерянного куска;
					  * - добавим кусок в список quorum/failed_parts;
					  * - если кусок ещё не удалён из списка для дедупликации blocks/block_num, то удалим его;
					  *
					  * Если что-то изменится, то ничего не сделаем - попадём сюда снова в следующий раз.
					  */

					/** Соберём версии узлов host у реплик.
					  * Когда реплика становится активной, она в той же транзакции (с созданием is_active), меняет значение host.
					  * Это позволит проследить, что реплики не стали активными.
					  */

					Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");

					zkutil::Ops ops;

					for (size_t i = 0, size = replicas.size(); i < size; ++i)
					{
						Stat stat;
						String path = zookeeper_path + "/replicas/" + replicas[i] + "/host";
						zookeeper->get(path, &stat);
						ops.push_back(new zkutil::Op::Check(path, stat.version));
					}

					/// Проверяем, что пока мы собирали версии, не ожила реплика с нужным куском.
					replica = findReplicaHavingPart(entry.new_part_name, true);

					/// Также за это время могла быть создана совсем новая реплика. Но если на старых не появится куска, то на новой его тоже не может быть.

					if (replica.empty())
					{
						Stat quorum_stat;
						String quorum_path = zookeeper_path + "/quorum/status";
						String quorum_str = zookeeper->get(quorum_path, &quorum_stat);
						ReplicatedMergeTreeQuorumEntry quorum_entry;
						quorum_entry.fromString(quorum_str);

						if (quorum_entry.part_name == entry.new_part_name)
						{
							ops.push_back(new zkutil::Op::Remove(quorum_path, quorum_stat.version));

							const auto partition_str = entry.new_part_name.substr(0, 6);
							ActiveDataPartSet::Part part_info;
							ActiveDataPartSet::parsePartName(entry.new_part_name, part_info);

							if (part_info.left != part_info.right)
								throw Exception("Logical error: log entry with quorum for part covering more than one block number",
									ErrorCodes::LOGICAL_ERROR);

							auto acl = zookeeper->getDefaultACL();

							ops.push_back(new zkutil::Op::Create(
								zookeeper_path + "/nonincrement_block_numbers/" + partition_str + "/block-" + padIndex(part_info.left),
								"",
								acl,
								zkutil::CreateMode::Persistent));

							ops.push_back(new zkutil::Op::Create(
								zookeeper_path + "/quorum/failed_parts/" + entry.new_part_name,
								"",
								acl,
								zkutil::CreateMode::Persistent));

							/// Удаление из blocks.
							if (zookeeper->exists(zookeeper_path + "/blocks/" + entry.block_id))
							{
								ops.push_back(new zkutil::Op::Remove(zookeeper_path + "/blocks/" + entry.block_id + "/number", -1));
								ops.push_back(new zkutil::Op::Remove(zookeeper_path + "/blocks/" + entry.block_id + "/checksum", -1));
								ops.push_back(new zkutil::Op::Remove(zookeeper_path + "/blocks/" + entry.block_id, -1));
							}

							auto code = zookeeper->tryMulti(ops);

							if (code == ZOK)
							{
								LOG_DEBUG(log, "Marked quorum for part " << entry.new_part_name << " as failed.");
								return true;	/// NOTE Удаление из virtual_parts не делается, но оно нужно только для мерджей.
							}
							else if (code == ZBADVERSION || code == ZNONODE || code == ZNODEEXISTS)
							{
								LOG_DEBUG(log, "State was changed or isn't expected when trying to mark quorum for part "
									<< entry.new_part_name << " as failed.");
							}
							else
								throw zkutil::KeeperException(code);
						}
						else
						{
							LOG_WARNING(log, "No active replica has part " << entry.new_part_name
								<< ", but that part needs quorum and /quorum/status contains entry about another part " << quorum_entry.part_name
								<< ". It means that part was successfully written to " << entry.quorum << " replicas, but then all of them goes offline."
								<< " Or it is a bug.");
						}
					}
				}

				if (replica.empty())
				{
					ProfileEvents::increment(ProfileEvents::ReplicatedPartFailedFetches);
					throw Exception("No active replica has part " + entry.new_part_name, ErrorCodes::NO_REPLICA_HAS_PART);
				}
			}

			fetchPart(entry.new_part_name, zookeeper_path + "/replicas/" + replica, false, entry.quorum);

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
						LOG_INFO(log, "No active replica has part " << entry.new_part_name << ". Will fetch merged part instead.");
						return false;
					}
				}

				/** Если ни у какой активной реплики нет куска, и в очереди нет слияний с его участием,
				  * проверим, есть ли у любой (активной или неактивной) реплики такой кусок или покрывающий его.
				  */
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
		removePartFromZooKeeper(part->name, ops);
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
	setThreadName("ReplMTQueueUpd");

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
				if ((*it)->currently_executing)
					continue;

				if (shouldExecuteLogEntry(**it, (*it)->postpone_reason))
				{
					entry = *it;
					entry->tagPartAsFuture(*this);
					queue.splice(queue.end(), queue, it);
					entry->currently_executing = true;
					++entry->num_tries;
					entry->last_attempt_time = time(0);
					break;
				}
				else
				{
					++(*it)->num_postponed;
					(*it)->last_postpone_time = time(0);
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

	bool was_exception = true;
	bool success = false;
	std::exception_ptr saved_exception;

	try
	{
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
		}
		catch (...)
		{
			saved_exception = std::current_exception();
			throw;
		}

		was_exception = false;
	}
	catch (const Exception & e)
	{
		if (e.code() == ErrorCodes::NO_REPLICA_HAS_PART)
		{
			/// Если ни у кого нет нужного куска, наверно, просто не все реплики работают; не будем писать в лог с уровнем Error.
			LOG_INFO(log, e.displayText());
		}
		else if (e.code() == ErrorCodes::ABORTED)
		{
			/// Прерванный мердж или скачивание куска - не ошибка.
			LOG_INFO(log, e.message());
		}
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
	entry->exception = saved_exception;
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
	return !was_exception;
}


void StorageReplicatedMergeTree::mergeSelectingThread()
{
	setThreadName("ReplMTMergeSel");

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

		/// Нельзя сливать куски, среди которых находится кусок, для которого неудовлетворён кворум.
		/// Замечание: теоретически, это можно было бы разрешить. Но это сделает логику более сложной.
		String quorum_node_value;
		if (zookeeper->tryGet(zookeeper_path + "/quorum/status", quorum_node_value))
		{
			ReplicatedMergeTreeQuorumEntry quorum_entry;
			quorum_entry.fromString(quorum_node_value);

			ActiveDataPartSet::Part part_info;
			ActiveDataPartSet::parsePartName(quorum_entry.part_name, part_info);

			if (part_info.left != part_info.right)
				throw Exception("Logical error: part written with quorum covers more than one block numbers", ErrorCodes::LOGICAL_ERROR);

			if (left->right <= part_info.left && right->left >= part_info.right)
				return false;
		}

		/// Можно слить куски, если все номера между ними заброшены - не соответствуют никаким блокам.
		/// Номера до RESERVED_BLOCK_NUMBERS всегда не соответствуют никаким блокам.
		for (Int64 number = std::max(RESERVED_BLOCK_NUMBERS, left->right + 1); number <= right->left - 1; ++number)
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
					LOG_TRACE(log, "Number of queued merges (" << merges_queued
						<< ") is greater than max_replicated_merges_in_queue ("
						<< data.settings.max_replicated_merges_in_queue << "), so won't select new parts to merge.");
					break;
				}

				MergeTreeData::DataPartsVector parts;

				String merged_name;

				size_t disk_space = DiskSpaceMonitor::getUnreservedFreeSpace(full_path);

				if (   !merger.selectPartsToMerge(parts, merged_name, disk_space, false, false, only_small, can_merge)
					&& !merger.selectPartsToMerge(parts, merged_name, disk_space, true, false, only_small, can_merge))
				{
					break;
				}

				bool all_in_zk = true;
				for (const auto & part : parts)
				{
					/// Если о каком-то из кусков нет информации в ZK, не будем сливать.
					if (!zookeeper->exists(replica_path + "/parts/" + part->name))
					{
						all_in_zk = false;

						if (part->modification_time + MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER < time(0))
						{
							LOG_WARNING(log, "Part " << part->name << " (that was selected for merge)"
								<< " with age " << (time(0) - part->modification_time)
								<< " seconds exists locally but not in ZooKeeper."
								<< " Won't do merge with that part and will check it.");
							enqueuePartForCheck(part->name);
						}
					}
				}
				if (!all_in_zk)
					break;

				LogEntry entry;
				entry.type = LogEntry::MERGE_PARTS;
				entry.source_replica = replica_name;
				entry.new_part_name = merged_name;
				entry.create_time = time(0);

				for (const auto & part : parts)
					entry.parts_to_merge.push_back(part->name);

				need_pull = true;

				zookeeper->create(zookeeper_path + "/log/log-", entry.toString(), zkutil::CreateMode::PersistentSequential);

				String month_name = parts[0]->name.substr(0, 6);
				for (size_t i = 0; i + 1 < parts.size(); ++i)
				{
					/// Уберем больше не нужные отметки о несуществующих блоках.
					for (Int64 number = std::max(RESERVED_BLOCK_NUMBERS, parts[i]->right + 1); number <= parts[i + 1]->left - 1; ++number)
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
	setThreadName("ReplMTAlter");

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
			auto columns_desc = ColumnsDescription<true>::parse(columns_str);

			auto & columns = columns_desc.columns;
			auto & materialized_columns = columns_desc.materialized;
			auto & alias_columns = columns_desc.alias;
			auto & column_defaults = columns_desc.defaults;

			bool changed_version = (stat.version != columns_version);

			{
				/// Если потребуется блокировать структуру таблицы, то приостановим мерджи.
				std::unique_ptr<MergeTreeMergeBlocker> merge_blocker;
				std::unique_ptr<MergeTreeMergeBlocker> unreplicated_merge_blocker;

				if (changed_version || force_recheck_parts)
				{
					merge_blocker = std::make_unique<MergeTreeMergeBlocker>(merger);
					if (unreplicated_merger)
						unreplicated_merge_blocker = std::make_unique<MergeTreeMergeBlocker>(*unreplicated_merger);
				}

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

				/// Важно, что уничтожается parts и merge_blocker перед wait-ом.
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

	LOG_DEBUG(log, "Alter thread finished");
}


void StorageReplicatedMergeTree::removePartFromZooKeeper(const String & part_name, zkutil::Ops & ops)
{
	String part_path = replica_path + "/parts/" + part_name;

	ops.push_back(new zkutil::Op::Remove(part_path + "/checksums", -1));
	ops.push_back(new zkutil::Op::Remove(part_path + "/columns", -1));
	ops.push_back(new zkutil::Op::Remove(part_path, -1));
}


void StorageReplicatedMergeTree::removePartAndEnqueueFetch(const String & part_name)
{
	auto zookeeper = getZooKeeper();

	String part_path = replica_path + "/parts/" + part_name;

	LogEntryPtr log_entry = new LogEntry;
	log_entry->type = LogEntry::GET_PART;
	log_entry->create_time = tryGetPartCreateTime(zookeeper, replica_path, part_name);
	log_entry->source_replica = "";
	log_entry->new_part_name = part_name;

	zkutil::Ops ops;
	ops.push_back(new zkutil::Op::Create(
		replica_path + "/queue/queue-", log_entry->toString(), zookeeper->getDefaultACL(),
		zkutil::CreateMode::PersistentSequential));

	removePartFromZooKeeper(part_name, ops);

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


void StorageReplicatedMergeTree::searchForMissingPart(const String & part_name)
{
	auto zookeeper = getZooKeeper();
	String part_path = replica_path + "/parts/" + part_name;

	/// Если кусок есть в ZooKeeper, удалим его оттуда и добавим в очередь задание скачать его.
	if (zookeeper->exists(part_path))
	{
		LOG_WARNING(log, "Checker: Part " << part_name << " exists in ZooKeeper but not locally. "
			"Removing from ZooKeeper and queueing a fetch.");
		ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

		removePartAndEnqueueFetch(part_name);
		return;
	}

	/// Если куска нет в ZooKeeper, проверим есть ли он хоть у кого-то.
	ActiveDataPartSet::Part part_info;
	ActiveDataPartSet::parsePartName(part_name, part_info);

	/** Логика такая:
		* - если у какой-то живой или неактивной реплики есть такой кусок, или покрывающий его кусок
		*   - всё Ок, ничего делать не нужно, он скачается затем при обработке очереди, когда реплика оживёт;
		*   - или, если реплика никогда не оживёт, то администратор удалит или создаст новую реплику с тем же адресом и см. всё сначала;
		* - если ни у кого нет такого или покрывающего его куска, то
		*   - если у кого-то есть все составляющие куски, то ничего делать не будем - это просто значит, что другие реплики ещё недоделали мердж
		*   - если ни у кого нет всех составляющих кусков, то признаем кусок навечно потерянным,
		*     и удалим запись из очереди репликации.
		*/

	LOG_WARNING(log, "Checker: Checking if anyone has part covering " << part_name << ".");

	bool found = false;

	size_t part_length_in_blocks = part_info.right + 1 - part_info.left;
	std::vector<char> found_blocks(part_length_in_blocks);

	Strings replicas = zookeeper->getChildren(zookeeper_path + "/replicas");
	for (const String & replica : replicas)
	{
		Strings parts = zookeeper->getChildren(zookeeper_path + "/replicas/" + replica + "/parts");
		for (const String & part_on_replica : parts)
		{
			if (part_on_replica == part_name || ActiveDataPartSet::contains(part_on_replica, part_name))
			{
				found = true;
				LOG_WARNING(log, "Checker: Found part " << part_on_replica << " on " << replica);
				break;
			}

			if (ActiveDataPartSet::contains(part_name, part_on_replica))
			{
				ActiveDataPartSet::Part part_on_replica_info;
				ActiveDataPartSet::parsePartName(part_on_replica, part_on_replica_info);

				for (auto block_num = part_on_replica_info.left; block_num <= part_on_replica_info.right; ++block_num)
					found_blocks.at(block_num - part_info.left) = 1;
			}
		}
		if (found)
			break;
	}

	if (found)
	{
		/// На какой-то живой или мёртвой реплике есть нужный кусок или покрывающий его.
		return;
	}

	size_t num_found_blocks = 0;
	for (auto found_block : found_blocks)
		num_found_blocks += (found_block == 1);

	if (num_found_blocks == part_length_in_blocks)
	{
		/// На совокупности живых или мёртвых реплик есть все куски, из которых можно составить нужный кусок. Ничего делать не будем.
		LOG_WARNING(log, "Checker: Found all blocks for missing part. Will wait for them to be merged.");
		return;
	}

	/// Ни у кого нет такого куска.
	LOG_ERROR(log, "Checker: No replica has part covering " << part_name);

	if (num_found_blocks != 0)
		LOG_WARNING(log, "When looking for smaller parts, that is covered by " << part_name
			<< ", we found just " << num_found_blocks << " of " << part_length_in_blocks << " blocks.");

	ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

	/// Есть ли он в очереди репликации? Если есть - удалим, так как задачу невозможно обработать.
	bool was_in_queue = false;
	{
		std::lock_guard<std::mutex> lock(queue_mutex);

		for (LogEntries::iterator it = queue.begin(); it != queue.end();)
		{
			if ((*it)->new_part_name == part_name)
			{
				zookeeper->tryRemove(replica_path + "/queue/" + (*it)->znode_name);
				queue.erase(it++);
				was_in_queue = true;
			}
			else
				++it;
		}
	}

	if (!was_in_queue)
	{
		/// Куска не было в нашей очереди. С чего бы это?
		LOG_ERROR(log, "Checker: Missing part " << part_name << " is not in our queue.");
		return;
	}

	/** Такая ситуация возможна, если на всех репликах, где был кусок, он испортился.
		* Например, у реплики, которая только что его записала, отключили питание, и данные не записались из кеша на диск.
		*/
	LOG_ERROR(log, "Checker: Part " << part_name << " is lost forever.");
	ProfileEvents::increment(ProfileEvents::ReplicatedDataLoss);

	/** Нужно добавить отсутствующий кусок в block_numbers, чтобы он не мешал слияниям.
		* Вот только в сам block_numbers мы его добавить не можем - если так сделать,
		*  ZooKeeper зачем-то пропустит один номер для автоинкремента,
		*  и в номерах блоков все равно останется дырка.
		* Специально из-за этого приходится отдельно иметь nonincrement_block_numbers.
		*
		* Кстати, если мы здесь сдохнем, то слияния не будут делаться сквозь эти отсутствующие куски.
		*
		* А ещё, не будем добавлять, если:
		* - потребовалось бы создать слишком много (больше 1000) узлов;
		* - кусок является первым в партиции или был при-ATTACH-ен.
		* NOTE Возможно, добавить также условие, если запись в очереди очень старая.
		*/

	if (part_length_in_blocks > 1000)
	{
		LOG_ERROR(log, "Won't add nonincrement_block_numbers because part spans too much blocks (" << part_length_in_blocks << ")");
		return;
	}

	if (part_info.left <= RESERVED_BLOCK_NUMBERS)
	{
		LOG_ERROR(log, "Won't add nonincrement_block_numbers because part is one of first in partition");
		return;
	}

	const auto partition_str = part_name.substr(0, 6);
	for (auto i = part_info.left; i <= part_info.right; ++i)
	{
		zookeeper->createIfNotExists(zookeeper_path + "/nonincrement_block_numbers", "");
		zookeeper->createIfNotExists(zookeeper_path + "/nonincrement_block_numbers/" + partition_str, "");
		AbandonableLockInZooKeeper::createAbandonedIfNotExists(
			zookeeper_path + "/nonincrement_block_numbers/" + partition_str + "/block-" + padIndex(i),
			*zookeeper);
	}
}


void StorageReplicatedMergeTree::checkPart(const String & part_name)
{
	LOG_WARNING(log, "Checker: Checking part " << part_name);
	ProfileEvents::increment(ProfileEvents::ReplicatedPartChecks);

	auto part = data.getActiveContainingPart(part_name);

	/// Этого или покрывающего куска у нас нет.
	if (!part)
	{
		searchForMissingPart(part_name);
	}
	/// У нас есть этот кусок, и он активен. Будем проверять, нужен ли нам этот кусок и правильные ли у него данные.
	else if (part->name == part_name)
	{
		auto zookeeper = getZooKeeper();
		auto table_lock = lockStructure(false);

		/// Если кусок есть в ZooKeeper, сверим его данные с его чексуммами, а их с ZooKeeper.
		if (zookeeper->exists(replica_path + "/parts/" + part_name))
		{
			LOG_WARNING(log, "Checker: Checking data of part " << part_name << ".");

			try
			{
				auto zk_checksums = MergeTreeData::DataPart::Checksums::parse(
					zookeeper->get(replica_path + "/parts/" + part_name + "/checksums"));
				zk_checksums.checkEqual(part->checksums, true);

				auto zk_columns = NamesAndTypesList::parse(
					zookeeper->get(replica_path + "/parts/" + part_name + "/columns"));
				if (part->columns != zk_columns)
					throw Exception("Columns of local part " + part_name + " are different from ZooKeeper");

				MergeTreePartChecker::Settings settings;
				settings.setIndexGranularity(data.index_granularity);
				settings.setRequireChecksums(true);
				settings.setRequireColumnFiles(true);
				MergeTreePartChecker::checkDataPart(
					data.getFullPath() + part_name, settings, data.primary_key_data_types);

				LOG_INFO(log, "Checker: Part " << part_name << " looks good.");
			}
			catch (...)
			{
				tryLogCurrentException(__PRETTY_FUNCTION__);

				LOG_ERROR(log, "Checker: Part " << part_name << " looks broken. Removing it and queueing a fetch.");
				ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

				removePartAndEnqueueFetch(part_name);

				/// Удалим кусок локально.
				data.renameAndDetachPart(part, "broken_");
			}
		}
		else if (part->modification_time + MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER < time(0))
		{
			/// Если куска нет в ZooKeeper, удалим его локально.
			/// Возможно, кусок кто-то только что записал, и еще не успел добавить в ZK.
			/// Поэтому удаляем только если кусок старый (не очень надежно).
			ProfileEvents::increment(ProfileEvents::ReplicatedPartChecksFailed);

			LOG_ERROR(log, "Checker: Unexpected part " << part_name << " in filesystem. Removing.");
			data.renameAndDetachPart(part, "unexpected_");
		}
		else
		{
			LOG_TRACE(log, "Checker: Young part " << part_name
				<< " with age " << (time(0) - part->modification_time)
				<< " seconds hasn't been added to ZooKeeper yet. It's ok.");
		}
	}
	else
	{
		/// Если у нас есть покрывающий кусок, игнорируем все проблемы с этим куском.
		/// В худшем случае в лог еще old_parts_lifetime секунд будут валиться ошибки, пока кусок не удалится как старый.
		LOG_WARNING(log, "Checker: We have part " << part->name << " covering part " << part_name);
	}
}


void StorageReplicatedMergeTree::partCheckThread()
{
	setThreadName("ReplMTPartCheck");

	while (!shutdown_called)
	{
		try
		{
			/// Достанем из очереди кусок для проверки.
			String part_name;
			{
				std::lock_guard<std::mutex> lock(parts_to_check_mutex);
				if (parts_to_check_queue.empty())
				{
					if (!parts_to_check_set.empty())
					{
						LOG_ERROR(log, "Checker: Non-empty parts_to_check_set with empty parts_to_check_queue. This is a bug.");
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

			checkPart(part_name);

			/// Удалим кусок из очереди проверок.
			{
				std::lock_guard<std::mutex> lock(parts_to_check_mutex);
				if (parts_to_check_queue.empty() || parts_to_check_queue.front() != part_name)
				{
					LOG_ERROR(log, "Checker: Someone changed parts_to_check_queue.front(). This is a bug.");
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

		/// Конечно, реплика может перестать быть активной или даже перестать существовать после возврата из этой функции.
	}

	return "";
}


/** Если для куска отслеживается кворум, то обновить информацию о нём в ZK.
  */
void StorageReplicatedMergeTree::updateQuorum(const String & part_name)
{
	auto zookeeper = getZooKeeper();

	/// Информация, на какие реплики был добавлен кусок, если кворум ещё не достигнут.
	const String quorum_status_path = zookeeper_path + "/quorum/status";
	/// Имя предыдущего куска, для которого был достигнут кворум.
	const String quorum_last_part_path = zookeeper_path + "/quorum/last_part";

	String value;
	zkutil::Stat stat;

	/// Если узла нет, значит по всем кворумным INSERT-ам уже был достигнут кворум, и ничего делать не нужно.
	while (zookeeper->tryGet(quorum_status_path, value, &stat))
	{
		ReplicatedMergeTreeQuorumEntry quorum_entry;
		quorum_entry.fromString(value);

		if (quorum_entry.part_name != part_name)
		{
			/// Кворум уже был достигнут. Более того, уже начался другой INSERT с кворумом.
			break;
		}

		quorum_entry.replicas.insert(replica_name);

		if (quorum_entry.replicas.size() >= quorum_entry.required_number_of_replicas)
		{
			/// Кворум достигнут. Удаляем узел, а также обновляем информацию о последнем куске, который был успешно записан с кворумом.

			zkutil::Ops ops;
			ops.push_back(new zkutil::Op::Remove(quorum_status_path, stat.version));
			ops.push_back(new zkutil::Op::SetData(quorum_last_part_path, part_name, -1));
			auto code = zookeeper->tryMulti(ops);

			if (code == ZOK)
			{
				break;
			}
			else if (code == ZNONODE)
			{
				/// Кворум уже был достигнут.
				break;
			}
			else if (code == ZBADVERSION)
			{
				/// Узел успели обновить. Надо заново его прочитать и повторить все действия.
				continue;
			}
			else
				throw zkutil::KeeperException(code, quorum_status_path);
		}
		else
		{
			/// Обновляем узел, прописывая туда на одну реплику больше.
			auto code = zookeeper->trySet(quorum_status_path, quorum_entry.toString(), stat.version);

			if (code == ZOK)
			{
				break;
			}
			else if (code == ZNONODE)
			{
				/// Кворум уже был достигнут.
				break;
			}
			else if (code == ZBADVERSION)
			{
				/// Узел успели обновить. Надо заново его прочитать и повторить все действия.
				continue;
			}
			else
				throw zkutil::KeeperException(code, quorum_status_path);
		}
	}
}


void StorageReplicatedMergeTree::fetchPart(const String & part_name, const String & replica_path, bool to_detached, size_t quorum)
{
	auto zookeeper = getZooKeeper();

	LOG_DEBUG(log, "Fetching part " << part_name << " from " << replica_path);

	TableStructureReadLockPtr table_lock;
	if (!to_detached)
		table_lock = lockStructure(true);

	ReplicatedMergeTreeAddress address(zookeeper->get(replica_path + "/host"));

	MergeTreeData::MutableDataPartPtr part = fetcher.fetchPart(part_name, replica_path, address.host, address.replication_port, to_detached);

	if (!to_detached)
	{
		zkutil::Ops ops;
		checkPartAndAddToZooKeeper(part, ops, part_name);

		MergeTreeData::Transaction transaction;
		auto removed_parts = data.renameTempPartAndReplace(part, nullptr, &transaction);

		zookeeper->multi(ops);
		transaction.commit();

		/** Если для этого куска отслеживается кворум, то надо его обновить.
		  * Если не успеем, в случае потери сессии, при перезапуске сервера - см. метод ReplicatedMergeTreeRestartingThread::updateQuorumIfWeHavePart.
		  */
		if (quorum)
			updateQuorum(part_name);

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

	LOG_DEBUG(log, "Fetched part " << part_name << " from " << replica_path << (to_detached ? " (to 'detached' directory)" : ""));
}


void StorageReplicatedMergeTree::shutdown()
{
	if (restarting_thread)
	{
		restarting_thread->stop();
		restarting_thread.reset();
	}

	endpoint_holder = nullptr;
	fetcher.cancel();
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
	/** У таблицы может быть два вида данных:
	  * - реплицируемые данные;
	  * - старые, нереплицируемые данные - они лежат отдельно и их целостность никак не контролируется.
	  * А ещё движок таблицы предоставляет возможность использовать "виртуальные столбцы".
	  * Один из них - _replicated позволяет определить, из какой части прочитаны данные,
	  *  или, при использовании в WHERE - выбрать данные только из одной части.
	  */

	Names virt_column_names;
	Names real_column_names;
	for (const auto & it : column_names)
		if (it == "_replicated")
			virt_column_names.push_back(it);
		else
			real_column_names.push_back(it);

	auto & select = typeid_cast<const ASTSelectQuery &>(*query);

	/// Try transferring some condition from WHERE to PREWHERE if enabled and viable
	if (settings.optimize_move_to_prewhere)
		if (select.where_expression && !select.prewhere_expression)
			MergeTreeWhereOptimizer{query, context, data, real_column_names, log};

	Block virtual_columns_block;
	ColumnUInt8 * column = new ColumnUInt8(2);
	ColumnPtr column_ptr = column;
	column->getData()[0] = 0;
	column->getData()[1] = 1;
	virtual_columns_block.insert(ColumnWithTypeAndName(column_ptr, new DataTypeUInt8, "_replicated"));

	/// Если запрошен столбец _replicated, пробуем индексировать.
	if (!virt_column_names.empty())
		VirtualColumnUtils::filterBlockWithQuery(query, virtual_columns_block, context);

	std::multiset<UInt8> values = VirtualColumnUtils::extractSingleValueFromBlock<UInt8>(virtual_columns_block, "_replicated");

	BlockInputStreams res;

	size_t part_index = 0;

	/** Настройки parallel_replica_offset и parallel_replicas_count позволяют читать с одной реплики одну часть данных, а с другой - другую.
	  * Для реплицируемых, данные разбиваются таким же механизмом, как работает секция SAMPLE.
	  * А для нереплицируемых данных, так как их целостность между репликами не контролируется,
	  *  с первой (settings.parallel_replica_offset == 0) реплики выбираются все данные, а с остальных - никакие.
	  */

	if ((settings.parallel_replica_offset == 0) && unreplicated_reader && values.count(0))
	{
		res = unreplicated_reader->read(real_column_names, query,
										context, settings, processed_stage,
										max_block_size, threads, &part_index, 0);

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
		/** Настройка select_sequential_consistency имеет два смысла:
		  * 1. Кидать исключение, если на реплике есть не все куски, которые были записаны на кворум остальных реплик.
		  * 2. Не читать куски, которые ещё не были записаны на кворум реплик.
		  * Для этого приходится синхронно сходить в ZooKeeper.
		  */
		Int64 max_block_number_to_read = 0;
		if (settings.select_sequential_consistency)
		{
			auto zookeeper = getZooKeeper();
			String last_part;
			zookeeper->tryGet(zookeeper_path + "/quorum/last_part", last_part);

			if (!last_part.empty() && !data.getPartIfExists(last_part))	/// TODO Отключение реплики при распределённых запросах.
				throw Exception("Replica doesn't have part " + last_part + " which was successfully written to quorum of other replicas."
					" Send query to another replica or disable 'select_sequential_consistency' setting.", ErrorCodes::REPLICA_IS_NOT_IN_QUORUM);

			if (last_part.empty())	/// Если ещё ни один кусок не был записан с кворумом.
			{
				String quorum_str;
				if (zookeeper->tryGet(zookeeper_path + "/quorum/status", quorum_str))
				{
					ReplicatedMergeTreeQuorumEntry quorum_entry;
					quorum_entry.fromString(quorum_str);
					ActiveDataPartSet::Part part_info;
					ActiveDataPartSet::parsePartName(quorum_entry.part_name, part_info);
					max_block_number_to_read = part_info.left - 1;
				}
			}
			else
			{
				ActiveDataPartSet::Part part_info;
				ActiveDataPartSet::parsePartName(last_part, part_info);
				max_block_number_to_read = part_info.right;
			}
		}

		auto res2 = reader.read(
			real_column_names, query, context, settings, processed_stage, max_block_size, threads, &part_index, max_block_number_to_read);

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


BlockOutputStreamPtr StorageReplicatedMergeTree::write(ASTPtr query, const Settings & settings)
{
	if (is_readonly)
		throw Exception("Table is in readonly mode", ErrorCodes::TABLE_IS_READ_ONLY);

	String insert_id;
	if (query)
		if (ASTInsertQuery * insert = typeid_cast<ASTInsertQuery *>(&*query))
			insert_id = insert->insert_id;

	return new ReplicatedMergeTreeBlockOutputStream(*this, insert_id, settings.insert_quorum);
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
	const auto & lut = DateLUT::instance();
	time_t start_time = lut.YYYYMMDDToDate(parse<UInt32>(month_name + "01"));
	DayNum_t left_date = lut.toDayNum(start_time);
	DayNum_t right_date = DayNum_t(static_cast<size_t>(left_date) + lut.daysInMonth(start_time) - 1);

	/// Уровень - right-left+1: кусок не мог образоваться в результате такого или большего количества слияний.
	return ActiveDataPartSet::getPartName(left_date, right_date, left, right, right - left + 1);
}


void StorageReplicatedMergeTree::dropUnreplicatedPartition(const Field & partition, const bool detach, const Settings & settings)
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
		if (part->month != month)
			continue;

		LOG_DEBUG(log, "Removing unreplicated part " << part->name);
		++removed_parts;

		if (detach)
			unreplicated_data->renameAndDetachPart(part, "");
		else
			unreplicated_data->replaceParts({part}, {}, false);
	}

	LOG_INFO(log, (detach ? "Detached " : "Removed ") << removed_parts << " unreplicated parts inside " << apply_visitor(FieldVisitorToString(), partition) << ".");
}


void StorageReplicatedMergeTree::dropPartition(ASTPtr query, const Field & field, bool detach, bool unreplicated, const Settings & settings)
{
	if (unreplicated)
	{
		dropUnreplicatedPartition(field, detach, settings);
		return;
	}

	auto zookeeper = getZooKeeper();
	String month_name = MergeTreeData::getMonthName(field);

	if (!is_leader_node)
	{
		/// Проксируем запрос в лидера.

		auto live_replicas = zookeeper->getChildren(zookeeper_path + "/leader_election");
		if (live_replicas.empty())
			throw Exception("No active replicas", ErrorCodes::NO_ACTIVE_REPLICAS);

		std::sort(live_replicas.begin(), live_replicas.end());
		const auto leader = zookeeper->get(zookeeper_path + "/leader_election/" + live_replicas.front());

		if (leader == replica_name)
			throw Exception("Leader was suddenly changed or logical error.", ErrorCodes::LEADERSHIP_CHANGED);

		ReplicatedMergeTreeAddress leader_address(zookeeper->get(zookeeper_path + "/replicas/" + leader + "/host"));

		auto new_query = query->clone();
		auto & alter = typeid_cast<ASTAlterQuery &>(*new_query);

		alter.database = leader_address.database;
		alter.table = leader_address.table;

		/// NOTE Работает только если есть доступ от пользователя default без пароля. Можно исправить с помощью добавления параметра в конфиг сервера.

		Connection connection(
			leader_address.host,
			leader_address.queries_port,
			leader_address.database,
			"", "", "ClickHouse replica");

		RemoteBlockInputStream stream(connection, formattedAST(new_query), &settings);
		NullBlockOutputStream output;

		copyData(stream, output);
		return;
	}

	/** Пропустим один номер в block_numbers для удаляемого месяца, и будем удалять только куски до этого номера.
	  * Это запретит мерджи удаляемых кусков с новыми вставляемыми данными.
	  * Инвариант: в логе не появятся слияния удаляемых кусков с другими кусками.
	  * NOTE: Если понадобится аналогично поддержать запрос DROP PART, для него придется придумать какой-нибудь новый механизм,
	  *        чтобы гарантировать этот инвариант.
	  */
	Int64 right;

	{
		AbandonableLockInZooKeeper block_number_lock = allocateBlockNumber(month_name);
		right = block_number_lock.getNumber();
		block_number_lock.unlock();
	}

	/// Такого никогда не должно происходить.
	if (right == 0)
		throw Exception("Logical error: just allocated block number is zero", ErrorCodes::LOGICAL_ERROR);
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
	entry.create_time = time(0);

	/// Если надо - дожидаемся выполнения операции на себе или на всех репликах.
	if (settings.replication_alter_partitions_sync != 0)
	{
		if (settings.replication_alter_partitions_sync == 1)
			waitForReplicaToProcessLogEntry(replica_name, entry);
		else
			waitForAllReplicasToProcessLogEntry(entry);
	}
}


void StorageReplicatedMergeTree::attachPartition(ASTPtr query, const Field & field, bool unreplicated, bool attach_part, const Settings & settings)
{
	auto zookeeper = getZooKeeper();
	String partition;

	if (attach_part)
		partition = field.safeGet<String>();
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
	Int64 min_used_number = RESERVED_BLOCK_NUMBERS;
	DayNum_t month = DateLUT::instance().makeDayNum(parse<UInt16>(partition.substr(0, 4)), parse<UInt8>(partition.substr(4, 2)), 0);

	{
		auto existing_parts = data.getDataParts();
		for (const auto & part : existing_parts)
			if (part->month == month)
				min_used_number = std::min(min_used_number, part->left);
	}

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
		entry.create_time = time(0);

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

	shutdown();

	auto zookeeper = getZooKeeper();

	if (zookeeper->expired())
		throw Exception("Table was not dropped because ZooKeeper session has been expired.", ErrorCodes::TABLE_WAS_NOT_DROPPED);

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

	String entry_str = entry.toString();
	String log_node_name;

	/** В эту функцию могут передать entry двух видов:
	  * 1. (более часто) Из директории log - общего лога, откуда реплики копируют записи в свою queue.
	  * 2. Из директории queue одной из реплик.
	  *
	  * Проблема в том, что номера (sequential нод) элементов очереди в log и в queue не совпадают.
	  * (И в queue не совпадают номера у одного и того же элемента лога для разных реплик.)
	  *
	  * Поэтому следует рассматривать эти случаи по-отдельности.
	  */

	/** Первое - нужно дождаться, пока реплика возьмёт к себе в queue элемент очереди из log,
	  *  если она ещё этого не сделала (см. функцию pullLogsToQueue).
	  *
	  * Для этого проверяем её узел log_pointer - максимальный номер взятого элемента из log плюс единица.
	  */

	if (0 == entry.znode_name.compare(0, strlen("log-"), "log-"))
	{
		/** В этом случае просто берём номер из имени ноды log-xxxxxxxxxx.
		  */

		UInt64 log_index = parse<UInt64>(entry.znode_name.substr(entry.znode_name.size() - 10));
		log_node_name = entry.znode_name;

		LOG_DEBUG(log, "Waiting for " << replica << " to pull " << log_node_name << " to queue");

		/// Дождемся, пока запись попадет в очередь реплики.
		while (true)
		{
			zkutil::EventPtr event = new Poco::Event;

			String log_pointer = zookeeper->get(zookeeper_path + "/replicas/" + replica + "/log_pointer", nullptr, event);
			if (!log_pointer.empty() && parse<UInt64>(log_pointer) > log_index)
				break;

			event->wait();
		}
	}
	else if (0 == entry.znode_name.compare(0, strlen("queue-"), "queue-"))
	{
		/** В этом случае номер log-ноды неизвестен. Нужно просмотреть все от log_pointer до конца,
		  *  ища ноду с таким же содержимым. И если мы её не найдём - значит реплика уже взяла эту запись в свою queue.
		  */

		String log_pointer = zookeeper->get(zookeeper_path + "/replicas/" + replica + "/log_pointer");

		Strings log_entries = zookeeper->getChildren(zookeeper_path + "/log");
		UInt64 log_index = 0;
		bool found = false;

		for (const String & log_entry_name : log_entries)
		{
			log_index = parse<UInt64>(log_entry_name.substr(log_entry_name.size() - 10));

			if (!log_pointer.empty() && log_index < parse<UInt64>(log_pointer))
				continue;

			String log_entry_str;
			bool exists = zookeeper->tryGet(zookeeper_path + "/log/" + log_entry_name, log_entry_str);
			if (exists && entry_str == log_entry_str)
			{
				found = true;
				log_node_name = log_entry_name;
				break;
			}
		}

		if (found)
		{
			LOG_DEBUG(log, "Waiting for " << replica << " to pull " << log_node_name << " to queue");

			/// Дождемся, пока запись попадет в очередь реплики.
			while (true)
			{
				zkutil::EventPtr event = new Poco::Event;

				String log_pointer = zookeeper->get(zookeeper_path + "/replicas/" + replica + "/log_pointer", nullptr, event);
				if (!log_pointer.empty() && parse<UInt64>(log_pointer) > log_index)
					break;

				event->wait();
			}
		}
	}
	else
		throw Exception("Logical error: unexpected name of log node: " + entry.znode_name, ErrorCodes::LOGICAL_ERROR);

	if (!log_node_name.empty())
		LOG_DEBUG(log, "Looking for node corresponding to " << log_node_name << " in " << replica << " queue");
	else
		LOG_DEBUG(log, "Looking for corresponding node in " << replica << " queue");

	/** Второе - найдем соответствующую запись в очереди указанной реплики (replica).
	  * Её номер может не совпадать ни с log-узлом, ни с queue-узлом у текущей реплики (у нас).
	  * Поэтому, ищем путём сравнения содержимого.
	  */

	Strings queue_entries = zookeeper->getChildren(zookeeper_path + "/replicas/" + replica + "/queue");
	String queue_entry_to_wait_for;

	for (const String & entry_name : queue_entries)
	{
		String queue_entry_str;
		bool exists = zookeeper->tryGet(zookeeper_path + "/replicas/" + replica + "/queue/" + entry_name, queue_entry_str);
		if (exists && queue_entry_str == entry_str)
		{
			queue_entry_to_wait_for = entry_name;
			break;
		}
	}

	/// Пока искали запись, ее уже выполнили и удалили.
	if (queue_entry_to_wait_for.empty())
	{
		LOG_DEBUG(log, "No corresponding node found. Assuming it has been already processed.");
		return;
	}

	LOG_DEBUG(log, "Waiting for " << queue_entry_to_wait_for << " to disappear from " << replica << " queue");

	/// Третье - дождемся, пока запись исчезнет из очереди реплики.
	zookeeper->waitForDisappear(zookeeper_path + "/replicas/" + replica + "/queue/" + queue_entry_to_wait_for);
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
		res.last_queue_update = last_queue_update;

		res.inserts_in_queue = 0;
		res.merges_in_queue = 0;
		res.queue_oldest_time = 0;
		res.inserts_oldest_time = 0;
		res.merges_oldest_time = 0;

		for (const LogEntryPtr & entry : queue)
		{
			if (entry->create_time && (!res.queue_oldest_time || entry->create_time < res.queue_oldest_time))
				res.queue_oldest_time = entry->create_time;

			if (entry->type == LogEntry::GET_PART)
			{
				++res.inserts_in_queue;

				if (entry->create_time && (!res.inserts_oldest_time || entry->create_time < res.inserts_oldest_time))
				{
					res.inserts_oldest_time = entry->create_time;
					res.oldest_part_to_get = entry->new_part_name;
				}
			}

			if (entry->type == LogEntry::MERGE_PARTS)
			{
				++res.merges_in_queue;

				if (entry->create_time && (!res.merges_oldest_time || entry->create_time < res.merges_oldest_time))
				{
					res.merges_oldest_time = entry->create_time;
					res.oldest_part_to_merge_to = entry->new_part_name;
				}
			}
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


void StorageReplicatedMergeTree::getQueue(LogEntriesData & res, String & replica_name_)
{
	res.clear();
	replica_name_ = replica_name;

	std::lock_guard<std::mutex> lock(queue_mutex);
	res.reserve(queue.size());
	for (const auto & entry : queue)
		res.emplace_back(*entry);
}


void StorageReplicatedMergeTree::getReplicaDelays(time_t & out_absolute_delay, time_t & out_relative_delay) const
{
	if (!restarting_thread)
		throw Exception("Table was shutted down or is in readonly mode.", ErrorCodes::TABLE_IS_READ_ONLY);

	restarting_thread->getReplicaDelays(out_absolute_delay, out_relative_delay);
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
				fetchPart(part, best_replica_path, true, 0);
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
