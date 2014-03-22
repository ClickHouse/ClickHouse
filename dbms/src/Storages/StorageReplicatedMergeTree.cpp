#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Parsers/formatAST.h>
#include <DB/IO/WriteBufferFromOStream.h>
#include <DB/IO/ReadBufferFromString.h>

namespace DB
{

void StorageReplicatedMergeTree::MyInterserverIOEndpoint::processQuery(const Poco::Net::HTMLForm & params, WriteBuffer & out)
{
	writeString("Hello. You requested part ", out);
	writeString(params.get("part"), out);
	writeString(".", out);
}


StorageReplicatedMergeTree::StorageReplicatedMergeTree(
	const String & zookeeper_path_,
	const String & replica_name_,
	bool attach,
	const String & path_, const String & name_, NamesAndTypesListPtr columns_,
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
	path(path_), name(name_), full_path(path + escapeForFileName(name) + '/'), zookeeper_path(zookeeper_path_),
	replica_name(replica_name_),
	data(	full_path, columns_, context_, primary_expr_ast_, date_column_name_, sampling_expression_,
			index_granularity_,mode_, sign_column_, settings_),
	reader(data), writer(data),
	log(&Logger::get("StorageReplicatedMergeTree")),
	shutdown_called(false)
{
	if (!zookeeper_path.empty() && *zookeeper_path.rbegin() == '/')
		zookeeper_path.erase(zookeeper_path.end() - 1);
	replica_path = zookeeper_path + "/replicas/" + replica_name;

	if (!attach)
	{
		if (!zookeeper.exists(zookeeper_path))
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

	String endpoint_name = "ReplicatedMergeTree:" + replica_path;
	InterserverIOEndpointPtr endpoint = new MyInterserverIOEndpoint(*this);
	endpoint_holder = new InterserverIOEndpointHolder(endpoint_name, endpoint, context.getInterserverIOHandler());

	activateReplica();
}

StoragePtr StorageReplicatedMergeTree::create(
	const String & zookeeper_path_,
	const String & replica_name_,
	bool attach,
	const String & path_, const String & name_, NamesAndTypesListPtr columns_,
	Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_,
	const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	MergeTreeData::Mode mode_,
	const String & sign_column_,
	const MergeTreeSettings & settings_)
{
	return (new StorageReplicatedMergeTree(zookeeper_path_, replica_name_, attach, path_, name_, columns_, context_, primary_expr_ast_,
		date_column_name_, sampling_expression_, index_granularity_, mode_, sign_column_, settings_))->thisPtr();
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
	zookeeper.create(zookeeper_path, "", zkutil::CreateMode::Persistent);

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

	zookeeper.create(zookeeper_path + "/metadata", metadata.str(), zkutil::CreateMode::Persistent);

	/// Создадим нужные "директории".
	zookeeper.create(zookeeper_path + "/replicas", "", zkutil::CreateMode::Persistent);
	zookeeper.create(zookeeper_path + "/blocks", "", zkutil::CreateMode::Persistent);
	zookeeper.create(zookeeper_path + "/block-numbers", "", zkutil::CreateMode::Persistent);
}

/** Проверить, что список столбцов и настройки таблицы совпадают с указанными в ZK (/metadata).
	* Если нет - бросить исключение.
	*/
void StorageReplicatedMergeTree::checkTableStructure()
{
	String metadata_str = zookeeper.get(zookeeper_path + "/metadata");
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
}

void StorageReplicatedMergeTree::createReplica()
{
	zookeeper.create(replica_path, "", zkutil::CreateMode::Persistent);
	zookeeper.create(replica_path + "/host", "", zkutil::CreateMode::Persistent);
	zookeeper.create(replica_path + "/log", "", zkutil::CreateMode::Persistent);
	zookeeper.create(replica_path + "/log_pointers", "", zkutil::CreateMode::Persistent);
	zookeeper.create(replica_path + "/queue", "", zkutil::CreateMode::Persistent);
	zookeeper.create(replica_path + "/parts", "", zkutil::CreateMode::Persistent);
}

void StorageReplicatedMergeTree::activateReplica()
{
	std::stringstream host;
	host << "host: " << context.getInterserverIOHost() << std::endl;
	host << "port: " << context.getInterserverIOPort() << std::endl;

	/// Одновременно объявим, что эта реплика активна и обновим хост.
	zkutil::Ops ops;
	ops.push_back(new zkutil::Op::Create(replica_path + "/is_active", "", zookeeper.getDefaultACL(), zkutil::CreateMode::Ephemeral));
	ops.push_back(new zkutil::Op::SetData(replica_path + "/host", host.str(), -1));
	zookeeper.multi(ops);

	replica_is_active_node = zkutil::EphemeralNodeHolder::existing(replica_path + "/is_active", zookeeper);
}

bool StorageReplicatedMergeTree::isTableEmpty()
{
	Strings replicas = zookeeper.getChildren(zookeeper_path + "/replicas");
	for (const auto & replica : replicas)
	{
		if (!zookeeper.getChildren(zookeeper_path + "/replicas/" + replica + "/parts").empty())
			return false;
	}
	return true;
}

void StorageReplicatedMergeTree::checkParts() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

void StorageReplicatedMergeTree::loadQueue() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

void StorageReplicatedMergeTree::pullLogsToQueue() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

void StorageReplicatedMergeTree::optimizeQueue() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

void StorageReplicatedMergeTree::executeSomeQueueEntry() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

bool StorageReplicatedMergeTree::tryExecute(const LogEntry & entry) { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

String StorageReplicatedMergeTree::findReplicaHavingPart(const String & part_name) { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }
void StorageReplicatedMergeTree::getPart(const String & name, const String & replica_name) { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

void StorageReplicatedMergeTree::shutdown()
{
	if (shutdown_called)
		return;
	shutdown_called = true;
	replica_is_active_node = nullptr;
	endpoint_holder = nullptr;

	/// Кажется, чтобы был невозможен дедлок, тут придется дождаться удаления MyInterserverIOEndpoint.
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
	return reader.read(column_names, query, settings, processed_stage, max_block_size, threads);
}

BlockOutputStreamPtr StorageReplicatedMergeTree::write(ASTPtr query) { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

void StorageReplicatedMergeTree::drop()
{
	replica_is_active_node = nullptr;
	zookeeper.removeRecursive(replica_path);
	if (zookeeper.getChildren(zookeeper_path + "/replicas").empty())
		zookeeper.removeRecursive(zookeeper_path);
}

}
