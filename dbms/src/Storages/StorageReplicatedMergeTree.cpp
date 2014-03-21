#include <DB/Storages/StorageReplicatedMergeTree.h>

namespace DB
{

StorageReplicatedMergeTree::StorageReplicatedMergeTree(
	const String & zookeeper_path_,
	const String & replica_name_,
	bool attach,
	const String & path_, const String & name_, NamesAndTypesListPtr columns_,
	const Context & context_,
	ASTPtr & primary_expr_ast_,
	const String & date_column_name_,
	const ASTPtr & sampling_expression_,
	size_t index_granularity_,
	MergeTreeData::Mode mode_,
	const String & sign_column_,
	const MergeTreeSettings & settings_)
	:
	path(path_), name(name_), full_path(path + escapeForFileName(name) + '/'), zookeeper_path(zookeeper_path_),
	replica_name(replica_name_),
	data(	full_path, columns_, context_, primary_expr_ast_, date_column_name_, sampling_expression_,
			index_granularity_,mode_, sign_column_, settings_),
	reader(data), writer(data),
	zookeeper(context_.getZooKeeper()), log(&Logger::get("StorageReplicatedMergeTree")),
	shutdown_called(false)
{
	if (zookeeper_path.empty() || *zookeeper_path.rbegin() != '/')
		zookeeper_path += '/';

	if (!attach)
	{
		if (isTableExistsInZooKeeper())
		{
			if (!isTableEmptyInZooKeeper())
				throw Exception("Can't add new replica to non-empty table", ErrorCodes::ADDING_REPLICA_TO_NON_EMPTY_TABLE);
			checkTableStructure();
			createNewReplicaInZooKeeper();
		}
		else
		{
			createNewTableInZooKeeper();
		}
	}
	else
	{
		checkTableStructure();
		checkParts();
	}
}

StoragePtr StorageReplicatedMergeTree::create(
	const String & zookeeper_path_,
	const String & replica_name_,
	bool attach,
	const String & path_, const String & name_, NamesAndTypesListPtr columns_,
	const Context & context_,
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

void StorageReplicatedMergeTree::shutdown() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

StorageReplicatedMergeTree::~StorageReplicatedMergeTree()
{

}

BlockInputStreams StorageReplicatedMergeTree::read(
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size,
		unsigned threads) { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

BlockOutputStreamPtr StorageReplicatedMergeTree::write(ASTPtr query) { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

/** Удаляет реплику из ZooKeeper. Если других реплик нет, удаляет всю таблицу из ZooKeeper.
	*/
void StorageReplicatedMergeTree::drop() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

bool StorageReplicatedMergeTree::isTableExistsInZooKeeper() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }
bool StorageReplicatedMergeTree::isTableEmptyInZooKeeper() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

void StorageReplicatedMergeTree::createNewTableInZooKeeper() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }
void StorageReplicatedMergeTree::createNewReplicaInZooKeeper() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

void StorageReplicatedMergeTree::removeReplicaInZooKeeper() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }
void StorageReplicatedMergeTree::removeTableInZooKeeper() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

/** Проверить, что список столбцов и настройки таблицы совпадают с указанными в ZK (/metadata).
	* Если нет - бросить исключение.
	*/
void StorageReplicatedMergeTree::checkTableStructure() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

/** Проверить, что множество кусков соответствует тому, что в ZK (/replicas/me/parts/).
	* Если каких-то кусков, описанных в ZK нет локально, бросить исключение.
	* Если какие-то локальные куски не упоминаются в ZK, удалить их.
	*  Но если таких слишком много, на всякий случай бросить исключение - скорее всего, это ошибка конфигурации.
	*/
void StorageReplicatedMergeTree::checkParts() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

/// Работа с очередью и логом.

/** Кладет в queue записи из ZooKeeper (/replicas/me/queue/).
	*/
void StorageReplicatedMergeTree::loadQueue() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

/** Копирует новые записи из логов всех реплик в очередь этой реплики.
	*/
void StorageReplicatedMergeTree::pullLogsToQueue() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

/** Делает преобразования над очередью:
	*  - Если есть MERGE_PARTS кусков, не все из которых у нас есть, заменяем его на GET_PART и
	*    убираем GET_PART для всех составляющих его кусков. NOTE: Наверно, это будет плохо работать. Придумать эвристики получше.
	*/
void StorageReplicatedMergeTree::optimizeQueue() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

/** По порядку пытается выполнить действия из очереди, пока не получится. Что получилось, выбрасывает из очереди.
	*/
void StorageReplicatedMergeTree::executeSomeQueueEntry() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

/** Попробовать выполнить действие из очереди. Возвращает false, если не получилось по какой-то ожидаемой причине:
	*  - GET_PART, и ни у кого нет этого куска. Это возможно, если этот кусок уже слили с кем-то и удалили.
	*  - Не смогли скачать у кого-то кусок, потому что его там уже нет.
	*  - Не смогли объединить куски, потому что не все из них у нас есть.
	*/
bool StorageReplicatedMergeTree::tryExecute(const LogEntry & entry) { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

/// Обмен кусками.

void StorageReplicatedMergeTree::registerEndpoint() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }
void StorageReplicatedMergeTree::unregisterEndpoint() { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

String StorageReplicatedMergeTree::findReplicaHavingPart(const String & part_name) { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }
void StorageReplicatedMergeTree::getPart(const String & name, const String & replica_name) { throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); }

}
