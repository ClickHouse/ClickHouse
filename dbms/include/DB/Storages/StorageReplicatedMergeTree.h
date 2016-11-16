#pragma once

#include <ext/shared_ptr_helper.hpp>

#include <DB/Storages/IStorage.h>
#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Storages/MergeTree/MergeTreeDataMerger.h>
#include <DB/Storages/MergeTree/MergeTreeDataWriter.h>
#include <DB/Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeQueue.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeCleanupThread.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeRestartingThread.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreePartCheckThread.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeAlterThread.h>
#include <DB/Storages/MergeTree/AbandonableLockInZooKeeper.h>
#include <DB/Storages/MergeTree/BackgroundProcessingPool.h>
#include <DB/Storages/MergeTree/DataPartsExchange.h>
#include <DB/Storages/MergeTree/RemoteDiskSpaceMonitor.h>
#include <DB/Storages/MergeTree/ShardedPartitionUploader.h>
#include <DB/Storages/MergeTree/RemoteQueryExecutor.h>
#include <DB/Storages/MergeTree/RemotePartChecker.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <zkutil/ZooKeeper.h>
#include <zkutil/LeaderElection.h>


namespace DB
{

/** Движок, использующий merge-дерево (см. MergeTreeData) и реплицируемый через ZooKeeper.
  *
  * ZooKeeper используется для следующих вещей:
  * - структура таблицы (/metadata, /columns)
  * - лог действий с данными (/log/log-..., /replicas/replica_name/queue/queue-...);
  * - список реплик (/replicas), признак активности реплики (/replicas/replica_name/is_active), адреса реплик (/replicas/replica_name/host);
  * - выбор реплики-лидера (/leader_election) - это та реплика, которая назначает мерджи;
  * - набор кусков данных на каждой реплике (/replicas/replica_name/parts);
  * - список последних N блоков данных с чексуммами, для дедупликации (/blocks);
  * - список инкрементальных номеров блоков (/block_numbers), которые мы сейчас собираемся вставить,
  *   или которые были неиспользованы (/nonincremental_block_numbers)
  *   для обеспечения линейного порядка вставки данных и мерджа данных только по интервалам в этой последовательности;
  * - координация записей с кворумом (/quorum).
  */

/** У реплицируемых таблиц есть общий лог (/log/log-...).
  * Лог - последовательность записей (LogEntry) о том, что делать.
  * Каждая запись - это одно из:
  * - обычная вставка данных (GET),
  * - мердж (MERGE),
  * - чуть менее обычная вставка данных (ATTACH),
  * - удаление партиции (DROP).
  *
  * Каждая реплика копирует (queueUpdatingThread, pullLogsToQueue) записи из лога в свою очередь (/replicas/replica_name/queue/queue-...),
  *  и затем выполняет их (queueTask).
  * Не смотря на название "очередь", выполнение может переупорядочиваться, при необходимости (shouldExecuteLogEntry, executeLogEntry).
  * Кроме того, записи в очереди могут генерироваться самостоятельно (не из лога), в следующих случаях:
  * - при создании новой реплики, туда помещаются действия на GET с других реплик (createReplica);
  * - если кусок повреждён (removePartAndEnqueueFetch) или отсутствовал при проверке (при старте - checkParts, во время работы - searchForMissingPart),
  *   туда помещаются действия на GET с других реплик;
  *
  * У реплики, на которую был сделан INSERT, в очереди тоже будет запись о GET этих данных.
  * Такая запись считается выполненной, как только обработчик очереди её увидит.
  *
  * У записи в логе есть время создания. Это время генерируется по часам на сервере, который создал запись
  * - того, на которых пришёл соответствующий запрос INSERT или ALTER.
  *
  * Для записей в очереди, которые реплика сделала для себя самостоятельно,
  * в качестве времени будет браться время создания соответствующего куска на какой-либо из реплик.
  */

class StorageReplicatedMergeTree : private ext::shared_ptr_helper<StorageReplicatedMergeTree>, public IStorage
{
friend class ext::shared_ptr_helper<StorageReplicatedMergeTree>;

public:
	/** Если !attach, либо создает новую таблицу в ZK, либо добавляет реплику в существующую таблицу.
	  */
	static StoragePtr create(
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
		const ASTPtr & sampling_expression_, /// nullptr, если семплирование не поддерживается.
		size_t index_granularity_,
		const MergeTreeData::MergingParams & merging_params_,
		bool has_force_restore_data_flag,
		const MergeTreeSettings & settings_);

	void shutdown() override;
	~StorageReplicatedMergeTree() override;

	std::string getName() const override
	{
		return "Replicated" + data.merging_params.getModeName() + "MergeTree";
	}

	std::string getTableName() const override { return table_name; }
	bool supportsSampling() const override { return data.supportsSampling(); }
	bool supportsFinal() const override { return data.supportsFinal(); }
	bool supportsPrewhere() const override { return data.supportsPrewhere(); }
	bool supportsParallelReplicas() const override { return true; }

	const NamesAndTypesList & getColumnsListImpl() const override { return data.getColumnsListNonMaterialized(); }

	NameAndTypePair getColumn(const String & column_name) const override
	{
		if (column_name == "_replicated") return NameAndTypePair("_replicated", std::make_shared<DataTypeUInt8>());
		return data.getColumn(column_name);
	}

	bool hasColumn(const String & column_name) const override
	{
		if (column_name == "_replicated") return true;
		return data.hasColumn(column_name);
	}

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Context & context,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1) override;

	BlockOutputStreamPtr write(ASTPtr query, const Settings & settings) override;

	bool optimize(const String & partition, bool final, const Settings & settings) override;

	void alter(const AlterCommands & params, const String & database_name, const String & table_name, const Context & context) override;

	void dropPartition(ASTPtr query, const Field & partition, bool detach, bool unreplicated, const Settings & settings) override;
	void attachPartition(ASTPtr query, const Field & partition, bool unreplicated, bool part, const Settings & settings) override;
	void fetchPartition(const Field & partition, const String & from, const Settings & settings) override;
	void freezePartition(const Field & partition, const String & with_name, const Settings & settings) override;

	void reshardPartitions(ASTPtr query, const String & database_name,
		const Field & first_partition, const Field & last_partition,
		const WeightedZooKeeperPaths & weighted_zookeeper_paths,
		const ASTPtr & sharding_key_expr, bool do_copy, const Field & coordinator,
		const Settings & settings) override;

	/** Удаляет реплику из ZooKeeper. Если других реплик нет, удаляет всю таблицу из ZooKeeper.
	  */
	void drop() override;

	void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

	bool supportsIndexForIn() const override { return true; }

	MergeTreeData & getData() { return data; }
	MergeTreeData * getUnreplicatedData() { return unreplicated_data.get(); }


	/** Для системной таблицы replicas. */
	struct Status
	{
		bool is_leader;
		bool is_readonly;
		bool is_session_expired;
		ReplicatedMergeTreeQueue::Status queue;
		UInt32 parts_to_check;
		String zookeeper_path;
		String replica_name;
		String replica_path;
		Int32 columns_version;
		UInt64 log_max_index;
		UInt64 log_pointer;
		UInt8 total_replicas;
		UInt8 active_replicas;
	};

	/// Получить статус таблицы. Если with_zk_fields = false - не заполнять поля, требующие запросов в ZK.
	void getStatus(Status & res, bool with_zk_fields = true);

	using LogEntriesData = std::vector<ReplicatedMergeTreeLogEntryData>;
	void getQueue(LogEntriesData & res, String & replica_name);

	void getReplicaDelays(time_t & out_absolute_delay, time_t & out_relative_delay);

	/// Добавить кусок в очередь кусков, чьи данные нужно проверить в фоновом потоке.
	void enqueuePartForCheck(const String & part_name, time_t delay_to_check_seconds = 0)
	{
		part_check_thread.enqueuePart(part_name, delay_to_check_seconds);
	}

private:
	void dropUnreplicatedPartition(const Field & partition, bool detach, const Settings & settings);

	friend class ReplicatedMergeTreeBlockOutputStream;
	friend class ReplicatedMergeTreeRestartingThread;
	friend class ReplicatedMergeTreePartCheckThread;
	friend class ReplicatedMergeTreeCleanupThread;
	friend class ReplicatedMergeTreeAlterThread;
	friend class ReplicatedMergeTreeRestartingThread;
	friend struct ReplicatedMergeTreeLogEntry;
	friend class ScopedPartitionMergeLock;

	friend class ReshardingWorker;
	friend class ShardedPartitionUploader::Client;
	friend class ShardedPartitionUploader::Service;

	using LogEntry = ReplicatedMergeTreeLogEntry;
	using LogEntryPtr = LogEntry::Ptr;

	Context & context;

	zkutil::ZooKeeperPtr current_zookeeper;		/// Используйте только с помощью методов ниже.
	std::mutex current_zookeeper_mutex;			/// Для пересоздания сессии в фоновом потоке.

	zkutil::ZooKeeperPtr tryGetZooKeeper();
	zkutil::ZooKeeperPtr getZooKeeper();
	void setZooKeeper(zkutil::ZooKeeperPtr zookeeper);

	/// Если true, таблица в офлайновом режиме, и в нее нельзя писать.
	bool is_readonly = false;

	String database_name;
	String table_name;
	String full_path;

	String zookeeper_path;
	String replica_name;
	String replica_path;

	/** Очередь того, что нужно сделать на этой реплике, чтобы всех догнать. Берется из ZooKeeper (/replicas/me/queue/).
	  * В ZK записи в хронологическом порядке. Здесь - не обязательно.
	  */
	ReplicatedMergeTreeQueue queue;

	/** /replicas/me/is_active.
	  */
	zkutil::EphemeralNodeHolderPtr replica_is_active_node;

	/** Версия ноды /columns в ZooKeeper, соответствующая текущим data.columns.
	  * Читать и изменять вместе с data.columns - под TableStructureLock.
	  */
	int columns_version = -1;

	/** Является ли эта реплика "ведущей". Ведущая реплика выбирает куски для слияния.
	  */
	bool is_leader_node = false;
	std::mutex leader_node_mutex;

	InterserverIOEndpointHolderPtr endpoint_holder;
	InterserverIOEndpointHolderPtr disk_space_monitor_endpoint_holder;
	InterserverIOEndpointHolderPtr sharded_partition_uploader_endpoint_holder;
	InterserverIOEndpointHolderPtr remote_query_executor_endpoint_holder;
	InterserverIOEndpointHolderPtr remote_part_checker_endpoint_holder;

	MergeTreeData data;
	MergeTreeDataSelectExecutor reader;
	MergeTreeDataWriter writer;
	MergeTreeDataMerger merger;

	DataPartsExchange::Fetcher fetcher;
	RemoteDiskSpaceMonitor::Client disk_space_monitor_client;
	ShardedPartitionUploader::Client sharded_partition_uploader_client;
	RemoteQueryExecutor::Client remote_query_executor_client;
	RemotePartChecker::Client remote_part_checker_client;

	zkutil::LeaderElectionPtr leader_election;

	/// Для чтения данных из директории unreplicated.
	std::unique_ptr<MergeTreeData> unreplicated_data;
	std::unique_ptr<MergeTreeDataSelectExecutor> unreplicated_reader;
	std::unique_ptr<MergeTreeDataMerger> unreplicated_merger;
	std::mutex unreplicated_mutex; /// Для мерджей и удаления нереплицируемых кусков.

	/// Нужно ли завершить фоновые потоки (кроме restarting_thread).
	std::atomic<bool> shutdown_called {false};
	Poco::Event shutdown_event;

	/// Потоки:

	/// Поток, следящий за обновлениями в логах всех реплик и загружающий их в очередь.
	std::thread queue_updating_thread;
	zkutil::EventPtr queue_updating_event = std::make_shared<Poco::Event>();

	/// Задание, выполняющее действия из очереди.
	BackgroundProcessingPool::TaskHandle queue_task_handle;

	/// Поток, выбирающий куски для слияния.
	std::thread merge_selecting_thread;
	Poco::Event merge_selecting_event;
	std::mutex merge_selecting_mutex; /// Берется на каждую итерацию выбора кусков для слияния.

	/// Поток, удаляющий старые куски, записи в логе и блоки.
	std::unique_ptr<ReplicatedMergeTreeCleanupThread> cleanup_thread;

	/// Поток, обрабатывающий переподключение к ZooKeeper при истечении сессии.
	std::unique_ptr<ReplicatedMergeTreeRestartingThread> restarting_thread;

	/// Поток, следящий за изменениями списка столбцов в ZooKeeper и обновляющий куски в соответствии с этими изменениями.
	std::unique_ptr<ReplicatedMergeTreeAlterThread> alter_thread;

	/// Поток, проверяющий данные кусков, а также очередь кусков для проверки.
	ReplicatedMergeTreePartCheckThread part_check_thread;

	/// Событие, пробуждающее метод alter от ожидания завершения запроса ALTER.
	zkutil::EventPtr alter_query_event = std::make_shared<Poco::Event>();

	Logger * log;

	StorageReplicatedMergeTree(
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
		const MergeTreeData::MergingParams & merging_params_,
		bool has_force_restore_data_flag,
		const MergeTreeSettings & settings_);

	/// Инициализация.

	/** Создает минимальный набор нод в ZooKeeper.
	  */
	void createTableIfNotExists();

	/** Создает реплику в ZooKeeper и добавляет в очередь все, что нужно, чтобы догнать остальные реплики.
	  */
	void createReplica();

	/** Создать узлы в ZK, которые должны быть всегда, но которые могли не существовать при работе старых версий сервера.
	  */
	void createNewZooKeeperNodes();

	/** Проверить, что список столбцов и настройки таблицы совпадают с указанными в ZK (/metadata).
	  * Если нет - бросить исключение.
	  */
	void checkTableStructure(bool skip_sanity_checks, bool allow_alter);

	/** Проверить, что множество кусков соответствует тому, что в ZK (/replicas/me/parts/).
	  * Если каких-то кусков, описанных в ZK нет локально, бросить исключение.
	  * Если какие-то локальные куски не упоминаются в ZK, удалить их.
	  *  Но если таких слишком много, на всякий случай бросить исключение - скорее всего, это ошибка конфигурации.
	  */
	void checkParts(bool skip_sanity_checks);

	/** Проверить, что чексумма куска совпадает с чексуммой того же куска на какой-нибудь другой реплике.
	  * Если ни у кого нет такого куска, ничего не проверяет.
	  * Не очень надежно: если две реплики добавляют кусок почти одновременно, ни одной проверки не произойдет.
	  * Кладет в ops действия, добавляющие данные о куске в ZooKeeper.
	  * Вызывать под TableStructureLock.
	  */
	void checkPartAndAddToZooKeeper(const MergeTreeData::DataPartPtr & part, zkutil::Ops & ops, String name_override = "");

	/** Исходит из допущения, что такого куска ещё нигде нет (Это обеспечено, если номер куска выделен с помощью AbandonableLock).
	  * Кладет в ops действия, добавляющие данные о куске в ZooKeeper.
	  * Вызывать под TableStructureLock.
	  */
	void addNewPartToZooKeeper(const MergeTreeData::DataPartPtr & part, zkutil::Ops & ops, String name_override = "");

	/// Кладет в ops действия, удаляющие кусок из ZooKeeper.
	void removePartFromZooKeeper(const String & part_name, zkutil::Ops & ops);

	/// Убирает кусок из ZooKeeper и добавляет в очередь задание скачать его. Предполагается это делать с битыми кусками.
	void removePartAndEnqueueFetch(const String & part_name);

	/// Выполнение заданий из очереди.

	/** Копирует новые записи из логов всех реплик в очередь этой реплики.
	  * Если next_update_event != nullptr, вызовет это событие, когда в логе появятся новые записи.
	  */
	void pullLogsToQueue(zkutil::EventPtr next_update_event = nullptr);

	/** Выполнить действие из очереди. Бросает исключение, если что-то не так.
	  * Возвращает, получилось ли выполнить. Если не получилось, запись нужно положить в конец очереди.
	  */
	bool executeLogEntry(const LogEntry & entry);

	void executeDropRange(const LogEntry & entry);
	bool executeAttachPart(const LogEntry & entry); /// Возвращает false, если куска нет, и его нужно забрать с другой реплики.

	/** Обновляет очередь.
	  */
	void queueUpdatingThread();

	/** Выполняет действия из очереди.
	  */
	bool queueTask();

	/// Выбор кусков для слияния.

	void becomeLeader();

	/** Выбирает куски для слияния и записывает в лог.
	  */
	void mergeSelectingThread();

	using MemoizedPartsThatCouldBeMerged = std::set<std::pair<std::string, std::string>>;
	/// Можно ли мерджить куски в указанном диапазоне? memo - необязательный параметр.
	bool canMergeParts(
		const MergeTreeData::DataPartPtr & left,
		const MergeTreeData::DataPartPtr & right,
		MemoizedPartsThatCouldBeMerged * memo);

	/** Записать выбранные куски для слияния в лог,
	  * Вызывать при заблокированном merge_selecting_mutex.
	  * Возвращает false, если какого-то куска нет в ZK.
	  */
	bool createLogEntryToMergeParts(
		const MergeTreeData::DataPartsVector & parts,
		const String & merged_name,
		ReplicatedMergeTreeLogEntryData * out_log_entry = nullptr);

	/// Обмен кусками.

	/** Возвращает пустую строку, если куска ни у кого нет.
	  */
	String findReplicaHavingPart(const String & part_name, bool active);

	/** Find replica having specified part or any part that covers it.
	  * If active = true, consider only active replicas.
	  * If found, returns replica name and set 'out_covering_part_name' to name of found largest covering part.
	  * If not found, returns empty string.
	  */
	String findReplicaHavingCoveringPart(const String & part_name, bool active, String & out_covering_part_name);

	/** Скачать указанный кусок с указанной реплики.
	  * Если to_detached, то кусок помещается в директорию detached.
	  * Если quorum != 0, то обновляется узел для отслеживания кворума.
	  * Returns false if part is already fetching right now.
	  */
	bool fetchPart(const String & part_name, const String & replica_path, bool to_detached, size_t quorum);

	std::unordered_set<String> currently_fetching_parts;
	std::mutex currently_fetching_parts_mutex;

	/** При отслеживаемом кворуме - добавить реплику в кворум для куска.
	  */
	void updateQuorum(const String & part_name);

	AbandonableLockInZooKeeper allocateBlockNumber(const String & month_name);

	/** Дождаться, пока все реплики, включая эту, выполнят указанное действие из лога.
	  * Если одновременно с этим добавляются реплики, может не дождаться добавленную реплику.
	  */
	void waitForAllReplicasToProcessLogEntry(const ReplicatedMergeTreeLogEntryData & entry);

	/** Дождаться, пока указанная реплика выполнит указанное действие из лога.
	  */
	void waitForReplicaToProcessLogEntry(const String & replica_name, const ReplicatedMergeTreeLogEntryData & entry);

	/// Кинуть исключение, если таблица readonly.
	void assertNotReadonly() const;

	/** Получить блокировку, которая защищает заданную партицию от задачи слияния.
	  * Блокировка является рекурсивной.
	  */
	std::string acquirePartitionMergeLock(const std::string & partition_name);

	/** Заявить, что больше не ссылаемся на блокировку соответствующую заданной
	  * партиции. Если ссылок больше нет, блокировка уничтожается.
	  */
	void releasePartitionMergeLock(const std::string & partition_name);


	/// Проверить наличие узла в ZK. Если он есть - запомнить эту информацию, и затем сразу отвечать true.
	std::unordered_set<std::string> existing_nodes_cache;
	std::mutex existing_nodes_cache_mutex;
	bool existsNodeCached(const std::string & path);


	/// Перешардирование.
	struct ReplicaSpaceInfo
	{
		long double factor = 0.0;
		size_t available_size = 0;
	};

	using ReplicaToSpaceInfo = std::map<std::string, ReplicaSpaceInfo>;

	/** Проверяет, что структуры локальной и реплицируемых таблиц совпадают.
	  */
	void enforceShardsConsistency(const WeightedZooKeeperPaths & weighted_zookeeper_paths);

	/** Получить информацию о свободном месте на репликах + дополнительную информацию
	  * для функции checkSpaceForResharding.
	  */
	ReplicaToSpaceInfo gatherReplicaSpaceInfo(const WeightedZooKeeperPaths & weighted_zookeeper_paths);

	/** Проверяет, что имеется достаточно свободного места локально и на всех репликах.
	  */
	bool checkSpaceForResharding(const ReplicaToSpaceInfo & replica_to_space_info, size_t partition_size) const;
};


extern const Int64 RESERVED_BLOCK_NUMBERS;
extern const int MAX_AGE_OF_LOCAL_PART_THAT_WASNT_ADDED_TO_ZOOKEEPER;

}
