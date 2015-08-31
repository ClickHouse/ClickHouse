#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Storages/MergeTree/MergeTreeDataMerger.h>
#include <DB/Storages/MergeTree/MergeTreeDataWriter.h>
#include <DB/Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreePartsExchange.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeCleanupThread.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeRestartingThread.h>
#include <DB/Storages/MergeTree/AbandonableLockInZooKeeper.h>
#include <DB/Storages/MergeTree/BackgroundProcessingPool.h>
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <zkutil/ZooKeeper.h>
#include <zkutil/LeaderElection.h>


namespace DB
{

/** Движок, использующий merge-дерево и реплицируемый через ZooKeeper.
  */
class StorageReplicatedMergeTree : public IStorage
{
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
		MergeTreeData::Mode mode_,
		const String & sign_column_,		/// Для Collapsing режима.
		const Names & columns_to_sum_,		/// Для Summing режима.
		const MergeTreeSettings & settings_);

	void shutdown() override;
	~StorageReplicatedMergeTree() override;

	std::string getName() const override
	{
		return "Replicated" + data.getModePrefix() + "MergeTree";
	}

	std::string getTableName() const override { return table_name; }
	bool supportsSampling() const override { return data.supportsSampling(); }
	bool supportsFinal() const override { return data.supportsFinal(); }
	bool supportsPrewhere() const override { return data.supportsPrewhere(); }
	bool supportsParallelReplicas() const override { return true; }

	const NamesAndTypesList & getColumnsListImpl() const override { return data.getColumnsListNonMaterialized(); }

	NameAndTypePair getColumn(const String & column_name) const override
	{
		if (column_name == "_replicated") return NameAndTypePair("_replicated", new DataTypeUInt8);
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

	BlockOutputStreamPtr write(ASTPtr query) override;

	bool optimize(const Settings & settings) override;

	void alter(const AlterCommands & params, const String & database_name, const String & table_name, Context & context) override;

	void dropPartition(const Field & partition, bool detach, bool unreplicated, const Settings & settings) override;
	void attachPartition(const Field & partition, bool unreplicated, bool part, const Settings & settings) override;
	void fetchPartition(const Field & partition, const String & from, const Settings & settings) override;
	void freezePartition(const Field & partition, const Settings & settings) override;

	/** Удаляет реплику из ZooKeeper. Если других реплик нет, удаляет всю таблицу из ZooKeeper.
	  */
	void drop() override;

	void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name) override;

	bool supportsIndexForIn() const override { return true; }

	/// Добавить кусок в очередь кусков, чьи данные нужно проверить в фоновом потоке.
	void enqueuePartForCheck(const String & name);

	MergeTreeData & getData() { return data; }
	MergeTreeData * getUnreplicatedData() { return unreplicated_data.get(); }


	/** Для системной таблицы replicas. */
	struct Status
	{
		bool is_leader;
		bool is_readonly;
		bool is_session_expired;
		UInt32 future_parts;
		UInt32 parts_to_check;
		String zookeeper_path;
		String replica_name;
		String replica_path;
		Int32 columns_version;
		UInt32 queue_size;
		UInt32 inserts_in_queue;
		UInt32 merges_in_queue;
		UInt32 queue_oldest_time;
		UInt32 inserts_oldest_time;
		UInt32 merges_oldest_time;
		UInt64 log_max_index;
		UInt64 log_pointer;
		UInt8 total_replicas;
		UInt8 active_replicas;
	};

	/// Получить статус таблицы. Если with_zk_fields = false - не заполнять поля, требующие запросов в ZK.
	void getStatus(Status & res, bool with_zk_fields = true);

private:
	void dropUnreplicatedPartition(const Field & partition, bool detach, const Settings & settings);

	friend class ReplicatedMergeTreeBlockOutputStream;
	friend class ReplicatedMergeTreeRestartingThread;
	friend class ReplicatedMergeTreeCleanupThread;
	friend struct ReplicatedMergeTreeLogEntry;
	friend struct FuturePartTagger;

	typedef ReplicatedMergeTreeLogEntry LogEntry;
	typedef LogEntry::Ptr LogEntryPtr;

	typedef std::list<LogEntryPtr> LogEntries;

	typedef std::set<String> StringSet;
	typedef std::list<String> StringList;

	Context & context;

	zkutil::ZooKeeperPtr current_zookeeper;		/// Используйте только с помощью методов ниже.
	std::mutex current_zookeeper_mutex;			/// Для пересоздания сессии в фоновом потоке.

	zkutil::ZooKeeperPtr getZooKeeper()
	{
		std::lock_guard<std::mutex> lock(current_zookeeper_mutex);
		return current_zookeeper;
	}

	void setZooKeeper(zkutil::ZooKeeperPtr zookeeper)
	{
		std::lock_guard<std::mutex> lock(current_zookeeper_mutex);
		current_zookeeper = zookeeper;
	}

	/// Если true, таблица в офлайновом режиме, и в нее нельзя писать.
	bool is_readonly = false;

	/// Каким будет множество активных кусков после выполнения всей текущей очереди.
	ActiveDataPartSet virtual_parts;

	/** Очередь того, что нужно сделать на этой реплике, чтобы всех догнать. Берется из ZooKeeper (/replicas/me/queue/).
	  * В ZK записи в хронологическом порядке. Здесь - не обязательно.
	  */
	LogEntries queue;
	std::mutex queue_mutex;

	/** Куски, которые появятся в результате действий, выполняемых прямо сейчас фоновыми потоками (этих действий нет в очереди).
	  * Использовать под залоченным queue_mutex.
	  */
	StringSet future_parts;

	/** Куски, для которых нужно проверить одно из двух:
	  *  - Если кусок у нас есть, сверить, его данные с его контрольными суммами, а их с ZooKeeper.
	  *  - Если куска у нас нет, проверить, есть ли он (или покрывающий его кусок) хоть у кого-то.
	  */
	StringSet parts_to_check_set;
	StringList parts_to_check_queue;
	std::mutex parts_to_check_mutex;
	Poco::Event parts_to_check_event;

	String database_name;
	String table_name;
	String full_path;

	String zookeeper_path;
	String replica_name;
	String replica_path;

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

	InterserverIOEndpointHolderPtr endpoint_holder;

	MergeTreeData data;
	MergeTreeDataSelectExecutor reader;
	MergeTreeDataWriter writer;
	MergeTreeDataMerger merger;
	ReplicatedMergeTreePartsFetcher fetcher;
	zkutil::LeaderElectionPtr leader_election;

	/// Для чтения данных из директории unreplicated.
	std::unique_ptr<MergeTreeData> unreplicated_data;
	std::unique_ptr<MergeTreeDataSelectExecutor> unreplicated_reader;
	std::unique_ptr<MergeTreeDataMerger> unreplicated_merger;
	std::mutex unreplicated_mutex; /// Для мерджей и удаления нереплицируемых кусков.

	/// Нужно ли завершить фоновые потоки (кроме restarting_thread).
	volatile bool shutdown_called = false;
	Poco::Event shutdown_event;

	/// Потоки:

	/// Поток, следящий за обновлениями в логах всех реплик и загружающий их в очередь.
	std::thread queue_updating_thread;
	zkutil::EventPtr queue_updating_event = zkutil::EventPtr(new Poco::Event);

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
	std::thread alter_thread;
	zkutil::EventPtr alter_thread_event = zkutil::EventPtr(new Poco::Event);

	/// Поток, проверяющий данные кусков.
	std::thread part_check_thread;

	/// Событие, пробуждающее метод alter от ожидания завершения запроса ALTER.
	zkutil::EventPtr alter_query_event = zkutil::EventPtr(new Poco::Event);

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
		MergeTreeData::Mode mode_,
		const String & sign_column_,
		const Names & columns_to_sum_,
		const MergeTreeSettings & settings_);

	/// Инициализация.

	/** Создает минимальный набор нод в ZooKeeper.
	  */
	void createTableIfNotExists();

	/** Создает реплику в ZooKeeper и добавляет в очередь все, что нужно, чтобы догнать остальные реплики.
	  */
	void createReplica();

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

	/// Положить все куски из data в virtual_parts.
	void initVirtualParts();


	/** Проверить, что чексумма куска совпадает с чексуммой того же куска на какой-нибудь другой реплике.
	  * Если ни у кого нет такого куска, ничего не проверяет.
	  * Не очень надежно: если две реплики добавляют кусок почти одновременно, ни одной проверки не произойдет.
	  * Кладет в ops действия, добавляющие данные о куске в ZooKeeper.
	  * Вызывать под TableStructureLock.
	  */
	void checkPartAndAddToZooKeeper(const MergeTreeData::DataPartPtr & part, zkutil::Ops & ops, String name_override = "");

	/// Убирает кусок из ZooKeeper и добавляет в очередь задание скачать его. Предполагается это делать с битыми кусками.
	void removePartAndEnqueueFetch(const String & part_name);

	/// Выполнение заданий из очереди.

	/** Кладет в queue записи из ZooKeeper (/replicas/me/queue/).
	  */
	void loadQueue();

	/** Копирует новые записи из логов всех реплик в очередь этой реплики.
	  * Если next_update_event != nullptr, вызовет это событие, когда в логе появятся новые записи.
	  */
	void pullLogsToQueue(zkutil::EventPtr next_update_event = nullptr);

	/** Можно ли сейчас попробовать выполнить это действие. Если нет, нужно оставить его в очереди и попробовать выполнить другое.
	  * Вызывается под queue_mutex.
	  */
	bool shouldExecuteLogEntry(const LogEntry & entry);

	/** Выполнить действие из очереди. Бросает исключение, если что-то не так.
	  * Возвращает, получилось ли выполнить. Если не получилось, запись нужно положить в конец очереди.
	  */
	bool executeLogEntry(const LogEntry & entry, BackgroundProcessingPool::Context & pool_context);

	void executeDropRange(const LogEntry & entry);
	bool executeAttachPart(const LogEntry & entry); /// Возвращает false, если куска нет, и его нужно забрать с другой реплики.

	/** Обновляет очередь.
	  */
	void queueUpdatingThread();

	/** Выполняет действия из очереди.
	  */
	bool queueTask(BackgroundProcessingPool::Context & context);

	/// Выбор кусков для слияния.

	void becomeLeader();

	/** Выбирает куски для слияния и записывает в лог.
	  */
	void mergeSelectingThread();

	/** Делает локальный ALTER, когда список столбцов в ZooKeeper меняется.
	  */
	void alterThread();

	/** Проверяет целостность кусков.
	  */
	void partCheckThread();

	/// Обмен кусками.

	/** Возвращает пустую строку, если куска ни у кого нет.
	  */
	String findReplicaHavingPart(const String & part_name, bool active);

	/** Скачать указанный кусок с указанной реплики.
	  * Если to_detached, то кусок помещается в директорию detached.
	  */
	void fetchPart(const String & part_name, const String & replica_path, bool to_detached = false);

	AbandonableLockInZooKeeper allocateBlockNumber(const String & month_name);

	/** Дождаться, пока все реплики, включая эту, выполнят указанное действие из лога.
	  * Если одновременно с этим добавляются реплики, может не дождаться добавленную реплику.
	  */
	void waitForAllReplicasToProcessLogEntry(const LogEntry & entry);

	/** Дождаться, пока указанная реплика выполнит указанное действие из лога.
	  */
	void waitForReplicaToProcessLogEntry(const String & replica_name, const LogEntry & entry);

	/** Преобразовать число в строку формате суффиксов автоинкрементных нод в ZooKeeper.
	  * Поддерживаются также отрицательные числа - для них имя ноды выглядит несколько глупо
	  *  и не соответствует никакой автоинкрементной ноде в ZK.
	  */
	static String padIndex(Int64 index)
	{
		String index_str = toString(index);
		return std::string(10 - index_str.size(), '0') + index_str;
	}
};

}
