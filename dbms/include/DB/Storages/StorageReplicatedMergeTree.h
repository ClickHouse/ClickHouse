#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Storages/MergeTree/MergeTreeDataMerger.h>
#include <DB/Storages/MergeTree/MergeTreeDataWriter.h>
#include <DB/Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreePartsExchange.h>
#include "MergeTree/AbandonableLockInZooKeeper.h"
#include <DB/DataTypes/DataTypesNumberFixed.h>
#include <zkutil/ZooKeeper.h>
#include <zkutil/LeaderElection.h>
#include <statdaemons/threadpool.hpp>

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
		Context & context_,
		ASTPtr & primary_expr_ast_,
		const String & date_column_name_,
		const ASTPtr & sampling_expression_, /// nullptr, если семплирование не поддерживается.
		size_t index_granularity_,
		MergeTreeData::Mode mode_ = MergeTreeData::Ordinary,
		const String & sign_column_ = "",
		const MergeTreeSettings & settings_ = MergeTreeSettings());

	void shutdown() override;
	~StorageReplicatedMergeTree();

	std::string getName() const override
	{
		return "Replicated" + data.getModePrefix() + "MergeTree";
	}

	std::string getTableName() const override { return table_name; }
	bool supportsSampling() const override { return data.supportsSampling(); }
	bool supportsFinal() const override { return data.supportsFinal(); }
	bool supportsPrewhere() const override { return data.supportsPrewhere(); }

	const NamesAndTypesList & getColumnsList() const override { return data.getColumnsList(); }

	NameAndTypePair getColumn(const String &column_name) const
	{
		if (column_name == "_replicated") return NameAndTypePair("_replicated", new DataTypeUInt8);
		return data.getColumn(column_name);
	}

	bool hasColumn(const String &column_name) const
	{
		if (column_name == "_replicated") return true;
		return data.hasColumn(column_name);
	}

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1) override;

	BlockOutputStreamPtr write(ASTPtr query) override;

	bool optimize() override;

	void alter(const AlterCommands & params, const String & database_name, const String & table_name, Context & context) override;

	void dropPartition(const Field & partition, bool detach) override;
	void attachPartition(const Field & partition, bool unreplicated, bool part) override;

	/** Удаляет реплику из ZooKeeper. Если других реплик нет, удаляет всю таблицу из ZooKeeper.
	  */
	void drop() override;

	void rename(const String & new_path_to_db, const String & new_database_name, const String & new_table_name);

	bool supportsIndexForIn() const override { return true; }

	/// Добавить кусок в очередь кусков, чьи данные нужно проверить в фоновом потоке.
	void enqueuePartForCheck(const String & name);

	MergeTreeData & getData() { return data; }
	MergeTreeData * getUnreplicatedData() { return unreplicated_data.get(); }

private:
	friend class ReplicatedMergeTreeBlockOutputStream;

	/// Добавляет кусок в множество future_parts.
	struct FuturePartTagger
	{
		String part;
		StorageReplicatedMergeTree & storage;

		FuturePartTagger(const String & part_, StorageReplicatedMergeTree & storage_)
			: part(part_), storage(storage_)
		{
			if (!storage.future_parts.insert(part).second)
				throw Exception("Tagging already tagged future part " + part + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);
		}

		~FuturePartTagger()
		{
			try
			{
				std::unique_lock<std::mutex> lock(storage.queue_mutex);
				if (!storage.future_parts.erase(part))
					throw Exception("Untagging already untagged future part " + part + ". This is a bug.", ErrorCodes::LOGICAL_ERROR);
			}
			catch (...)
			{
				tryLogCurrentException(__PRETTY_FUNCTION__);
			}
		}
	};

	typedef Poco::SharedPtr<FuturePartTagger> FuturePartTaggerPtr;

	struct LogEntry
	{
		typedef Poco::SharedPtr<LogEntry> Ptr;

		enum Type
		{
			GET_PART,    /// Получить кусок с другой реплики.
			MERGE_PARTS, /// Слить куски.
			DROP_RANGE,  /// Удалить куски в указанном месяце в указанном диапазоне номеров.
			ATTACH_PART, /// Перенести кусок из директории detached или unreplicated.
		};

		String znode_name;

		Type type;
		String source_replica; /// Пустая строка значит, что эта запись была добавлена сразу в очередь, а не скопирована из лога.

		/// Имя куска, получающегося в результате.
		/// Для DROP_RANGE имя несуществующего куска. Нужно удалить все куски, покрытые им.
		String new_part_name;

		Strings parts_to_merge;

		/// Для DROP_RANGE, true значит, что куски нужно не удалить, а перенести в директорию detached.
		bool detach = false;

		/// Для ATTACH_PART имя куска в директории detached или unreplicated.
		String source_part_name;
		/// Нужно переносить из директории unreplicated, а не detached.
		bool attach_unreplicated;

		FuturePartTaggerPtr future_part_tagger;
		bool currently_executing = false; /// Доступ под queue_mutex.
		std::condition_variable execution_complete; /// Пробуждается когда currently_executing становится false.

		void addResultToVirtualParts(StorageReplicatedMergeTree & storage)
		{
			if (type == MERGE_PARTS || type == GET_PART || type == DROP_RANGE || type == ATTACH_PART)
				storage.virtual_parts.add(new_part_name);
		}

		void tagPartAsFuture(StorageReplicatedMergeTree & storage)
		{
			if (type == MERGE_PARTS || type == GET_PART || type == ATTACH_PART)
				future_part_tagger = new FuturePartTagger(new_part_name, storage);
		}

		void writeText(WriteBuffer & out) const;
		void readText(ReadBuffer & in);

		String toString() const
		{
			String s;
			{
				WriteBufferFromString out(s);
				writeText(out);
			}
			return s;
		}

		static Ptr parse(const String & s)
		{
			ReadBufferFromString in(s);
			Ptr res = new LogEntry;
			res->readText(in);
			assertEOF(in);
			return res;
		}
	};

	typedef LogEntry::Ptr LogEntryPtr;

	typedef std::list<LogEntryPtr> LogEntries;

	typedef std::set<String> StringSet;
	typedef std::list<String> StringList;

	Context & context;
	zkutil::ZooKeeperPtr zookeeper;

	/// Если true, таблица в офлайновом режиме, и в нее нельзя писать.
	bool is_read_only = false;

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
	Poco::FastMutex parts_to_check_mutex;
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

	/** Случайные данные, которые мы записали в /replicas/me/is_active.
	  */
	String active_node_identifier;

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
	Poco::FastMutex unreplicated_mutex; /// Для мерджей и удаления нереплицируемых кусков.

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
	std::thread cleanup_thread;

	/// Поток, обрабатывающий переподключение к ZooKeeper при истечении сессии (очень маловероятное событие).
	std::thread restarting_thread;
	Poco::Event restarting_event;

	/// Поток, следящий за изменениями списка столбцов в ZooKeeper и обновляющий куски в соответствии с этими изменениями.
	std::thread alter_thread;
	zkutil::EventPtr alter_thread_event = zkutil::EventPtr(new Poco::Event);

	/// Поток, проверяющий данные кусков.
	std::thread part_check_thread;

	/// Событие, пробуждающее метод alter от ожидания завершения запроса ALTER.
	zkutil::EventPtr alter_query_event = zkutil::EventPtr(new Poco::Event);

	Logger * log;

	/// Нужно ли завершить фоновые потоки (кроме restarting_thread).
	volatile bool shutdown_called = false;
	Poco::Event shutdown_event;
	/// Нужно ли завершить restarting_thread.
	volatile bool permanent_shutdown_called = false;

	StorageReplicatedMergeTree(
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
		MergeTreeData::Mode mode_ = MergeTreeData::Ordinary,
		const String & sign_column_ = "",
		const MergeTreeSettings & settings_ = MergeTreeSettings());

	/// Инициализация.

	/** Создает минимальный набор нод в ZooKeeper.
	  */
	void createTable();

	/** Создает реплику в ZooKeeper и добавляет в очередь все, что нужно, чтобы догнать остальные реплики.
	  */
	void createReplica();

	/** Отметить в ZooKeeper, что эта реплика сейчас активна.
	  */
	void activateReplica();

	/** Проверить, что список столбцов и настройки таблицы совпадают с указанными в ZK (/metadata).
	  * Если нет - бросить исключение.
	  */
	void checkTableStructure(bool skip_sanity_checks);

	/** Проверить, что множество кусков соответствует тому, что в ZK (/replicas/me/parts/).
	  * Если каких-то кусков, описанных в ZK нет локально, бросить исключение.
	  * Если какие-то локальные куски не упоминаются в ZK, удалить их.
	  *  Но если таких слишком много, на всякий случай бросить исключение - скорее всего, это ошибка конфигурации.
	  */
	void checkParts(bool skip_sanity_checks);

	/// Положить все куски из data в virtual_parts.
	void initVirtualParts();

	/// Запустить или остановить фоновые потоки. Используется для частичной переинициализации при пересоздании сессии в ZooKeeper.
	void startup();
	void partialShutdown();

	/// Запретить запись в таблицу и завершить все фоновые потоки.
	void goReadOnly();


	/** Проверить, что чексумма куска совпадает с чексуммой того же куска на какой-нибудь другой реплике.
	  * Если ни у кого нет такого куска, ничего не проверяет.
	  * Не очень надежно: если две реплики добавляют кусок почти одновременно, ни одной проверки не произойдет.
	  * Кладет в ops действия, добавляющие данные о куске в ZooKeeper.
	  * Вызывать под TableStructureLock.
	  */
	void checkPartAndAddToZooKeeper(MergeTreeData::DataPartPtr part, zkutil::Ops & ops, String name_override = "");

	/// Убирает кусок из ZooKeeper и добавляет в очередь задание скачать его. Предполагается это делать с битыми кусками.
	void removePartAndEnqueueFetch(const String & part_name);

	void clearOldParts();

	/// Удалить из ZooKeeper старые записи в логе.
	void clearOldLogs();

	/// Удалить из ZooKeeper старые хеши блоков. Это делает ведущая реплика.
	void clearOldBlocks();

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

	/** Удаляет устаревшие данные.
	  */
	void cleanupThread();

	/** Делает локальный ALTER, когда список столбцов в ZooKeeper меняется.
	  */
	void alterThread();

	/** Проверяет целостность кусков.
	  */
	void partCheckThread();

	/** Когда сессия в ZooKeeper протухает, переходит на новую.
	  */
	void restartingThread();

	/// Вызывается во время выбора кусков для слияния.
	bool canMergeParts(const MergeTreeData::DataPartPtr & left, const MergeTreeData::DataPartPtr & right);

	/// Обмен кусками.

	/** Возвращает пустую строку, если куска ни у кого нет.
	  */
	String findReplicaHavingPart(const String & part_name, bool active);

	/** Скачать указанный кусок с указанной реплики.
	  */
	void fetchPart(const String & part_name, const String & replica_name);

	///

	AbandonableLockInZooKeeper allocateBlockNumber(const String & month_name);

	/** Дождаться, пока все реплики, включая эту, выполнят указанное действие из лога.
	  * Если одновременно с этим добавляются реплики, может не дождаться добавленную реплику.
	  */
	void waitForAllReplicasToProcessLogEntry(const LogEntry & entry);
};

}
