#include <DB/Storages/MergeTree/ReplicatedMergeTreeLogEntry.h>
#include <DB/Storages/MergeTree/ActiveDataPartSet.h>
#include <DB/Storages/MergeTree/MergeTreeData.h>

#include <zkutil/ZooKeeper.h>


namespace DB
{


class MergeTreeDataMerger;


class ReplicatedMergeTreeQueue
{
private:
	friend class CurrentlyExecuting;

	using StringSet = std::set<String>;

	using LogEntry = ReplicatedMergeTreeLogEntry;
	using LogEntryPtr = LogEntry::Ptr;

	using Queue = std::list<LogEntryPtr>;

	struct ByTime
	{
		bool operator()(const LogEntryPtr & lhs, const LogEntryPtr & rhs) const
		{
			return std::forward_as_tuple(lhs.get()->create_time, lhs.get())
				 < std::forward_as_tuple(rhs.get()->create_time, rhs.get());
		}
	};

	/// Для вычисления min_unprocessed_insert_time, max_processed_insert_time, по которым вычисляется отставание реплик.
	using InsertsByTime = std::set<LogEntryPtr, ByTime>;


	String zookeeper_path;
	String replica_path;
	String logger_name;

	/** Очередь того, что нужно сделать на этой реплике, чтобы всех догнать. Берется из ZooKeeper (/replicas/me/queue/).
	  * В ZK записи в хронологическом порядке. Здесь - не обязательно.
	  */
	Queue queue;

	InsertsByTime inserts_by_time;
	time_t min_unprocessed_insert_time = 0;
	time_t max_processed_insert_time = 0;

	time_t last_queue_update = 0;

	/// Куски, которые появятся в результате действий, выполняемых прямо сейчас фоновыми потоками (этих действий нет в очереди).
	/// Используется, чтобы не выполнять в тот же момент другие действия с этими кусками.
	StringSet future_parts;

	/// На доступ к queue, future_parts, ...
	std::mutex mutex;

	/// Обеспечивает только один одновременный вызов pullLogsToQueue.
	std::mutex pull_logs_to_queue_mutex;

	/** Каким будет множество активных кусков после выполнения всей текущей очереди - добавления новых кусков и выполнения слияний.
	  * Используется, чтобы определять, какие мерджи уже были назначены:
	  *  - если в этом множестве есть кусок, то мерджи более мелких кусков внутри его диапазона не делаются.
	  * Дополнительно, сюда также добавляются специальные элементы, чтобы явно запретить мерджи в некотором диапазоне (см. disableMergesInRange).
	  * Это множество защищено своим mutex-ом.
	  */
	ActiveDataPartSet virtual_parts;

	Logger * log = nullptr;


	/// Положить набор (уже существующих) кусков в virtual_parts.
	void initVirtualParts(const MergeTreeData::DataParts & parts);

	/// Загрузить (инициализировать) очередь из ZooKeeper (/replicas/me/queue/).
	void load(zkutil::ZooKeeperPtr zookeeper);

	void insertUnlocked(LogEntryPtr & entry);

	void remove(zkutil::ZooKeeperPtr zookeeper, LogEntryPtr & entry);

	/** Можно ли сейчас попробовать выполнить это действие. Если нет, нужно оставить его в очереди и попробовать выполнить другое.
	  * Вызывается под queue_mutex.
	  */
	bool shouldExecuteLogEntry(const LogEntry & entry, String & out_postpone_reason, MergeTreeDataMerger & merger, MergeTreeData & data);

	/// После удаления элемента очереди, обновить времена insert-ов в оперативке. Выполняется под queue_mutex.
	/// Возвращает информацию, какие времена изменились - эту информацию можно передать в updateTimesInZooKeeper.
	void updateTimesOnRemoval(const LogEntryPtr & entry, bool & min_unprocessed_insert_time_changed, bool & max_processed_insert_time_changed);

	/// Обновить времена insert-ов в ZooKeeper.
	void updateTimesInZooKeeper(zkutil::ZooKeeperPtr zookeeper, bool min_unprocessed_insert_time_changed, bool max_processed_insert_time_changed);


	/// Помечает элемент очереди как выполняющийся.
	class CurrentlyExecuting
	{
	private:
		ReplicatedMergeTreeQueue::LogEntryPtr entry;
		ReplicatedMergeTreeQueue & queue;

		friend class ReplicatedMergeTreeQueue;

		/// Создаётся только в функции selectEntryToProcess. Вызывается под mutex-ом.
		CurrentlyExecuting(ReplicatedMergeTreeQueue::LogEntryPtr & entry, ReplicatedMergeTreeQueue & queue);
	public:
		~CurrentlyExecuting();
	};

public:
	ReplicatedMergeTreeQueue() {}

	void initialize(const String & zookeeper_path_, const String & replica_path_, const String & logger_name_,
		const MergeTreeData::DataParts & parts, zkutil::ZooKeeperPtr zookeeper);

	/** Вставить действие в конец очереди.
	  * Для восстановления битых кусков во время работы.
	  * Не вставляет само действие в ZK (сделайте это самостоятельно).
	  */
	void insert(zkutil::ZooKeeperPtr zookeeper, LogEntryPtr & entry);

	/** Удалить действие с указанным куском (в качестве new_part_name) из очереди.
	  * Вызывается для невыполнимых действий в очереди - старых потерянных кусков.
	  */
	bool remove(zkutil::ZooKeeperPtr zookeeper, const String & part_name);

	/** Скопировать новые записи из общего лога в очередь этой реплики. Установить log_pointer в соответствующее значение.
	  * Если next_update_event != nullptr, вызовет это событие, когда в логе появятся новые записи.
	  * Возвращает true, если новые записи были.
	  */
	bool pullLogsToQueue(zkutil::ZooKeeperPtr zookeeper, zkutil::EventPtr next_update_event);

	/** Удалить из очереди действия с кусками, покрываемыми part_name (из ZK и из оперативки).
	  * А также дождаться завершения их выполнения, если они сейчас выполняются.
	  */
	void removeGetsAndMergesInRange(zkutil::ZooKeeperPtr zookeeper, const String & part_name);

	/** В случае, когда для выполнения мерджа в part_name недостаёт кусков
	  * - переместить действия со сливаемыми кусками в конец очереди
	  * (чтобы раньше скачать готовый смердженный кусок с другой реплики).
	  */
	StringSet moveSiblingPartsForMergeToEndOfQueue(const String & part_name);

	/** Выбрать следующее действие для обработки.
	  * merger используется только чтобы проверить, не приостановлены ли мерджи.
	  */
	using SelectedEntry = std::pair<ReplicatedMergeTreeQueue::LogEntryPtr, std::unique_ptr<CurrentlyExecuting>>;
	SelectedEntry selectEntryToProcess(MergeTreeDataMerger & merger, MergeTreeData & data);

	/** Выполнить функцию func для обработки действия.
	  * При этом, на время выполнения, отметить элемент очереди как выполняющийся
	  *  (добавить в future_parts и другое).
	  * Если в процессе обработки было исключение - сохраняет его в entry.
	  * Возвращает true, если в процессе обработки не было исключений.
	  */
	bool processEntry(std::function<zkutil::ZooKeeperPtr()> get_zookeeper, LogEntryPtr & entry, const std::function<bool(LogEntryPtr &)> func);

	/// Будет ли кусок в будущем слит в более крупный (или мерджи кусков в данном диапазоне запрещены)?
	bool partWillBeMergedOrMergesDisabled(const String & part_name) const;

	/// Запретить слияния в указанном диапазоне.
	void disableMergesInRange(const String & part_name);

	/// Посчитать количество слияний в очереди.
	size_t countMerges();

	struct Status
	{
		UInt32 future_parts;
		UInt32 queue_size;
		UInt32 inserts_in_queue;
		UInt32 merges_in_queue;
		UInt32 queue_oldest_time;
		UInt32 inserts_oldest_time;
		UInt32 merges_oldest_time;
		String oldest_part_to_get;
		String oldest_part_to_merge_to;
		UInt32 last_queue_update;
	};

	/// Получить информацию об очереди.
	Status getStatus();

	/// Получить данные элементов очереди.
	using LogEntriesData = std::vector<ReplicatedMergeTreeLogEntryData>;
	void getEntries(LogEntriesData & res);

	/// Получить информацию о временах insert-ов.
	void getInsertTimes(time_t & out_min_unprocessed_insert_time, time_t & out_max_processed_insert_time) const;
};


/** Преобразовать число в строку формате суффиксов автоинкрементных нод в ZooKeeper.
  * Поддерживаются также отрицательные числа - для них имя ноды выглядит несколько глупо
  *  и не соответствует никакой автоинкрементной ноде в ZK.
  */
String padIndex(Int64 index);

}
