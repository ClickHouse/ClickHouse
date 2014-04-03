#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Storages/MergeTree/MergeTreeData.h>
#include <DB/Storages/MergeTree/MergeTreeDataMerger.h>
#include <DB/Storages/MergeTree/MergeTreeDataWriter.h>
#include <DB/Storages/MergeTree/MergeTreeDataSelectExecutor.h>
#include "MergeTree/ReplicatedMergeTreePartsExchange.h"
#include <zkutil/ZooKeeper.h>

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
		const String & path_, const String & name_, NamesAndTypesListPtr columns_,
		Context & context_,
		ASTPtr & primary_expr_ast_,
		const String & date_column_name_,
		const ASTPtr & sampling_expression_, /// NULL, если семплирование не поддерживается.
		size_t index_granularity_,
		MergeTreeData::Mode mode_ = MergeTreeData::Ordinary,
		const String & sign_column_ = "",
		const MergeTreeSettings & settings_ = MergeTreeSettings());

	void shutdown();
	~StorageReplicatedMergeTree();

	std::string getName() const override
	{
		return "Replicated" + data.getModePrefix() + "MergeTree";
	}

	std::string getTableName() const override { return name; }
	std::string getSignColumnName() const { return data.getSignColumnName(); }
	bool supportsSampling() const override { return data.supportsSampling(); }
	bool supportsFinal() const override { return data.supportsFinal(); }
	bool supportsPrewhere() const override { return data.supportsPrewhere(); }

	const NamesAndTypesList & getColumnsList() const override { return data.getColumnsList(); }

	BlockInputStreams read(
		const Names & column_names,
		ASTPtr query,
		const Settings & settings,
		QueryProcessingStage::Enum & processed_stage,
		size_t max_block_size = DEFAULT_BLOCK_SIZE,
		unsigned threads = 1) override;

	BlockOutputStreamPtr write(ASTPtr query) override;

	/** Удаляет реплику из ZooKeeper. Если других реплик нет, удаляет всю таблицу из ZooKeeper.
	  */
	void drop() override;

private:
	friend class ReplicatedMergeTreeBlockOutputStream;

	struct LogEntry
	{
		enum Type
		{
			GET_PART,
			MERGE_PARTS,
		};

		String znode_name;

		Type type;
		String new_part_name;
		Strings parts_to_merge;

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

		static LogEntry parse(const String & s)
		{
			ReadBufferFromString in(s);
			LogEntry res;
			res.readText(in);
			assertEOF(in);
			return res;
		}
	};

	typedef std::list<LogEntry> LogEntries;

	Context & context;
	zkutil::ZooKeeper & zookeeper;

	/** "Очередь" того, что нужно сделать на этой реплике, чтобы всех догнать. Берется из ZooKeeper (/replicas/me/queue/).
	  * В ZK записи в хронологическом порядке. Здесь записи в том порядке, в котором их лучше выполнять.
	  */
	LogEntries queue;
	Poco::FastMutex queue_mutex;

	String path;
	String name;
	String full_path;

	String zookeeper_path;
	String replica_name;
	String replica_path;

	/** /replicas/me/is_active.
	  */
	zkutil::EphemeralNodeHolderPtr replica_is_active_node;

	/** Является ли эта реплика "ведущей". Ведущая реплика выбирает куски для слияния.
	  */
	bool is_leader_node;

	InterserverIOEndpointHolderPtr endpoint_holder;

	MergeTreeData data;
	MergeTreeDataSelectExecutor reader;
	MergeTreeDataWriter writer;
	ReplicatedMergeTreePartsFetcher fetcher;

	Logger * log;

	volatile bool shutdown_called;

	StorageReplicatedMergeTree(
		const String & zookeeper_path_,
		const String & replica_name_,
		bool attach,
		const String & path_, const String & name_, NamesAndTypesListPtr columns_,
		Context & context_,
		ASTPtr & primary_expr_ast_,
		const String & date_column_name_,
		const ASTPtr & sampling_expression_,
		size_t index_granularity_,
		MergeTreeData::Mode mode_ = MergeTreeData::Ordinary,
		const String & sign_column_ = "",
		const MergeTreeSettings & settings_ = MergeTreeSettings());

	/// Инициализация.

	/** Проверяет, что в ZooKeeper в таблице нет данных.
	  */
	bool isTableEmpty();

	/** Создает минимальный набор нод в ZooKeeper.
	  */
	void createTable();
	void createReplica();

	/** Отметить в ZooKeeper, что эта реплика сейчас активна.
	  */
	void activateReplica();

	/** Проверить, что список столбцов и настройки таблицы совпадают с указанными в ZK (/metadata).
	  * Если нет - бросить исключение.
	  */
	void checkTableStructure();

	/** Проверить, что множество кусков соответствует тому, что в ZK (/replicas/me/parts/).
	  * Если каких-то кусков, описанных в ZK нет локально, бросить исключение.
	  * Если какие-то локальные куски не упоминаются в ZK, удалить их.
	  *  Но если таких слишком много, на всякий случай бросить исключение - скорее всего, это ошибка конфигурации.
	  */
	void checkParts();

	/// Работа с очередью и логом.

	/** Кладет в queue записи из ZooKeeper (/replicas/me/queue/).
	  */
	void loadQueue();

	/** Копирует новые записи из логов всех реплик в очередь этой реплики.
	  */
	void pullLogsToQueue();

	/** Делает преобразования над очередью:
	  *  - Если есть MERGE_PARTS кусков, не все из которых у нас есть, заменяем его на GET_PART и
	  *    убираем GET_PART для всех составляющих его кусков. NOTE: Наверно, это будет плохо работать. Придумать эвристики получше.
	  */
	void optimizeQueue();

	/** По порядку пытается выполнить действия из очереди, пока не получится. Что получилось, выбрасывает из очереди.
	  */
	void executeSomeQueueEntry();

	/** Попробовать выполнить действие из очереди. Возвращает false, если не получилось по какой-то ожидаемой причине:
	  *  - GET_PART, и ни у кого нет этого куска. Это возможно, если этот кусок уже слили с кем-то и удалили.
	  *  - Не смогли скачать у кого-то кусок, потому что его там уже нет.
	  *  - Не смогли объединить куски, потому что не все из них у нас есть.
	  */
	bool tryExecute(const LogEntry & entry);

	/// Обмен кусками.

	String findReplicaHavingPart(const String & part_name);
	void getPart(const String & name, const String & replica_name);
};

}
