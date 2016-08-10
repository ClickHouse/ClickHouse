#pragma once

#include <DB/Databases/IDatabase.h>
#include <DB/Common/UInt128.h>


namespace DB
{

/** Позволяет создавать "облачные таблицы".
  * Список таких таблиц хранится в ZooKeeper.
  * Все серверы, ссылающиеся на один путь в ZooKeeper, видят один и тот же список таблиц.
  * CREATE, DROP, RENAME атомарны.
  *
  * Для БД задаётся уровень репликации N.
  * При записи в "облачную" таблицу, некоторым образом выбирается N живых серверов в разных датацентрах,
  *  на каждом из них создаётся локальная таблица, и в них производится запись.
  *
  * Движок имеет параметры: Cloud(zookeeper_path, replication_factor, datacenter_name)
  * Пример: Cloud('/clickhouse/clouds/production/', 3, 'FIN')
  *
  * Структура в ZooKeeper:
  *
  * cloud_path                   - путь к "облаку"; может существовать несколько разных независимых облаков
		/table_definitions       - множество уникальных определений таблиц, чтобы не писать их много раз для большого количества таблиц
			/hash128 -> sql      - отображение: хэш от определения таблицы (идентификатор) -> само определение таблицы в виде CREATE запроса
		/tables                  - список таблиц
			/database_name       - имя базы данных
				/name_hash_mod -> compressed_table_list
			                     - список таблиц сделан двухуровневым, чтобы уменьшить количество узлов в ZooKeeper при наличии большого количества таблиц
			                     - узлы создаются для каждого остатка от деления хэша от имени таблицы, например, на 4096
			                     - и в каждом узле хранится список таблиц (имя таблицы, имя локальной таблицы, хэш от структуры, список хостов) в сжатом виде
		/local_tables            - список локальных таблиц, чтобы по имени локальной таблицы можно было определить её структуру
			/database_name
				/name_hash_mod -> compressed_table_list
				                 - список пар (хэш от имени таблицы, хэш от структуры) в сжатом виде
		/locality_keys           - сериализованный список ключей локальности в порядке их появления
			                     - ключ локальности - произвольная строка
			                     - движок БД определяет серверы для расположения данных таким образом,
								   чтобы, при одинаковом множестве живых серверов,
								   одному ключу локальности соответствовала одна группа из N серверов для расположения данных.
		/nodes                   - список серверов, на которых зарегистрированы облачные БД с таким путём в ZK
			/hostname            - имя хоста
		TODO	/alive           - эфемерная нода для предварительной проверки живости
				/datacenter      - имя датацентра
		TODO	/disk_space

  * К одному облаку может относиться несколько БД, названных по-разному. Например, БД hits и visits могут относиться к одному облаку.
  */
class DatabaseCloud : public IDatabase
{
private:
	const String name;
	const String data_path;
	String zookeeper_path;
	const size_t replication_factor;
	const String hostname;
	const String datacenter_name;

	Logger * log;

	Context & context;

	/** Локальные таблицы - это таблицы, находящиеся непосредственно на локальном сервере.
	  * Они хранят данные облачных таблиц: облачная таблица представлена несколькими локальными таблицами на разных серверах.
	  * Эти таблицы не видны пользователю при перечислении таблиц, хотя доступны при обращении по имени.
	  * Имя локальных таблиц имеет специальную форму, например, начинаться на _local, чтобы они не путались с облачными таблицами.
	  * Локальные таблицы загружаются лениво, при первом обращении.
	  */
	Tables local_tables_cache;
	mutable std::mutex local_tables_mutex;

	friend class DatabaseCloudIterator;

public:
	DatabaseCloud(
		bool attach,
		const String & name_,
		const String & zookeeper_path_,
		size_t replication_factor_,
		const String & datacenter_name_,
		Context & context_);

	String getEngineName() const override { return "Cloud"; }

	void loadTables(Context & context, ThreadPool * thread_pool, bool has_force_restore_data_flag) override;

	bool isTableExist(const String & table_name) const override;
	StoragePtr tryGetTable(const String & table_name) override;

	DatabaseIteratorPtr getIterator() override;

	bool empty() const override;

	void createTable(const String & table_name, const StoragePtr & table, const ASTPtr & query, const String & engine) override;
	void removeTable(const String & table_name) override;

	void attachTable(const String & table_name, const StoragePtr & table) override;
	StoragePtr detachTable(const String & table_name) override;

	void renameTable(const Context & context, const String & table_name, IDatabase & to_database, const String & to_table_name) override;

	ASTPtr getCreateQuery(const String & table_name) const override;

	void shutdown() override;
	void drop() override;

	void alterTable(
		const Context & context,
		const String & name,
		const NamesAndTypesList & columns,
		const NamesAndTypesList & materialized_columns,
		const NamesAndTypesList & alias_columns,
		const ColumnDefaults & column_defaults,
		const ASTModifier & engine_modifier) override
	{
		throw Exception("ALTER TABLE is not supported by database engine " + getEngineName(), ErrorCodes::NOT_IMPLEMENTED);
	}

	using Hash = UInt128;

private:
	void createZookeeperNodes();

	/// Получить имя узла, в котором будет храниться часть списка таблиц. (Список таблиц является двухуровневым.)
	String getNameOfNodeWithTables(const String & table_name) const;

	/// Хэшировать имя таблицы вместе с именем БД.
	Hash getTableHash(const String & table_name) const;

	/// Определения таблиц хранятся косвенным образом и адресуются своим хэшом. Вычислить хэш.
	Hash getHashForTableDefinition(const String & definition) const;

	/// Пойти в ZooKeeper и по хэшу получить определение таблицы.
	String getTableDefinitionFromHash(Hash hash) const;

	/// Определить серверы, на которых будут храниться данные таблицы.
	std::vector<String> selectHostsForTable(const String & locality_key) const;
};

}
