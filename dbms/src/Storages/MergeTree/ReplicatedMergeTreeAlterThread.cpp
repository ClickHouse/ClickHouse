#include <memory>
#include <DB/Common/setThreadName.h>
#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <DB/Storages/ColumnsDescription.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>
#include <DB/Storages/MergeTree/ReplicatedMergeTreeAlterThread.h>
#include <DB/Databases/IDatabase.h>


namespace DB
{

static const auto ALTER_ERROR_SLEEP_MS = 10 * 1000;


ReplicatedMergeTreeAlterThread::ReplicatedMergeTreeAlterThread(StorageReplicatedMergeTree & storage_)
	: storage(storage_),
	log(&Logger::get(storage.database_name + "." + storage.table_name + " (StorageReplicatedMergeTree, AlterThread)")),
	thread([this] { run(); }) {}


void ReplicatedMergeTreeAlterThread::run()
{
	setThreadName("ReplMTAlter");

	bool force_recheck_parts = true;

	while (!need_stop)
	{
		try
		{
			/** Имеем описание столбцов в ZooKeeper, общее для всех реплик (Пример: /clickhouse/tables/02-06/visits/columns),
			  *  а также описание столбцов в локальном файле с метаданными (storage.data.getColumnsList()).
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

			auto zookeeper = storage.getZooKeeper();

			zkutil::Stat stat;
			const String columns_str = zookeeper->get(storage.zookeeper_path + "/columns", &stat, wakeup_event);
			auto columns_desc = ColumnsDescription<true>::parse(columns_str);

			auto & columns = columns_desc.columns;
			auto & materialized_columns = columns_desc.materialized;
			auto & alias_columns = columns_desc.alias;
			auto & column_defaults = columns_desc.defaults;

			bool changed_version = (stat.version != storage.columns_version);

			{
				/// Если потребуется блокировать структуру таблицы, то приостановим мерджи.
				MergeTreeDataMerger::Blocker merge_blocker;
				MergeTreeDataMerger::Blocker unreplicated_merge_blocker;

				if (changed_version || force_recheck_parts)
				{
					merge_blocker = storage.merger.cancel();
					if (storage.unreplicated_merger)
						unreplicated_merge_blocker = storage.unreplicated_merger->cancel();
				}

				MergeTreeData::DataParts parts;

				/// Если описание столбцов изменилось, обновим структуру таблицы локально.
				if (changed_version)
				{
					LOG_INFO(log, "Changed version of 'columns' node in ZooKeeper. Waiting for structure write lock.");

					auto table_lock = storage.lockStructureForAlter();

					const auto columns_changed = columns != storage.data.getColumnsListNonMaterialized();
					const auto materialized_columns_changed = materialized_columns != storage.data.materialized_columns;
					const auto alias_columns_changed = alias_columns != storage.data.alias_columns;
					const auto column_defaults_changed = column_defaults != storage.data.column_defaults;

					if (columns_changed || materialized_columns_changed || alias_columns_changed ||
						column_defaults_changed)
					{
						LOG_INFO(log, "Columns list changed in ZooKeeper. Applying changes locally.");

						storage.context.getDatabase(storage.database_name)->alterTable(
							storage.context, storage.table_name,
							columns, materialized_columns, alias_columns, column_defaults, {});

						if (columns_changed)
						{
							storage.data.setColumnsList(columns);

							if (storage.unreplicated_data)
								storage.unreplicated_data->setColumnsList(columns);
						}

						if (materialized_columns_changed)
						{
							storage.materialized_columns = materialized_columns;
							storage.data.materialized_columns = std::move(materialized_columns);
						}

						if (alias_columns_changed)
						{
							storage.alias_columns = alias_columns;
							storage.data.alias_columns = std::move(alias_columns);
						}

						if (column_defaults_changed)
						{
							storage.column_defaults = column_defaults;
							storage.data.column_defaults = std::move(column_defaults);
						}

						LOG_INFO(log, "Applied changes to table.");
					}
					else
					{
						LOG_INFO(log, "Columns version changed in ZooKeeper, but data wasn't changed. It's like cyclic ALTERs.");
					}

					/// Нужно получить список кусков под блокировкой таблицы, чтобы избежать race condition с мерджем.
					parts = storage.data.getDataParts();

					storage.columns_version = stat.version;
				}

				/// Обновим куски.
				if (changed_version || force_recheck_parts)
				{
					auto table_lock = storage.lockStructure(false);

					if (changed_version)
						LOG_INFO(log, "ALTER-ing parts");

					int changed_parts = 0;

					if (!changed_version)
						parts = storage.data.getDataParts();

					const auto columns_plus_materialized = storage.data.getColumnsList();

					for (const MergeTreeData::DataPartPtr & part : parts)
					{
						/// Обновим кусок и запишем результат во временные файлы.
						/// TODO: Можно пропускать проверку на слишком большие изменения, если в ZooKeeper есть, например,
						///  нода /flags/force_alter.
						auto transaction = storage.data.alterDataPart(
							part, columns_plus_materialized, storage.data.primary_expr_ast, false);

						if (!transaction)
							continue;

						++changed_parts;

						/// Обновим метаданные куска в ZooKeeper.
						zkutil::Ops ops;
						ops.push_back(new zkutil::Op::SetData(
							storage.replica_path + "/parts/" + part->name + "/columns", transaction->getNewColumns().toString(), -1));
						ops.push_back(new zkutil::Op::SetData(
							storage.replica_path + "/parts/" + part->name + "/checksums", transaction->getNewChecksums().toString(), -1));

						try
						{
							zookeeper->multi(ops);
						}
						catch (const zkutil::KeeperException & e)
						{
							/// Куска не существует в ZK. Добавим в очередь для проверки - может быть, кусок лишний, и его надо убрать локально.
							if (e.code == ZNONODE)
								storage.enqueuePartForCheck(part->name);

							throw;
						}

						/// Применим изменения файлов.
						transaction->commit();
					}

					/// То же самое для нереплицируемых данных.
					if (storage.unreplicated_data)
					{
						parts = storage.unreplicated_data->getDataParts();

						for (const MergeTreeData::DataPartPtr & part : parts)
						{
							auto transaction = storage.unreplicated_data->alterDataPart(
								part, columns_plus_materialized, storage.data.primary_expr_ast, false);

							if (!transaction)
								continue;

							++changed_parts;

							transaction->commit();
						}
					}

					/// Список столбцов для конкретной реплики.
					zookeeper->set(storage.replica_path + "/columns", columns_str);

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

			wakeup_event->wait();
		}
		catch (...)
		{
			tryLogCurrentException(__PRETTY_FUNCTION__);

			force_recheck_parts = true;

			wakeup_event->tryWait(ALTER_ERROR_SLEEP_MS);
		}
	}

	LOG_DEBUG(log, "Alter thread finished");
}

}
