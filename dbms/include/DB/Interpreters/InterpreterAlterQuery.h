#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Storages/AlterCommands.h>
#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/IInterpreter.h>

namespace DB
{



/** Позволяет добавить или удалить столбец в таблице.
  * Также позволяет осуществить манипуляции с партициями таблиц семейства MergeTree.
  */
class InterpreterAlterQuery : public IInterpreter
{
public:
	InterpreterAlterQuery(ASTPtr query_ptr_, const Context & context_);

	BlockIO execute() override;

	/** Изменяет список столбцов в метаданных таблицы на диске. Нужно вызывать под TableStructureLock соответствующей таблицы.
	  */
	static void updateMetadata(const String & database,
		const String & table,
		const NamesAndTypesList & columns,
		const NamesAndTypesList & materialized_columns,
		const NamesAndTypesList & alias_columns,
		const ColumnDefaults & column_defaults,
		const Context & context);

private:
	struct PartitionCommand
	{
		enum Type
		{
			DROP_PARTITION,
			ATTACH_PARTITION,
			FETCH_PARTITION,
			FREEZE_PARTITION,
			RESHARD_PARTITION
		};

		Type type;

		Field partition;
		bool detach; /// true для DETACH PARTITION.

		bool unreplicated;
		bool part;

		String from; /// Для FETCH PARTITION - путь в ZK к шарду, с которого скачивать партицию.

		/// Для RESHARD PARTITION.
		Field last_partition;
		WeightedZooKeeperPaths weighted_zookeeper_paths;
		ASTPtr sharding_key_expr;
		bool do_copy;
		Field coordinator;

		static PartitionCommand dropPartition(const Field & partition, bool detach, bool unreplicated)
		{
			return {DROP_PARTITION, partition, detach, unreplicated};
		}

		static PartitionCommand attachPartition(const Field & partition, bool unreplicated, bool part)
		{
			return {ATTACH_PARTITION, partition, false, unreplicated, part};
		}

		static PartitionCommand fetchPartition(const Field & partition, const String & from)
		{
			return {FETCH_PARTITION, partition, false, false, false, from};
		}

		static PartitionCommand freezePartition(const Field & partition)
		{
			return {FREEZE_PARTITION, partition};
		}

		static PartitionCommand reshardPartitions(const Field & first_partition_, const Field & last_partition_,
			const WeightedZooKeeperPaths & weighted_zookeeper_paths_, const ASTPtr & sharding_key_expr_,
			bool do_copy_, const Field & coordinator_)
		{
			return {RESHARD_PARTITION, first_partition_, false, false, false, {},
				last_partition_, weighted_zookeeper_paths_, sharding_key_expr_, do_copy_, coordinator_};
		}
	};

	typedef std::vector<PartitionCommand> PartitionCommands;

	ASTPtr query_ptr;

	Context context;

	static void parseAlter(const ASTAlterQuery::ParameterContainer & params,
		AlterCommands & out_alter_commands, PartitionCommands & out_partition_commands);
};

}
