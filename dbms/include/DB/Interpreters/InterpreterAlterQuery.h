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
		bool detach = false; /// true для DETACH PARTITION.

		bool unreplicated = false;
		bool part = false;

		String from; /// Для FETCH PARTITION - путь в ZK к шарду, с которого скачивать партицию.

		/// Для RESHARD PARTITION.
		Field last_partition;
		WeightedZooKeeperPaths weighted_zookeeper_paths;
		ASTPtr sharding_key_expr;
		bool do_copy = false;
		Field coordinator;

		/// For FREEZE PARTITION
		String with_name;

		static PartitionCommand dropPartition(const Field & partition, bool detach, bool unreplicated)
		{
			PartitionCommand res;
			res.type = DROP_PARTITION;
			res.partition = partition;
			res.detach = detach;
			res.unreplicated = unreplicated;
			return res;
		}

		static PartitionCommand attachPartition(const Field & partition, bool unreplicated, bool part)
		{
			PartitionCommand res;
			res.type = ATTACH_PARTITION;
			res.partition = partition;
			res.unreplicated = unreplicated;
			res.part = part;
			return res;
		}

		static PartitionCommand fetchPartition(const Field & partition, const String & from)
		{
			PartitionCommand res;
			res.type = FETCH_PARTITION;
			res.partition = partition;
			res.from = from;
			return res;
		}

		static PartitionCommand freezePartition(const Field & partition, const String & with_name)
		{
			PartitionCommand res;
			res.type = FREEZE_PARTITION;
			res.partition = partition;
			res.with_name = with_name;
			return res;
		}

		static PartitionCommand reshardPartitions(const Field & first_partition_, const Field & last_partition_,
			const WeightedZooKeeperPaths & weighted_zookeeper_paths_, const ASTPtr & sharding_key_expr_,
			bool do_copy_, const Field & coordinator_)
		{
			return {RESHARD_PARTITION, first_partition_, false, false, false, {},
				last_partition_, weighted_zookeeper_paths_, sharding_key_expr_, do_copy_, coordinator_};
		}
	};

	using PartitionCommands = std::vector<PartitionCommand>;

	ASTPtr query_ptr;

	Context context;

	static void parseAlter(const ASTAlterQuery::ParameterContainer & params,
		AlterCommands & out_alter_commands, PartitionCommands & out_partition_commands);
};

}
