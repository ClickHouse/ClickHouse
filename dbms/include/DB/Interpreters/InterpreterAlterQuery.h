#pragma once

#include <DB/Storages/IStorage.h>
#include <DB/Storages/AlterCommands.h>
#include <DB/Interpreters/Context.h>
#include <DB/Parsers/ASTIdentifier.h>

namespace DB
{



/** Позволяет добавить или удалить столбец в таблице.
  */
class InterpreterAlterQuery
{
public:
	InterpreterAlterQuery(ASTPtr query_ptr_, Context & context_);

	void execute();

	/** Изменяет список столбцов в метаданных таблицы на диске. Нужно вызывать под TableStructureLock соответствующей таблицы.
	  */
	static void updateMetadata(const String & database,
		const String & table,
		const NamesAndTypesList & columns,
		const NamesAndTypesList & materialized_columns,
		const NamesAndTypesList & alias_columns,
		const ColumnDefaults & column_defaults,
		Context & context);
private:
	struct PartitionCommand
	{
		enum Type
		{
			DROP_PARTITION,
			ATTACH_PARTITION,
		};

		Type type;

		Field partition;
		bool detach; /// true для DETACH PARTITION.

		bool unreplicated;
		bool part;

		static PartitionCommand dropPartition(const Field & partition, bool detach)
		{
			return {DROP_PARTITION, partition, detach};
		}

		static PartitionCommand attachPartition(const Field & partition, bool unreplicated, bool part)
		{
			return {ATTACH_PARTITION, partition, false, unreplicated, part};
		}
	};

	typedef std::vector<PartitionCommand> PartitionCommands;

	ASTPtr query_ptr;

	Context context;

	static void parseAlter(const ASTAlterQuery::ParameterContainer & params, const DataTypeFactory & data_type_factory,
		AlterCommands & out_alter_commands, PartitionCommands & out_partition_commands);
};

}
