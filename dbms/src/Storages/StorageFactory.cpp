#include <Poco/Util/Application.h>
#include <Poco/Util/AbstractConfiguration.h>

#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/ASTLiteral.h>

#include <DB/Interpreters/Context.h>
#include <DB/Interpreters/reinterpretAsIdentifier.h>

#include <DB/Storages/StorageLog.h>
#include <DB/Storages/StorageTinyLog.h>
#include <DB/Storages/StorageMemory.h>
#include <DB/Storages/StorageNull.h>
#include <DB/Storages/StorageMerge.h>
#include <DB/Storages/StorageMergeTree.h>
#include <DB/Storages/StorageDistributed.h>
#include <DB/Storages/StorageSystemNumbers.h>
#include <DB/Storages/StorageSystemOne.h>
#include <DB/Storages/StorageFactory.h>
#include <DB/Storages/StorageView.h>
#include <DB/Storages/StorageMaterializedView.h>
#include <DB/Storages/StorageChunks.h>
#include <DB/Storages/StorageChunkRef.h>
#include <DB/Storages/StorageChunkMerger.h>
#include <DB/Storages/StorageReplicatedMergeTree.h>


namespace DB
{

static bool endsWith(const std::string & s, const std::string & suffix)
{
	return s.size() >= suffix.size() && 0 == strncmp(s.data() + s.size() - suffix.size(), suffix.data(), suffix.size());
}

static bool startsWith(const std::string & s, const std::string & prefix)
{
	return s.size() >= prefix.size() && 0 == strncmp(s.data(), prefix.data(), prefix.size());
}

/** Для StorageMergeTree: достать первичный ключ в виде ASTExpressionList.
  * Он может быть указан в кортеже: (CounterID, Date),
  *  или как один столбец: CounterID.
  */
static ASTPtr extractPrimaryKey(const ASTPtr & node, const std::string & storage_name)
{
	const ASTFunction * primary_expr_func = typeid_cast<const ASTFunction *>(&*node);

	if (primary_expr_func && primary_expr_func->name == "tuple")
	{
		/// Первичный ключ указан в кортеже.
		return primary_expr_func->children.at(0);
	}
	else
	{
		/// Первичный ключ состоит из одного столбца.
		ASTExpressionList * res = new ASTExpressionList;
		ASTPtr res_ptr = res;
		res->children.push_back(node);
		return res_ptr;
	}
}


StoragePtr StorageFactory::get(
	const String & name,
	const String & data_path,
	const String & table_name,
	const String & database_name,
	Context & local_context,
	Context & context,
	ASTPtr & query,
	NamesAndTypesListPtr columns,
	bool attach) const
{
	if (name == "Log")
	{
		return StorageLog::create(data_path, table_name, columns, context.getSettings().max_compress_block_size);
	}
	else if (name == "Chunks")
	{
		return StorageChunks::create(data_path, table_name, database_name, columns, context, attach);
	}
	else if (name == "ChunkRef")
	{
		throw Exception("Table with storage ChunkRef must not be created manually.", ErrorCodes::TABLE_MUST_NOT_BE_CREATED_MANUALLY);
	}
	else if (name == "View")
	{
		return StorageView::create(table_name, database_name, context, query, columns);
	}
	else if (name == "MaterializedView")
	{
		return StorageMaterializedView::create(table_name, database_name, context, query, columns, attach);
	}
	else if (name == "ChunkMerger")
	{
		ASTs & args_func = typeid_cast<ASTFunction &>(*typeid_cast<ASTCreateQuery &>(*query).storage).children;

		do
		{
			if (args_func.size() != 1)
				break;

			ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

			if (args.size() < 3 || args.size() > 4)
				break;

			String source_database = reinterpretAsIdentifier(args[0], local_context).name;
			String source_table_name_regexp = safeGet<const String &>(typeid_cast<ASTLiteral &>(*args[1]).value);
			size_t chunks_to_merge = safeGet<UInt64>(typeid_cast<ASTLiteral &>(*args[2]).value);

			String destination_name_prefix = "group_";
			String destination_database = source_database;

			if (args.size() > 3)
				destination_name_prefix = typeid_cast<ASTIdentifier &>(*args[3]).name;

			return StorageChunkMerger::create(database_name, table_name, columns, source_database, source_table_name_regexp, destination_name_prefix, chunks_to_merge, context);
		} while(false);

		throw Exception("Storage ChunkMerger requires from 3 to 4 parameters:"
			" source database, regexp for source table names, number of chunks to merge, [destination tables name prefix].",
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
	}
	else if (name == "TinyLog")
	{
		return StorageTinyLog::create(data_path, table_name, columns, attach, context.getSettings().max_compress_block_size);
	}
	else if (name == "Memory")
	{
		return StorageMemory::create(table_name, columns);
	}
	else if (name == "Null")
	{
		return StorageNull::create(table_name, columns);
	}
	else if (name == "Merge")
	{
		/** В запросе в качестве аргумента для движка указано имя БД, в которой находятся таблицы-источники,
		  *  а также регексп для имён таблиц-источников.
		  */
		ASTs & args_func = typeid_cast<ASTFunction &>(*typeid_cast<ASTCreateQuery &>(*query).storage).children;

		if (args_func.size() != 1)
			throw Exception("Storage Merge requires exactly 2 parameters"
				" - name of source database and regexp for table names.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

		if (args.size() != 2)
			throw Exception("Storage Merge requires exactly 2 parameters"
				" - name of source database and regexp for table names.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		String source_database 		= reinterpretAsIdentifier(args[0], local_context).name;
		String table_name_regexp	= safeGet<const String &>(typeid_cast<ASTLiteral &>(*args[1]).value);

		return StorageMerge::create(table_name, columns, source_database, table_name_regexp, context);
	}
	else if (name == "Distributed")
	{
		/** В запросе в качестве аргумента для движка указано имя конфигурационной секции,
		  *  в которой задан список удалённых серверов, а также имя удалённой БД и имя удалённой таблицы.
		  */
		ASTs & args_func = typeid_cast<ASTFunction &>(*typeid_cast<ASTCreateQuery &>(*query).storage).children;

		if (args_func.size() != 1)
			throw Exception("Storage Distributed requires 3 parameters"
				" - name of configuration section with list of remote servers, name of remote database, name of remote table.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

		if (args.size() != 3 && args.size() != 4)
			throw Exception("Storage Distributed requires 3 or 4 parameters"
				" - name of configuration section with list of remote servers, name of remote database, name of remote table,"
				" sharding key expression (optional).",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		String cluster_name 	= typeid_cast<ASTIdentifier &>(*args[0]).name;
		String remote_database 	= reinterpretAsIdentifier(args[1], local_context).name;
		String remote_table 	= typeid_cast<ASTIdentifier &>(*args[2]).name;

		const auto & sharding_key = args.size() == 4 ? args[3] : nullptr;

		return StorageDistributed::create(
			table_name, columns, remote_database, remote_table, cluster_name, context, sharding_key, data_path);
	}
	else if (endsWith(name, "MergeTree"))
	{
		/** Движки [Replicated][Summing|Collapsing|Aggregating|]MergeTree  (8 комбинаций)
		  * В качестве аргумента для движка должно быть указано:
		  *  - (для Replicated) Путь к таблице в ZooKeeper
		  *  - (для Replicated) Имя реплики в ZooKeeper
		  *  - имя столбца с датой;
		  *  - (не обязательно) выражение для семплирования (запрос с SAMPLE x будет выбирать строки, у которых в этом столбце значение меньше, чем x*UINT32_MAX);
		  *  - выражение для сортировки (либо скалярное выражение, либо tuple из нескольких);
		  *  - index_granularity;
		  *  - (для Collapsing) имя столбца, содержащего тип строчки с изменением "визита" (принимающего значения 1 и -1).
		  * Например: ENGINE = ReplicatedCollapsingMergeTree('/tables/mytable', 'rep02', EventDate, (CounterID, EventDate, intHash32(UniqID), VisitID), 8192, Sign).
		  */

		String name_part = name.substr(0, name.size() - strlen("MergeTree"));

		bool replicated = startsWith(name_part, "Replicated");
		if (replicated)
			name_part = name_part.substr(strlen("Replicated"));

		MergeTreeData::Mode mode = MergeTreeData::Ordinary;

		if (name_part == "Collapsing")
			mode = MergeTreeData::Collapsing;
		else if (name_part == "Summing")
			mode = MergeTreeData::Summing;
		else if (name_part == "Aggregating")
			mode = MergeTreeData::Aggregating;
		else if (!name_part.empty())
			throw Exception("Unknown storage " + name, ErrorCodes::UNKNOWN_STORAGE);

		ASTs & args_func = typeid_cast<ASTFunction &>(*typeid_cast<ASTCreateQuery &>(*query).storage).children;

		ASTs args;

		if (args_func.size() == 1)
			args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

		size_t additional_params = (replicated ? 2 : 0) + (mode == MergeTreeData::Collapsing ? 1 : 0);
		if (args.size() != additional_params + 3 && args.size() != additional_params + 4)
		{
			String params;
			if (replicated)
				params += "path in ZooKeeper, replica name or '', ";
			params += "name of column with date, [name of column for sampling], primary key expression, index granularity";
			if (mode == MergeTreeData::Collapsing)
				params += ", sign column";
			throw Exception("Storage " + name + " requires " + toString(additional_params + 3) + " or "
				+ toString(additional_params + 4) +" parameters: " + params,
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		}

		String zookeeper_path;
		String replica_name;

		String date_column_name;
		ASTPtr primary_expr_list;
		ASTPtr sampling_expression;
		UInt64 index_granularity;

		String sign_column_name;

		if (replicated)
		{
			auto ast = typeid_cast<ASTLiteral *>(&*args[0]);
			if (ast && ast->value.getType() == Field::Types::String)
				zookeeper_path = safeGet<String>(ast->value);
			else
				throw Exception("Path in ZooKeeper must be a string literal", ErrorCodes::BAD_ARGUMENTS);

			ast = typeid_cast<ASTLiteral *>(&*args[1]);
			if (ast && ast->value.getType() == Field::Types::String)
				replica_name = safeGet<String>(ast->value);
			else
				throw Exception("Replica name must be a string literal", ErrorCodes::BAD_ARGUMENTS);

			if (replica_name.empty())
				throw Exception("No replica name in config", ErrorCodes::NO_REPLICA_NAME_GIVEN);

			args.erase(args.begin(), args.begin() + 2);
		}

		if (mode == MergeTreeData::Collapsing)
		{
			if (auto ast = typeid_cast<ASTIdentifier *>(&*args.back()))
				sign_column_name = ast->name;
			else
				throw Exception("Sign column name must be an unquoted string", ErrorCodes::BAD_ARGUMENTS);

			args.pop_back();
		}

		if (args.size() == 4)
		{
			sampling_expression = args[1];
			args.erase(args.begin() + 1);
		}

		if (auto ast = typeid_cast<ASTIdentifier *>(&*args[0]))
			date_column_name = ast->name;
		else
			throw Exception("Date column name must be an unquoted string", ErrorCodes::BAD_ARGUMENTS);

		primary_expr_list = extractPrimaryKey(args[1], name);

		auto ast = typeid_cast<ASTLiteral *>(&*args[2]);
		if (ast && ast->value.getType() == Field::Types::UInt64)
				index_granularity = safeGet<UInt64>(ast->value);
		else
			throw Exception("Index granularity must be a positive integer", ErrorCodes::BAD_ARGUMENTS);

		if (replicated)
			return StorageReplicatedMergeTree::create(zookeeper_path, replica_name, attach, data_path, database_name, table_name,
				columns, context, primary_expr_list, date_column_name,
				sampling_expression, index_granularity, mode, sign_column_name);
		else
			return StorageMergeTree::create(data_path, database_name, table_name,
				columns, context, primary_expr_list, date_column_name,
				sampling_expression, index_granularity, mode, sign_column_name);
	}
	else
		throw Exception("Unknown storage " + name, ErrorCodes::UNKNOWN_STORAGE);
}


}
