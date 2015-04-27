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
#include <DB/Storages/StorageBuffer.h>
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
#include <DB/Storages/StorageSet.h>
#include <DB/Storages/StorageJoin.h>


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
static ASTPtr extractPrimaryKey(const ASTPtr & node)
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

/** Для StorageMergeTree: достать список имён столбцов.
  * Он может быть указан в кортеже: (Clicks, Cost),
  *  или как один столбец: Clicks.
  */
static Names extractColumnNames(const ASTPtr & node)
{
	const ASTFunction * expr_func = typeid_cast<const ASTFunction *>(&*node);

	if (expr_func && expr_func->name == "tuple")
	{
		const auto & elements = expr_func->children.at(0)->children;
		Names res;
		res.reserve(elements.size());
		for (const auto & elem : elements)
			res.push_back(typeid_cast<const ASTIdentifier &>(*elem).name);

		return res;
	}
	else
	{
		return { typeid_cast<const ASTIdentifier &>(*node).name };
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
	const NamesAndTypesList & materialized_columns,
	const NamesAndTypesList & alias_columns,
	const ColumnDefaults & column_defaults,
	bool attach) const
{
	if (name == "Log")
	{
		return StorageLog::create(
			data_path, table_name, columns,
			materialized_columns, alias_columns, column_defaults,
			context.getSettings().max_compress_block_size);
	}
	else if (name == "Chunks")
	{
		return StorageChunks::create(
			data_path, table_name, database_name, columns,
			materialized_columns, alias_columns, column_defaults,
			context, attach);
	}
	else if (name == "ChunkRef")
	{
		throw Exception("Table with storage ChunkRef must not be created manually.", ErrorCodes::TABLE_MUST_NOT_BE_CREATED_MANUALLY);
	}
	else if (name == "View")
	{
		return StorageView::create(
			table_name, database_name, context, query, columns,
			materialized_columns, alias_columns, column_defaults);
	}
	else if (name == "MaterializedView")
	{
		return StorageMaterializedView::create(
			table_name, database_name, context, query, columns,
			materialized_columns, alias_columns, column_defaults,
			attach);
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

			return StorageChunkMerger::create(
				database_name, table_name, columns,
				materialized_columns, alias_columns, column_defaults,
				source_database, source_table_name_regexp,
				destination_name_prefix, chunks_to_merge, context);
		} while (false);

		throw Exception("Storage ChunkMerger requires from 3 to 4 parameters:"
			" source database, regexp for source table names, number of chunks to merge, [destination tables name prefix].",
			ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
	}
	else if (name == "TinyLog")
	{
		return StorageTinyLog::create(
			data_path, table_name, columns,
			materialized_columns, alias_columns, column_defaults,
			attach, context.getSettings().max_compress_block_size);
	}
	else if (name == "Set")
	{
		return StorageSet::create(
			data_path, table_name, columns,
			materialized_columns, alias_columns, column_defaults);
	}
	else if (name == "Join")
	{
		/// Join(ANY, LEFT, k1, k2, ...)

		ASTs & args_func = typeid_cast<ASTFunction &>(*typeid_cast<ASTCreateQuery &>(*query).storage).children;

		constexpr auto params_error_message = "Storage Join requires at least 3 parameters: Join(ANY|ALL, LEFT|INNER, keys...).";

		if (args_func.size() != 1)
			throw Exception(params_error_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

		if (args.size() < 3)
			throw Exception(params_error_message, ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		const ASTIdentifier * strictness_id = typeid_cast<ASTIdentifier *>(&*args[0]);
		if (!strictness_id)
			throw Exception("First parameter of storage Join must be ANY or ALL (without quotes).", ErrorCodes::BAD_ARGUMENTS);

		const String strictness_str = Poco::toLower(strictness_id->name);
		ASTJoin::Strictness strictness;
		if (strictness_str == "any")
			strictness = ASTJoin::Strictness::Any;
		else if (strictness_str == "all")
			strictness = ASTJoin::Strictness::All;
		else
			throw Exception("First parameter of storage Join must be ANY or ALL (without quotes).", ErrorCodes::BAD_ARGUMENTS);

		const ASTIdentifier * kind_id = typeid_cast<ASTIdentifier *>(&*args[1]);
		if (!kind_id)
			throw Exception("Second parameter of storage Join must be LEFT or INNER (without quotes).", ErrorCodes::BAD_ARGUMENTS);

		const String kind_str = Poco::toLower(kind_id->name);
		ASTJoin::Kind kind;
		if (kind_str == "left")
			kind = ASTJoin::Kind::Left;
		else if (kind_str == "inner")
			kind = ASTJoin::Kind::Inner;
		else if (kind_str == "right")
			kind = ASTJoin::Kind::Right;
		else if (kind_str == "full")
			kind = ASTJoin::Kind::Full;
		else
			throw Exception("Second parameter of storage Join must be LEFT or INNER or RIGHT or FULL (without quotes).", ErrorCodes::BAD_ARGUMENTS);

		Names key_names;
		key_names.reserve(args.size() - 2);
		for (size_t i = 2, size = args.size(); i < size; ++i)
		{
			const ASTIdentifier * key = typeid_cast<ASTIdentifier *>(&*args[i]);
			if (!key)
				throw Exception("Parameter №" + toString(i + 1) + " of storage Join don't look like column name.", ErrorCodes::BAD_ARGUMENTS);

			key_names.push_back(key->name);
		}

		return StorageJoin::create(
			data_path, table_name,
			key_names, kind, strictness,
			columns, materialized_columns, alias_columns, column_defaults);
	}
	else if (name == "Memory")
	{
		return StorageMemory::create(table_name, columns, materialized_columns, alias_columns, column_defaults);
	}
	else if (name == "Null")
	{
		return StorageNull::create(table_name, columns, materialized_columns, alias_columns, column_defaults);
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

		return StorageMerge::create(
			table_name, columns,
			materialized_columns, alias_columns, column_defaults,
			source_database, table_name_regexp, context);
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
			table_name, columns,
			materialized_columns, alias_columns, column_defaults,
			remote_database, remote_table, cluster_name,
			context, sharding_key, data_path);
	}
	else if (name == "Buffer")
	{
		/** Buffer(db, table, num_buckets, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes)
		  *
		  * db, table - в какую таблицу сбрасывать данные из буфера.
		  * num_buckets - уровень параллелизма.
		  * min_time, max_time, min_rows, max_rows, min_bytes, max_bytes - условия вытеснения из буфера.
		  */

		ASTs & args_func = typeid_cast<ASTFunction &>(*typeid_cast<ASTCreateQuery &>(*query).storage).children;

		if (args_func.size() != 1)
			throw Exception("Storage Buffer requires 9 parameters: "
				" destination database, destination table, num_buckets, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		ASTs & args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

		if (args.size() != 9)
			throw Exception("Storage Buffer requires 9 parameters: "
				" destination_database, destination_table, num_buckets, min_time, max_time, min_rows, max_rows, min_bytes, max_bytes.",
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);

		String destination_database = reinterpretAsIdentifier(args[0], local_context).name;
		String destination_table 	= typeid_cast<ASTIdentifier &>(*args[1]).name;

		size_t num_buckets = apply_visitor(FieldVisitorConvertToNumber<size_t>(), typeid_cast<ASTLiteral &>(*args[2]).value);

		time_t min_time = apply_visitor(FieldVisitorConvertToNumber<size_t>(), typeid_cast<ASTLiteral &>(*args[3]).value);
		time_t max_time = apply_visitor(FieldVisitorConvertToNumber<size_t>(), typeid_cast<ASTLiteral &>(*args[4]).value);
		size_t min_rows = apply_visitor(FieldVisitorConvertToNumber<size_t>(), typeid_cast<ASTLiteral &>(*args[5]).value);
		size_t max_rows = apply_visitor(FieldVisitorConvertToNumber<size_t>(), typeid_cast<ASTLiteral &>(*args[6]).value);
		size_t min_bytes = apply_visitor(FieldVisitorConvertToNumber<size_t>(), typeid_cast<ASTLiteral &>(*args[7]).value);
		size_t max_bytes = apply_visitor(FieldVisitorConvertToNumber<size_t>(), typeid_cast<ASTLiteral &>(*args[8]).value);

		return StorageBuffer::create(
			table_name, columns, context,
			num_buckets, {min_time, min_rows, min_bytes}, {max_time, max_rows, max_bytes},
			destination_database, destination_table);
	}
	else if (endsWith(name, "MergeTree"))
	{
		/** Движки [Replicated][|Summing|Collapsing|Aggregating|Unsorted]MergeTree (2 * 5 комбинаций)
		  * В качестве аргумента для движка должно быть указано:
		  *  - (для Replicated) Путь к таблице в ZooKeeper
		  *  - (для Replicated) Имя реплики в ZooKeeper
		  *  - имя столбца с датой;
		  *  - (не обязательно) выражение для семплирования (запрос с SAMPLE x будет выбирать строки, у которых в этом столбце значение меньше, чем x*UINT32_MAX);
		  *  - выражение для сортировки (либо скалярное выражение, либо tuple из нескольких);
		  *  - index_granularity;
		  *  - (для Collapsing) имя столбца, содержащего тип строчки с изменением "визита" (принимающего значения 1 и -1).
		  * Например: ENGINE = ReplicatedCollapsingMergeTree('/tables/mytable', 'rep02', EventDate, (CounterID, EventDate, intHash32(UniqID), VisitID), 8192, Sign).
		  *  - (для Summing, не обязательно) кортеж столбцов, которых следует суммировать. Если не задано - используются все числовые столбцы, не входящие в первичный ключ.
		  * Например: ENGINE = ReplicatedCollapsingMergeTree('/tables/mytable', 'rep02', EventDate, (CounterID, EventDate, intHash32(UniqID), VisitID), 8192, Sign).
		  *
		  * MergeTree(date, [sample_key], primary_key, index_granularity)
		  * CollapsingMergeTree(date, [sample_key], primary_key, index_granularity, sign)
		  * SummingMergeTree(date, [sample_key], primary_key, index_granularity, [columns_to_sum])
		  * AggregatingMergeTree(date, [sample_key], primary_key, index_granularity)
		  * UnsortedMergeTree(date, index_granularity)	TODO Добавить описание ниже.
		  */

		const char * verbose_help = R"(

MergeTree is family of storage engines.

MergeTrees is different in two ways:
- it may be replicated and non-replicated;
- it may do different actions on merge: nothing; sign collapse; sum; apply aggregete functions.

So we have 8 combinations:
    MergeTree, CollapsingMergeTree, SummingMergeTree, AggregatingMergeTree,
    ReplicatedMergeTree, ReplicatedCollapsingMergeTree, ReplicatedSummingMergeTree, ReplicatedAggregatingMergeTree.

In most of cases, you need MergeTree or ReplicatedMergeTree.

For replicated merge trees, you need to supply path in ZooKeeper and replica name as first two parameters.
Path in ZooKeeper is like '/clickhouse/tables/01/' where /clickhouse/tables/ is common prefix and 01 is shard name.
Replica name is like 'mtstat01-1' - it may be hostname or any suitable string identifying replica.
You may use macro substitutions for these parameters. It's like ReplicatedMergeTree('/clickhouse/tables/{shard}/', '{replica}'...
Look at <macros> section in server configuration file.

Next parameter (which is first for unreplicated tables and third for replicated tables) is name of date column.
Date column must exist in table and have type Date (not DateTime).
It is used for internal data partitioning and works like some kind of index.

If your source data doesn't have column of type Date, but have DateTime column, you may add values for Date column while loading,
 or you may INSERT your source data to table of type Log and then transform it with INSERT INTO t SELECT toDate(time) AS date, * FROM ...
If your source data doesn't have any date or time, you may just pass any constant for date column while loading.

Next parameter is optional sampling expression. Sampling expression is used to implement SAMPLE clause in query for approximate query execution.
If you don't need approximate query execution, simply omit this parameter.
Sample expression must be one of elements of primary key tuple. For example, if your primary key is (CounterID, EventDate, intHash64(UserID)), your sampling expression might be intHash64(UserID).

Next parameter is primary key tuple. It's like (CounterID, EventDate, intHash64(UserID)) - list of column names or functional expressions in round brackets. If your primary key have just one element, you may omit round brackets.

Careful choice of primary key is extremely important for processing short-time queries.

Next parameter is index (primary key) granularity. Good value is 8192. You have no reasons to use any other value.

For Collapsing mode, last parameter is name of sign column - special column that is used to 'collapse' rows with same primary key while merge.

For Summing mode, last parameter is optional list of columns to sum while merge. List is passed in round brackets, like (PageViews, Cost).
If this parameter is omitted, storage will sum all numeric columns except columns participated in primary key.


Examples:

MergeTree(EventDate, (CounterID, EventDate), 8192)

MergeTree(EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192)

CollapsingMergeTree(StartDate, intHash32(UserID), (CounterID, StartDate, intHash32(UserID), VisitID), 8192, Sign)

SummingMergeTree(EventDate, (OrderID, EventDate, BannerID, PhraseID, ContextType, RegionID, PageID, IsFlat, TypeID, ResourceNo), 8192)

SummingMergeTree(EventDate, (OrderID, EventDate, BannerID, PhraseID, ContextType, RegionID, PageID, IsFlat, TypeID, ResourceNo), 8192, (Shows, Clicks, Cost, CostCur, ShowsSumPosition, ClicksSumPosition, SessionNum, SessionLen, SessionCost, GoalsNum, SessionDepth))

ReplicatedMergeTree('/clickhouse/tables/{layer}-{shard}/hits', '{replica}', EventDate, intHash32(UserID), (CounterID, EventDate, intHash32(UserID), EventTime), 8192)


For further info please read the documentation: http://clickhouse.yandex-team.ru/
)";

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
		else if (name_part == "Unsorted")
			mode = MergeTreeData::Unsorted;
		else if (!name_part.empty())
			throw Exception("Unknown storage " + name + verbose_help, ErrorCodes::UNKNOWN_STORAGE);

		ASTs & args_func = typeid_cast<ASTFunction &>(*typeid_cast<ASTCreateQuery &>(*query).storage).children;

		ASTs args;

		if (args_func.size() == 1)
			args = typeid_cast<ASTExpressionList &>(*args_func.at(0)).children;

		/// NOTE Слегка запутанно.
		size_t num_additional_params = (replicated ? 2 : 0) + (mode == MergeTreeData::Collapsing);

		if (mode == MergeTreeData::Unsorted
			&& args.size() != num_additional_params + 2)
		{
			String params;

			if (replicated)
				params +=
				"\npath in ZooKeeper,"
				"\nreplica name,";

			params +=
				"\nname of column with date,"
				"\nindex granularity\n";

			throw Exception("Storage " + name + " requires "
				+ toString(num_additional_params + 2) + " parameters: " + params + verbose_help,
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		}

		if (mode != MergeTreeData::Summing && mode != MergeTreeData::Unsorted
			&& args.size() != num_additional_params + 3
			&& args.size() != num_additional_params + 4)
		{
			String params;

			if (replicated)
				params +=
					"\npath in ZooKeeper,"
					"\nreplica name,";

			params +=
				"\nname of column with date,"
				"\n[sampling element of primary key],"
				"\nprimary key expression,"
				"\nindex granularity\n";

			if (mode == MergeTreeData::Collapsing)
				params += ", sign column";

			throw Exception("Storage " + name + " requires "
				+ toString(num_additional_params + 3) + " or "
				+ toString(num_additional_params + 4) + " parameters: " + params + verbose_help,
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		}

		if (mode == MergeTreeData::Summing
			&& args.size() != num_additional_params + 3
			&& args.size() != num_additional_params + 4
			&& args.size() != num_additional_params + 5)
		{
			String params;

			if (replicated)
				params +=
					"\npath in ZooKeeper,"
					"\nreplica name,";

			params +=
				"\nname of column with date,"
				"\n[sampling element of primary key],"
				"\nprimary key expression,"
				"\nindex granularity,"
				"\n[list of columns to sum]\n";

			throw Exception("Storage " + name + " requires "
				+ toString(num_additional_params + 3) + " or "
				+ toString(num_additional_params + 4) + " or "
				+ toString(num_additional_params + 5) + " parameters: " + params + verbose_help,
				ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
		}

		/// Для Replicated.
		String zookeeper_path;
		String replica_name;

		/// Для всех.
		String date_column_name;
		ASTPtr primary_expr_list;
		ASTPtr sampling_expression;
		UInt64 index_granularity;

		/// Для Collapsing.
		String sign_column_name;

		/// Для Summing.
		Names columns_to_sum;

		if (replicated)
		{
			auto ast = typeid_cast<ASTLiteral *>(&*args[0]);
			if (ast && ast->value.getType() == Field::Types::String)
				zookeeper_path = safeGet<String>(ast->value);
			else
				throw Exception(String("Path in ZooKeeper must be a string literal") + verbose_help, ErrorCodes::BAD_ARGUMENTS);

			ast = typeid_cast<ASTLiteral *>(&*args[1]);
			if (ast && ast->value.getType() == Field::Types::String)
				replica_name = safeGet<String>(ast->value);
			else
				throw Exception(String("Replica name must be a string literal") + verbose_help, ErrorCodes::BAD_ARGUMENTS);

			if (replica_name.empty())
				throw Exception(String("No replica name in config") + verbose_help, ErrorCodes::NO_REPLICA_NAME_GIVEN);

			args.erase(args.begin(), args.begin() + 2);
		}

		if (mode == MergeTreeData::Collapsing)
		{
			if (auto ast = typeid_cast<ASTIdentifier *>(&*args.back()))
				sign_column_name = ast->name;
			else
				throw Exception(String("Sign column name must be an unquoted string") + verbose_help, ErrorCodes::BAD_ARGUMENTS);

			args.pop_back();
		}
		else if (mode == MergeTreeData::Summing)
		{
			/// Если последний элемент - не index granularity (литерал), то это - список суммируемых столбцов.
			if (!typeid_cast<const ASTLiteral *>(&*args.back()))
			{
				columns_to_sum = extractColumnNames(args.back());
				args.pop_back();
			}
		}

		/// Если присутствует выражение для сэмплирования. MergeTree(date, [sample_key], primary_key, index_granularity)
		if (args.size() == 4)
		{
			sampling_expression = args[1];
			args.erase(args.begin() + 1);
		}

		/// Теперь осталось только три параметра - date, primary_key, index_granularity.

		if (auto ast = typeid_cast<ASTIdentifier *>(&*args[0]))
			date_column_name = ast->name;
		else
			throw Exception(String("Date column name must be an unquoted string") + verbose_help, ErrorCodes::BAD_ARGUMENTS);

		if (mode != MergeTreeData::Unsorted)
			primary_expr_list = extractPrimaryKey(args[1]);

		auto ast = typeid_cast<ASTLiteral *>(&*args.back());
		if (ast && ast->value.getType() == Field::Types::UInt64)
				index_granularity = safeGet<UInt64>(ast->value);
		else
			throw Exception(String("Index granularity must be a positive integer") + verbose_help, ErrorCodes::BAD_ARGUMENTS);

		if (replicated)
			return StorageReplicatedMergeTree::create(
				zookeeper_path, replica_name, attach, data_path, database_name, table_name,
				columns, materialized_columns, alias_columns, column_defaults,
				context, primary_expr_list, date_column_name,
				sampling_expression, index_granularity, mode, sign_column_name, columns_to_sum);
		else
			return StorageMergeTree::create(
				data_path, database_name, table_name,
				columns, materialized_columns, alias_columns, column_defaults,
				context, primary_expr_list, date_column_name,
				sampling_expression, index_granularity, mode, sign_column_name, columns_to_sum);
	}
	else
		throw Exception("Unknown storage " + name, ErrorCodes::UNKNOWN_STORAGE);
}


}
