#include <DB/DataStreams/RemoteBlockInputStream.h>
#include <DB/DataStreams/MaterializingBlockInputStream.h>
#include <DB/DataStreams/BlockExtraInfoInputStream.h>

#include <DB/Storages/StorageDistributed.h>
#include <DB/Storages/VirtualColumnFactory.h>
#include <DB/Storages/Distributed/DistributedBlockOutputStream.h>
#include <DB/Storages/Distributed/DirectoryMonitor.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/Parsers/ASTInsertQuery.h>
#include <DB/Parsers/ASTSelectQuery.h>
#include <DB/Parsers/ASTIdentifier.h>
#include <DB/Parsers/TablePropertiesQueriesASTs.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <DB/Interpreters/InterpreterDescribeQuery.h>
#include <DB/Interpreters/ExpressionAnalyzer.h>

#include <DB/Core/Field.h>

#include <memory>

namespace DB
{

namespace
{
	/// select query has database and table names as AST pointers
	/// Создает копию запроса, меняет имена базы данных и таблицы.
	inline ASTPtr rewriteSelectQuery(const ASTPtr & query, const std::string & database, const std::string & table)
	{
		auto modified_query_ast = query->clone();

		auto & actual_query = typeid_cast<ASTSelectQuery &>(*modified_query_ast);
		actual_query.database = new ASTIdentifier{{}, database, ASTIdentifier::Database};
		actual_query.table = new ASTIdentifier{{}, table, ASTIdentifier::Table};

		return modified_query_ast;
	}

	/// insert query has database and table names as bare strings
	/// Создает копию запроса, меняет имена базы данных и таблицы.
	inline ASTPtr rewriteInsertQuery(const ASTPtr & query, const std::string & database, const std::string & table)
	{
		auto modified_query_ast = query->clone();

		auto & actual_query = typeid_cast<ASTInsertQuery &>(*modified_query_ast);
		actual_query.database = database;
		actual_query.table = table;
		/// make sure query is not INSERT SELECT
		actual_query.select = nullptr;

		return modified_query_ast;
	}

	BlockExtraInfo toBlockExtraInfo(const Cluster::Address & address)
	{
		BlockExtraInfo block_extra_info;
		block_extra_info.host = address.host_name;
		block_extra_info.resolved_address = address.resolved_address.toString();
		block_extra_info.port = address.port;
		block_extra_info.user = address.user;
		block_extra_info.is_valid = true;
		return block_extra_info;
	}
}


StorageDistributed::StorageDistributed(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & remote_database_,
	const String & remote_table_,
	Cluster & cluster_,
	Context & context_,
	const ASTPtr & sharding_key_,
	const String & data_path_)
	: name(name_), columns(columns_),
	remote_database(remote_database_), remote_table(remote_table_),
	context(context_), cluster(cluster_),
	sharding_key_expr(sharding_key_ ? ExpressionAnalyzer(sharding_key_, context, nullptr, *columns).getActions(false) : nullptr),
	sharding_key_column_name(sharding_key_ ? sharding_key_->getColumnName() : String{}),
	write_enabled(!data_path_.empty() && (((cluster.getLocalShardCount() + cluster.getRemoteShardCount()) < 2) || sharding_key_)),
	path(data_path_.empty() ? "" : (data_path_ + escapeForFileName(name) + '/'))
{
	createDirectoryMonitors();
}

StorageDistributed::StorageDistributed(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	const String & remote_database_,
	const String & remote_table_,
	Cluster & cluster_,
	Context & context_,
	const ASTPtr & sharding_key_,
	const String & data_path_)
	: IStorage{materialized_columns_, alias_columns_, column_defaults_},
	name(name_), columns(columns_),
	remote_database(remote_database_), remote_table(remote_table_),
	context(context_), cluster(cluster_),
	sharding_key_expr(sharding_key_ ? ExpressionAnalyzer(sharding_key_, context, nullptr, *columns).getActions(false) : nullptr),
	sharding_key_column_name(sharding_key_ ? sharding_key_->getColumnName() : String{}),
	write_enabled(!data_path_.empty() && (((cluster.getLocalShardCount() + cluster.getRemoteShardCount()) < 2) || sharding_key_)),
	path(data_path_.empty() ? "" : (data_path_ + escapeForFileName(name) + '/'))
{
	createDirectoryMonitors();
}

StoragePtr StorageDistributed::create(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const NamesAndTypesList & materialized_columns_,
	const NamesAndTypesList & alias_columns_,
	const ColumnDefaults & column_defaults_,
	const String & remote_database_,
	const String & remote_table_,
	const String & cluster_name,
	Context & context_,
	const ASTPtr & sharding_key_,
	const String & data_path_)
{
	context_.initClusters();

	return (new StorageDistributed{
		name_, columns_,
		materialized_columns_, alias_columns_, column_defaults_,
		remote_database_, remote_table_,
		context_.getCluster(cluster_name), context_,
		sharding_key_, data_path_
	})->thisPtr();
}


StoragePtr StorageDistributed::create(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & remote_database_,
	const String & remote_table_,
	SharedPtr<Cluster> & owned_cluster_,
	Context & context_)
{
	auto res = new StorageDistributed{
		name_, columns_, remote_database_,
		remote_table_, *owned_cluster_, context_
	};

	/// Захватываем владение объектом-кластером.
	res->owned_cluster = owned_cluster_;

	return res->thisPtr();
}

BlockInputStreams StorageDistributed::read(
	const Names & column_names,
	ASTPtr query,
	const Context & context,
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	const size_t max_block_size,
	const unsigned threads)
{
	Settings new_settings = settings;
	new_settings.queue_max_wait_ms = Cluster::saturate(new_settings.queue_max_wait_ms, settings.limits.max_execution_time);
	/// Не имеет смысла на удалённых серверах, так как запрос отправляется обычно с другим user-ом.
	new_settings.max_concurrent_queries_for_user = 0;

	size_t result_size = (cluster.getRemoteShardCount() * settings.max_parallel_replicas) + cluster.getLocalShardCount();

	processed_stage = result_size == 1 || settings.distributed_group_by_no_merge
		? QueryProcessingStage::Complete
		: QueryProcessingStage::WithMergeableState;

	BlockInputStreams res;
	const auto & modified_query_ast = rewriteSelectQuery(
		query, remote_database, remote_table);
	const auto & modified_query = queryToString(modified_query_ast);

	/// Ограничение сетевого трафика, если нужно.
	ThrottlerPtr throttler;
	if (settings.limits.max_network_bandwidth || settings.limits.max_network_bytes)
		throttler.reset(new Throttler(
			settings.limits.max_network_bandwidth,
			settings.limits.max_network_bytes,
			"Limit for bytes to send or receive over network exceeded."));

	Tables external_tables;

	if (settings.global_subqueries_method == GlobalSubqueriesMethod::PUSH)
		external_tables = context.getExternalTables();

	/// Распределить шарды равномерно по потокам.

	size_t remote_count = cluster.getRemoteShardCount();

	/// Отключаем мультиплексирование шардов, если есть ORDER BY без GROUP BY.
	//const ASTSelectQuery & ast = *(static_cast<const ASTSelectQuery *>(modified_query_ast.get()));

	/** Функциональность shard_multiplexing не доделана - выключаем её.
	  * (Потому что установка соединений с разными шардами в рамках одного потока выполняется не параллельно.)
	  * Подробнее смотрите в https://███████████.yandex-team.ru/METR-18300
	  */
	//bool enable_shard_multiplexing = !(ast.order_expression_list && !ast.group_expression_list);
	bool enable_shard_multiplexing = false;

	size_t thread_count;

	if (!enable_shard_multiplexing)
		thread_count = remote_count;
	else if (remote_count == 0)
		thread_count = 0;
	else if (settings.max_distributed_processing_threads == 0)
		thread_count = 1;
	else
		thread_count = std::min(remote_count, static_cast<size_t>(settings.max_distributed_processing_threads));

	size_t pools_per_thread = (thread_count > 0) ? (remote_count / thread_count) : 0;
	size_t remainder = (thread_count > 0) ? (remote_count % thread_count) : 0;

	ConnectionPoolsPtr pools;
	bool do_init = true;

	/// Цикл по шардам.
	size_t current_thread = 0;
	for (const auto & shard_info : cluster.getShardsInfo())
	{
		if (shard_info.isLocal())
		{
			/// Добавляем запросы к локальному ClickHouse.

			DB::Context new_context = context;
			new_context.setSettings(new_settings);

			for (size_t i = 0; i < shard_info.local_addresses.size(); ++i)
			{
				InterpreterSelectQuery interpreter(modified_query_ast, new_context, processed_stage);

				/** Материализация нужна, так как с удалённых серверов константы приходят материализованными.
				  * Если этого не делать, то в разных потоках будут получаться разные типы (Const и не-Const) столбцов,
				  * а это не разрешено, так как весь код исходит из допущения, что в потоке блоков все типы одинаковые.
				  */
				res.emplace_back(new MaterializingBlockInputStream(interpreter.execute().in));
			}
		}
		else
		{
			size_t excess = (current_thread < remainder) ? 1 : 0;
			size_t actual_pools_per_thread = pools_per_thread + excess;

			if (actual_pools_per_thread == 1)
			{
				res.emplace_back(new RemoteBlockInputStream{
					shard_info.pool, modified_query, &new_settings, throttler,
					external_tables, processed_stage, context});
				++current_thread;
			}
			else
			{
				if (do_init)
				{
					pools = new ConnectionPools;
					do_init = false;
				}

				pools->push_back(shard_info.pool);
				if (pools->size() == actual_pools_per_thread)
				{
					res.emplace_back(new RemoteBlockInputStream{
						pools, modified_query, &new_settings, throttler,
						external_tables, processed_stage, context});
					do_init = true;
					++current_thread;
				}
			}
		}
	}

	return res;
}

BlockOutputStreamPtr StorageDistributed::write(ASTPtr query, const Settings & settings)
{
	if (!write_enabled)
		throw Exception{
			"Method write is not supported by storage " + getName() +
			" with more than one shard and no sharding key provided",
			ErrorCodes::STORAGE_REQUIRES_PARAMETER
		};

	return new DistributedBlockOutputStream{
		*this,
		rewriteInsertQuery(query, remote_database, remote_table)
	};
}

void StorageDistributed::alter(const AlterCommands & params, const String & database_name, const String & table_name, Context & context)
{
	auto lock = lockStructureForAlter();
	params.apply(*columns, materialized_columns, alias_columns, column_defaults);
	InterpreterAlterQuery::updateMetadata(database_name, table_name,
		*columns, materialized_columns, alias_columns, column_defaults, context);
}

void StorageDistributed::shutdown()
{
	directory_monitors.clear();
}

BlockInputStreams StorageDistributed::describe(const Context & context, const Settings & settings)
{
	Settings new_settings = settings;
	new_settings.queue_max_wait_ms = Cluster::saturate(new_settings.queue_max_wait_ms, settings.limits.max_execution_time);
	/// Не имеет смысла на удалённых серверах, так как запрос отправляется обычно с другим user-ом.
	new_settings.max_concurrent_queries_for_user = 0;

	/// Создать запрос DESCRIBE TABLE.

	auto describe_query = new ASTDescribeQuery;
	describe_query->database = remote_database;
	describe_query->table = remote_table;

	ASTPtr ast = describe_query;
	const auto query = queryToString(ast);

	/// Ограничение сетевого трафика, если нужно.
	ThrottlerPtr throttler;
	if (settings.limits.max_network_bandwidth || settings.limits.max_network_bytes)
		throttler.reset(new Throttler(
			settings.limits.max_network_bandwidth,
			settings.limits.max_network_bytes,
			"Limit for bytes to send or receive over network exceeded."));

	BlockInputStreams res;

	/// Распределить шарды равномерно по потокам.

	size_t remote_count = 0;
	for (const auto & shard_info : cluster.getShardsInfo())
	{
		if (shard_info.hasRemoteConnections())
			++remote_count;
	}

	size_t thread_count;

	/** Функциональность shard_multiplexing не доделана - выключаем её.
	  * (Потому что установка соединений с разными шардами в рамках одного потока выполняется не параллельно.)
	  * Подробнее смотрите в https://███████████.yandex-team.ru/METR-18300
	  */

/*	if (remote_count == 0)
		thread_count = 0;
	else if (settings.max_distributed_processing_threads == 0)
		thread_count = 1;
	else
		thread_count = std::min(remote_count, static_cast<size_t>(settings.max_distributed_processing_threads));
	*/
	thread_count = remote_count;

	size_t pools_per_thread = (thread_count > 0) ? (remote_count / thread_count) : 0;
	size_t remainder = (thread_count > 0) ?  (remote_count % thread_count) : 0;

	ConnectionPoolsPtr pools;
	bool do_init = true;

	/// Цикл по шардам.
	size_t current_thread = 0;
	for (const auto & shard_info : cluster.getShardsInfo())
	{
		if (shard_info.isLocal())
		{
			/// Добавляем запросы к локальному ClickHouse.

			DB::Context new_context = context;
			new_context.setSettings(new_settings);

			for (const auto & address : shard_info.local_addresses)
			{
				InterpreterDescribeQuery interpreter(ast, new_context);
				BlockInputStreamPtr stream = new MaterializingBlockInputStream(interpreter.execute().in);
				stream = new BlockExtraInfoInputStream(stream, toBlockExtraInfo(address));
				res.emplace_back(stream);
			}
		}

		if (shard_info.hasRemoteConnections())
		{
			size_t excess = (current_thread < remainder) ? 1 : 0;
			size_t actual_pools_per_thread = pools_per_thread + excess;

			if (actual_pools_per_thread == 1)
			{
				auto stream = new RemoteBlockInputStream{shard_info.pool, query, &new_settings, throttler};
				stream->doBroadcast();
				stream->appendExtraInfo();
				res.emplace_back(stream);
				++current_thread;
			}
			else
			{
				if (do_init)
				{
					pools = new ConnectionPools;
					do_init = false;
				}

				pools->push_back(shard_info.pool);
				if (pools->size() == actual_pools_per_thread)
				{
					auto stream = new RemoteBlockInputStream{pools, query, &new_settings, throttler};
					stream->doBroadcast();
					stream->appendExtraInfo();
					res.emplace_back(stream);

					do_init = true;
					++current_thread;
				}
			}
		}
	}

	return res;
}

NameAndTypePair StorageDistributed::getColumn(const String & column_name) const
{
	if (const auto & type = VirtualColumnFactory::tryGetType(column_name))
		return { column_name, type };

	return getRealColumn(column_name);
}

bool StorageDistributed::hasColumn(const String & column_name) const
{
	return VirtualColumnFactory::hasColumn(column_name) || IStorage::hasColumn(column_name);
}

void StorageDistributed::createDirectoryMonitor(const std::string & name)
{
	directory_monitors.emplace(name, std::make_unique<DirectoryMonitor>(*this, name));
}

void StorageDistributed::createDirectoryMonitors()
{
	if (path.empty())
		return;

	Poco::File{path}.createDirectory();

	Poco::DirectoryIterator end;
	for (Poco::DirectoryIterator it{path}; it != end; ++it)
		if (it->isDirectory())
			createDirectoryMonitor(it.name());
}

void StorageDistributed::requireDirectoryMonitor(const std::string & name)
{
	if (!directory_monitors.count(name))
		createDirectoryMonitor(name);
}

size_t StorageDistributed::getShardCount() const
{
	return cluster.getRemoteShardCount();
}

}
