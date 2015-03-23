#include <DB/DataStreams/RemoteBlockInputStream.h>
#include <DB/DataStreams/MaterializingBlockInputStream.h>

#include <DB/Storages/StorageDistributed.h>
#include <DB/Storages/VirtualColumnFactory.h>
#include <DB/Storages/Distributed/DistributedBlockOutputStream.h>
#include <DB/Storages/Distributed/DirectoryMonitor.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/Parsers/ASTInsertQuery.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/InterpreterAlterQuery.h>

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
	sharding_key_expr(sharding_key_ ? ExpressionAnalyzer(sharding_key_, context, *columns).getActions(false) : nullptr),
	sharding_key_column_name(sharding_key_ ? sharding_key_->getColumnName() : String{}),
	write_enabled(!data_path_.empty() && (cluster.getLocalNodesNum() + cluster.pools.size() < 2 || sharding_key_)),
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
	sharding_key_expr(sharding_key_ ? ExpressionAnalyzer(sharding_key_, context, *columns).getActions(false) : nullptr),
	sharding_key_column_name(sharding_key_ ? sharding_key_->getColumnName() : String{}),
	write_enabled(!data_path_.empty() && (cluster.getLocalNodesNum() + cluster.pools.size() < 2 || sharding_key_)),
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

	size_t result_size = (cluster.pools.size() * settings.max_parallel_replicas) + cluster.getLocalNodesNum();

	processed_stage = result_size == 1
		? QueryProcessingStage::Complete
		: QueryProcessingStage::WithMergeableState;

	BlockInputStreams res;
	const auto & modified_query_ast = rewriteSelectQuery(
		query, remote_database, remote_table);
	const auto & modified_query = queryToString(modified_query_ast);

	/// Ограничение сетевого трафика, если нужно.
	ThrottlerPtr throttler;
	if (settings.limits.max_network_bandwidth)
		throttler.reset(new Throttler(settings.limits.max_network_bandwidth));

	Tables external_tables = context.getExternalTables();

	/// Цикл по шардам.
	for (auto & conn_pool : cluster.pools)
		res.emplace_back(new RemoteBlockInputStream{
			conn_pool, modified_query, &new_settings, throttler,
			external_tables, processed_stage, context});

	/// Добавляем запросы к локальному ClickHouse.
	if (cluster.getLocalNodesNum() > 0)
	{
		DB::Context new_context = context;
		new_context.setSettings(new_settings);

		for (size_t i = 0; i < cluster.getLocalNodesNum(); ++i)
		{
			InterpreterSelectQuery interpreter(modified_query_ast, new_context, processed_stage);

			/** Материализация нужна, так как с удалённых серверов константы приходят материализованными.
			  * Если этого не делать, то в разных потоках будут получаться разные типы (Const и не-Const) столбцов,
			  *  а это не разрешено, так как весь код исходит из допущения, что в потоке блоков все типы одинаковые.
			  */
			res.emplace_back(new MaterializingBlockInputStream(interpreter.execute()));
		}
	}

	return res;
}

BlockOutputStreamPtr StorageDistributed::write(ASTPtr query)
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

}
