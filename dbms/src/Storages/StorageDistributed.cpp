#include <DB/Parsers/formatAST.h>

#include <DB/DataStreams/RemoteBlockInputStream.h>
#include <DB/DataStreams/RemoveColumnsBlockInputStream.h>

#include <DB/Storages/StorageDistributed.h>
#include <DB/Storages/VirtualColumnFactory.h>
#include <DB/Storages/Distributed/DistributedBlockOutputStream.h>

#include <Poco/Net/NetworkInterface.h>
#include <DB/Client/ConnectionPool.h>

#include <DB/Common/escapeForFileName.h>

#include <DB/Interpreters/InterpreterSelectQuery.h>
#include <DB/Interpreters/InterpreterAlterQuery.h>
#include <boost/bind.hpp>
#include <DB/Core/Field.h>

namespace DB
{

namespace {
	template <typename ASTType>
	inline std::string queryToString(const ASTPtr & query)
	{
		const auto & query_ast = typeid_cast<const ASTType &>(*query);

		std::ostringstream s;
		formatAST(query_ast, s, 0, false, true);

		return s.str();
	}

	/// select and insert query have different types for database and table, hence two specializations
	template <typename ASTType> struct rewrite_traits;
	template <> struct rewrite_traits<ASTSelectQuery> { using type = ASTPtr; };
	template <> struct rewrite_traits<ASTInsertQuery> { using type = const std::string &; };

	template <typename ASTType>
	typename rewrite_traits<ASTType>::type rewrite(const std::string & name, const ASTIdentifier::Kind kind) = delete;

	/// select query has database and table names as AST pointers
	template <>
	inline ASTPtr rewrite<ASTSelectQuery>(const std::string & name, const ASTIdentifier::Kind kind)
	{
		return new ASTIdentifier{{}, name, kind};
	}

	/// insert query has database and table names as bare strings
	template <>
	inline const std::string & rewrite<ASTInsertQuery>(const std::string & name, ASTIdentifier::Kind)
	{
		return name;
	}

	/// Создает копию запроса, меняет имена базы данных и таблицы.
	template <typename ASTType>
	inline ASTPtr rewriteQuery(const ASTPtr & query, const std::string & database, const std::string & table)
	{		
		/// Создаем копию запроса.
		auto modified_query_ast = query->clone();

		/// Меняем имена таблицы и базы данных
		auto & modified_query = typeid_cast<ASTType &>(*modified_query_ast);
		modified_query.database = rewrite<ASTType>(database, ASTIdentifier::Database);
		modified_query.table = rewrite<ASTType>(table, ASTIdentifier::Table);

		/// copy elision and RVO will work as intended, but let's be more explicit
		return std::move(modified_query_ast);
	}
}


StorageDistributed::StorageDistributed(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & remote_database_,
	const String & remote_table_,
	Cluster & cluster_,
	const Context & context_,
	const ASTPtr & sharding_key_,
	const String & data_path_)
	: name(name_), columns(columns_),
	remote_database(remote_database_), remote_table(remote_table_),
	context(context_), cluster(cluster_),
	sharding_key_expr(sharding_key_ ? ExpressionAnalyzer(sharding_key_, context, *columns).getActions(false) : nullptr),
	sharding_key_column_name(sharding_key_ ? sharding_key_->getColumnName() : String{}),
	write_enabled(cluster.getLocalNodesNum() + cluster.pools.size() < 2 || sharding_key_),
	path(data_path_ + escapeForFileName(name) + '/')
{
	std::cout << "table `" << name << "` in " << path << std::endl;
	for (auto & shard_info : cluster.shard_info_vec) {
		std::cout
			<< "\twill write to " << path + shard_info.dir_name
			<< " with weight " << shard_info.weight
			<< std::endl;
	}

	createDirectoryMonitors();
}

StoragePtr StorageDistributed::create(
	const std::string & name_,
	NamesAndTypesListPtr columns_,
	const String & remote_database_,
	const String & remote_table_,
	const String & cluster_name,
	Context & context_,
	const ASTPtr & sharding_key_,
	const String & data_path_)
{
	context_.initClusters();

	return (new StorageDistributed{
		name_, columns_, remote_database_, remote_table_,
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
	const Settings & settings,
	QueryProcessingStage::Enum & processed_stage,
	size_t max_block_size,
	unsigned threads)
{
	Settings new_settings = settings;
	new_settings.queue_max_wait_ms = Cluster::saturate(new_settings.queue_max_wait_ms, settings.limits.max_execution_time);

	size_t result_size = cluster.pools.size() + cluster.getLocalNodesNum();

	processed_stage = result_size == 1
		? QueryProcessingStage::Complete
		: QueryProcessingStage::WithMergeableState;

	BlockInputStreams res;
	const auto & modified_query_ast = rewriteQuery<ASTSelectQuery>(
		query, remote_database, remote_table
	);
	const auto & modified_query = queryToString<ASTSelectQuery>(modified_query_ast);

	/// Цикл по шардам.
	for (auto & conn_pool : cluster.pools)
		res.emplace_back(new RemoteBlockInputStream{
			conn_pool, modified_query, &new_settings,
			external_tables, processed_stage
		});

	/// Добавляем запросы к локальному ClickHouse.
	if (cluster.getLocalNodesNum() > 0)
	{
		DB::Context new_context = context;
		new_context.setSettings(new_settings);
		for (auto & it : external_tables)
			if (!new_context.tryGetExternalTable(it.first))
				new_context.addExternalTable(it.first, it.second);

		for(size_t i = 0; i < cluster.getLocalNodesNum(); ++i)
		{
			InterpreterSelectQuery interpreter(modified_query_ast, new_context, processed_stage);
				res.push_back(interpreter.execute());
		}
	}

	external_tables.clear();
	return res;
}

BlockOutputStreamPtr StorageDistributed::write(ASTPtr query)
{
	if (!write_enabled)
		throw Exception{
			"Method write is not supported by storage " + getName() + " with no sharding key provided",
			ErrorCodes::NOT_IMPLEMENTED
		};

	return new DistributedBlockOutputStream{
		*this, this->cluster,
		queryToString<ASTInsertQuery>(rewriteQuery<ASTInsertQuery>(
			query, remote_database, remote_table
		))
	};
}

void StorageDistributed::alter(const AlterCommands & params, const String & database_name, const String & table_name, Context & context)
{
	auto lock = lockStructureForAlter();
	params.apply(*columns);
	InterpreterAlterQuery::updateMetadata(database_name, table_name, *columns, context);
}

void StorageDistributed::shutdown()
{
	quit.store(true, std::memory_order_relaxed);

	for (auto & name_thread_pair : directory_monitor_threads)
		name_thread_pair.second.join();
}

NameAndTypePair StorageDistributed::getColumn(const String & column_name) const
{
	auto type = VirtualColumnFactory::tryGetType(column_name);
	if (type)
		return NameAndTypePair(column_name, type);

	return getRealColumn(column_name);
}

bool StorageDistributed::hasColumn(const String & column_name) const
{
	return VirtualColumnFactory::hasColumn(column_name) || hasRealColumn(column_name);
}

void StorageDistributed::createDirectoryMonitors()
{
	Poco::File(path).createDirectory();

	Poco::DirectoryIterator end;
	for (Poco::DirectoryIterator it(path); it != end; ++it)
		if (it->isDirectory())
			createDirectoryMonitor(it.name());
}

void StorageDistributed::createDirectoryMonitor(const std::string & name)
{
	if (directory_monitor_threads.count(name))
		return;

	directory_monitor_threads.emplace(
		name, 
		std::thread{
			&StorageDistributed::directoryMonitorFunc, this, path + name + '/'
		}
	);
}

void StorageDistributed::directoryMonitorFunc(const std::string & path)
{
	std::cout << "created monitor for directory " << path << std::endl;

	while (!quit.load(std::memory_order_relaxed))
	{
	}

	std::cout << "exiting monitor for directory " << path << std::endl;
}

}
