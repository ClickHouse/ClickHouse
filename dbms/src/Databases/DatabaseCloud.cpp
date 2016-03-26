#include <zkutil/ZooKeeper.h>
#include <DB/Interpreters/Context.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/Common/SipHash.h>
#include <DB/Common/UInt128.h>
#include <DB/Databases/DatabaseCloud.h>
#include <DB/Databases/DatabasesCommon.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/ReadBufferFromString.h>
#include <DB/IO/HexWriteBuffer.h>
#include <DB/Parsers/ASTCreateQuery.h>
#include <DB/Parsers/parseQuery.h>
#include <DB/Parsers/ParserCreateQuery.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}

namespace
{
	constexpr size_t TABLE_TO_NODE_DIVISOR = 4096;

	using Hash = DatabaseCloud::Hash;
}


void DatabaseCloud::createZookeeperNodes()
{
	zkutil::ZooKeeperPtr zookeeper = context->getZookeeper();

	zookeeper->createAncestors(zookeeper_path);

	auto acl = zookeeper->getDefaultACL();

	zkutil::Ops ops;
	ops.push_back(new zkutil::Op::Create(zookeeper_path, "", acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/table_definitions", "", acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/tables", "", acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/local_tables", "", acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/ordered_locality_keys", "", acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/nodes", "", acl, zkutil::CreateMode::Persistent));

	auto code = zookeeper->tryMulti(ops);
	if (code == ZOK)
		LOG_INFO(log, "Created new cloud.");
	else if (code == ZNODEEXISTS)
		LOG_INFO(log, "Adding server to existing cloud.");
	else
		throw zkutil::KeeperException(code);

	zookeeper->createIfNotExists(zookeeper_path + "/tables/" + name, "");
	zookeeper->createIfNotExists(zookeeper_path + "/local_tables/" + name, "");
	zookeeper->createIfNotExists(zookeeper_path + "/nodes/" + hostname, "");
	zookeeper->createIfNotExists(zookeeper_path + "/nodes/" + hostname + "/datacenter", datacenter_name);
}


DatabaseCloud::DatabaseCloud(
	bool attach,
	const String & name_,
	const String & zookeeper_path_,
	size_t replication_factor_,
	const String & datacenter_name_,
	Context & context_,
	boost::threadpool::pool * thread_pool)
	:
	name(name_),
	zookeeper_path(context_.getMacros().expand(zookeeper_path_)),
	replication_factor(replication_factor_),
	datacenter_name(context_.getMacros().expand(datacenter_name_)),
	log(&Logger::get("DatabaseCloud (" + name + ")")),
	context(context_)
{
	if (zookeeper_path.empty())
		throw Exception("Logical error: empty zookeeper_path passed", ErrorCodes::LOGICAL_ERROR);

	if (zookeeper_path.back() == '/')
		zookeeper_path.pop_back();

	hostname = context.getInterserverIOAddress().first;

	data_path = context.getPath() + "/data/" + escapeForFileName(name) + "/";

	if (!attach)
		createZookeeperNodes();
}


Hash DatabaseCloud::getTableHash(const String & table_name) const
{
	SipHash hash;
	hash.update(name.data(), name.size() + 1);	/// Хэшируем также нулевой байт в качестве разделителя.
	hash.update(table_name.data(), table_name.size());

	Hash res;
	hash.get128(reinterpret_cast<char *>(&res));
	return res;
}


String DatabaseCloud::getNameOfNodeWithTables(const String & table_name) const
{
	Hash hash = getTableHash(table_name);
	String res;
	WriteBufferFromString out(res);
	writeText(hash.first % TABLE_TO_NODE_DIVISOR, out);
	out.next();
	return res;
}


static String hashToHex(Hash hash)
{
	String res;
	WriteBufferFromString str_out(res);
	HexWriteBuffer hex_out(str_out);
	writePODBinary(hash, hex_out);
	hex_out.next();
	str_out.next();
	return res;
}


String DatabaseCloud::getTableDefinitionFromHash(Hash hash) const
{
	zkutil::ZooKeeperPtr zookeeper = context.getZooKeeper();
	return zookeeper->get(zookeeper_path + "/table_definitions/" + hashToHex(hash));
}


/** Описание одной таблицы в списке таблиц в ZooKeeper.
  * Без имени таблицы (правая часть отображения).
  */
struct TableDescription
{
	/// Хэш от структуры таблицы. Сама структура хранится отдельно.
	Hash definition_hash;
	/// Имя локальной таблицы для хранения данных. Может быть пустым, если в таблицу ещё ничего не записывали.
	String local_table_name;
	/// Список хостов, на которых расположены данные таблицы. Может быть пустым, если в таблицу ещё ничего не записывали.
	std::vector<String> hosts;

	void write(WriteBuffer & buf) const
	{
		writePODBinary(definition_hash, buf);
		writeVectorBinary(hosts, buf);
	}

	void read(ReadBuffer & buf)
	{
		readPODBinary(definition_hash, buf);
		readVectorBinary(hosts, buf);
	}
};


/** Множество таблиц в ZooKeeper.
  * Точнее, его кусок, относящийся к одной ноде.
  * (Всё множество разбито по TABLE_TO_NODE_DIVISOR нод.)
  */
struct TableSet
{
	/// Имя -> описание. В упорядоченном виде, данные будут лучше сжиматься.
	using Container = std::map<String, TableDescription>;
	Container map;

	TableSet(const String & data)
	{
		ReadBufferFromString in(data);
		read(in);
	}

	void write(WriteBuffer & buf) const
	{
		writeCString("Version 1\n", buf);

		CompressedWriteBuffer out(buf);		/// NOTE Можно уменьшить размер выделяемого буфера.
		for (const auto & kv : map)
		{
			writeBinary(kv.first, out);
			kv.second.write(out);
		}
	}

	void read(ReadBuffer & buf)
	{
		assertString("Version 1\n", buf);

		CompressedReadBuffer in(buf);
		while (!in.eof())
		{
			Container::value_type kv;
			readBinary(kv.first, in);
			kv.second.read(in);
			map.emplace(std::move(kv));
		}
	}
};


/** Множество локальных таблиц в ZooKeeper.
  * Точнее, его кусок, относящийся к одной ноде.
  * (Всё множество разбито по TABLE_TO_NODE_DIVISOR нод.)
  */
struct LocalTableSet
{
	/// Хэш от имени -> хэш от структуры.
	using Container = std::map<Hash, Hash>;
	Container map;

	TableSet(const String & data)
	{
		ReadBufferFromString in(data);
		read(in);
	}

	void write(WriteBuffer & buf) const
	{
		writeCString("Version 1\n", buf);

		CompressedWriteBuffer out(buf);		/// NOTE Можно уменьшить размер выделяемого буфера.
		for (const auto & kv : map)
		{
			writePODBinary(kv.first, out);
			writePODBinary(kv.second, out);
		}
	}

	void read(ReadBuffer & buf)
	{
		assertString("Version 1\n", buf);

		CompressedReadBuffer in(buf);
		while (!in.eof())
		{
			Container::value_type kv;
			readPODBinary(kv.first, in);
			readPODBinary(kv.second, in);
			map.emplace(std::move(kv));
		}
	}
};


bool DatabaseCloud::isTableExist(const String & table_name) const
{
	/// Ищем локальную таблицу в кэше локальных таблиц или в файловой системе в path.
	/// Если не нашли - ищем облачную таблицу в ZooKeeper.

	{
		std::lock_guard<std::mutex> lock(local_tables_mutex);
		if (local_tables_cache.count(table_name))
			return true;
	}

	String table_name_escaped = escapeForFileName(table_name);
	if (Poco::File(data_path + table_name_escaped).exists())
		return true;

	zkutil::ZooKeeperPtr zookeeper = context.getZooKeeper();

	String table_set_data;
	if (!zookeeper->tryGet(zookeeper_path + "/tables/" + name + "/" + getNameOfNodeWithTables(table_name), table_set_data))
		return false;

	TableSet table_set(table_set_data);
	return table_set.map.count(table_name);
}


StoragePtr DatabaseCloud::tryGetTable(const String & table_name)
{
	/// Ищем локальную таблицу.
	/// Если не нашли - ищем облачную таблицу в ZooKeeper.

	{
		std::lock_guard<std::mutex> lock(local_tables_mutex);
		auto it = local_tables_cache.find(table_name);
		if (it != local_tables_cache.end())
			return it->second;
	}

	zkutil::ZooKeeperPtr zookeeper = context.getZooKeeper();

	String table_name_escaped = escapeForFileName(table_name);
	if (Poco::File(data_path + table_name_escaped).exists())
	{
		LocalTableSet local_tables_info(zookeeper->get(
			zookeeper_path + "/local_tables/" + name + "/" + getNameOfNodeWithTables(table_name)));

		Hash table_hash = getTableHash(table_name);
		String definition = getTableDefinitionFromHash(local_tables_info.map.at(table_hash));

		/// Инициализируем локальную таблицу.
		{
			std::lock_guard<std::mutex> lock(local_tables_mutex);

			/// А если таблицу только что создали?
			auto it = local_tables_cache.find(table_name);
			if (it != local_tables_cache.end())
				return it->second;

			String table_name;
			StoragePtr table;
			std::tie(table_name, table) = createTableFromDefinition(
				definition, name, data_path, context,
				"in zookeeper node " + zookeeper_path + "/table_definitions/" + hashToHex(table_hash));

			local_tables_cache.emplace(table_name, table);
			return table;
		}
	}
	else
	{
		const TableSet tables_info(zookeeper->get(
			zookeeper_path + "/tables/" + name + "/" + getNameOfNodeWithTables(table_name)));

		const TableDescription & description = tables_info.at(table_name);
		String definition = getTableDefinitionFromHash(description.definition_hash);

		/// TODO Инициализация объекта StorageCloud
		return {};
	}
}


/// Список таблиц может быть неконсистентным, так как он получается неатомарно, а по кускам, по мере итерации.
class DatabaseCloudIterator : public IDatabaseIterator
{
private:
	DatabasePtr owned_database;
	zkutil::ZooKeeperPtr zookeeper;
	const String & zookeeper_path;

	bool first = true;
	const Strings nodes;
	Strings::const_iterator nodes_iterator;

	TableSet table_set;
	TableSet::Container::iterator table_set_iterator;

	DatabaseCloud & parent()
	{
		return static_cast<DatabaseCloud &>(*owned_database);
	}

	bool fetchNextTableSet()
	{
		do
		{
			if (first)
				first = false;
			else
				++nodes_iterator;

			if (nodes_iterator == nodes.end())
				return false;

			table_set = TableSet(zookeeper->get(zookeeper_path + "/" + *nodes_iterator));
			table_set_iterator = table_set.map.begin();
		}
		while (!table_set.map.empty());		/// Пропускаем пустые table set-ы.

		return true;
	}

public:
	DatabaseCloudIterator(DatabasePtr database)
		: owned_database(database),
		zookeeper(parent().context.getZooKeeper()),
		zookeeper_path(parent().zookeeper_path + "/tables/" + parent().name),
		nodes(zookeeper->getChildren(zookeeper_path)),
		nodes_iterator(nodes.begin())
	{
		fetchNextTableSet();
	}

	void next() override
	{
		++table_set_iterator;
		if (table_set_iterator == table_set.end())
			fetchNextTableSet();
	}

	bool isValid() const override
	{
		return nodes_iterator != nodes.end();
	}

	const String & name() const override
	{
		return table_set_iterator->first;
	}

	StoragePtr & table() const
	{
		String definition = parent().getTableDefinitionFromHash(table_set_iterator->second.definition_hash);

		/// TODO Инициализация объекта StorageCloud
		return {};
	}
};


DatabaseIteratorPtr DatabaseCloud::getIterator()
{
	return std::make_unique<DatabaseCloudIterator>(shared_from_this());
}


bool DatabaseCloud::empty() const
{
	/// Есть хотя бы один непустой узел среди списков таблиц.

	zkutil::ZooKeeperPtr zookeeper = context.getZooKeeper();
	Strings nodes = zookeeper->getChildren(zookeeper_path + "/tables/" + name);
	if (nodes.empty())
		return true;

	for (const auto & node : nodes)
		if (!zookeeper->get(zookeeper_path + "/tables/" + name + "/" + node).empty())
			return false;

	return true;
}


ASTPtr DatabaseCloud::getCreateQuery(const String & table_name) const
{
	zkutil::ZooKeeperPtr zookeeper = context.getZooKeeper();
	Hash definition_hash;

	String table_name_escaped = escapeForFileName(table_name);
	if (Poco::File(data_path + table_name_escaped).exists())
	{
		LocalTableSet local_tables_info(zookeeper->get(
			zookeeper_path + "/local_tables/" + name + "/" + getNameOfNodeWithTables(table_name)));

		Hash table_hash = getTableHash(table_name);
		definition_hash = local_tables_info.map.at(table_hash);
	}
	else
	{
		const TableSet tables_info(zookeeper->get(
			zookeeper_path + "/tables/" + name + "/" + getNameOfNodeWithTables(table_name)));

		const TableDescription & description = tables_info.at(table_name);
		definition_hash = description.definition_hash;
	}

	String definition = getTableDefinitionFromHash(definition_hash);

	ParserCreateQuery parser;
	ASTPtr ast = parseQuery(parser, definition.data(), definition.data() + definition.size(),
		"in zookeeper node " + zookeeper_path + "/table_definitions/" + hashToHex(definition_hash));

	ASTCreateQuery & ast_create_query = typeid_cast<ASTCreateQuery &>(*ast);
	ast_create_query.attach = false;
	ast_create_query.database = name;

	return ast;
}


void DatabaseCloud::shutdown()
{
	/// Нельзя удерживать блокировку во время shutdown.
	/// Потому что таблицы могут внутри функции shutdown работать с БД, а mutex не рекурсивный.
	Tables local_tables_snapshot;
	{
		std::lock_guard<std::mutex> lock(local_tables_mutex);
		local_tables_snapshot = local_tables_cache;
	}

	for (auto & name_table : local_tables_snapshot)
		name_table.second->shutdown();

	{
		std::lock_guard<std::mutex> lock(local_tables_mutex);
		local_tables_cache.clear();
	}
}

}
