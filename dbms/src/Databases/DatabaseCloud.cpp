#include <zkutil/ZooKeeper.h>
#include <DB/Interpreters/Context.h>
#include <DB/Common/escapeForFileName.h>
#include <DB/Common/SipHash.h>
#include <DB/Common/UInt128.h>
#include <DB/Databases/DatabaseCloud.h>
#include <DB/IO/CompressedWriteBuffer.h>
#include <DB/IO/CompressedReadBuffer.h>
#include <DB/IO/WriteBufferFromString.h>
#include <DB/IO/ReadBufferFromString.h>


namespace DB
{

namespace ErrorCodes
{
	extern const int LOGICAL_ERROR;
}

namespace
{
	constexpr size_t TABLE_TO_NODE_DIVISOR = 4096;
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
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/ordered_locality_keys", "", acl, zkutil::CreateMode::Persistent));
	ops.push_back(new zkutil::Op::Create(zookeeper_path + "/nodes", "", acl, zkutil::CreateMode::Persistent));

	auto code = zookeeper->tryMulti(ops);
	if (code == ZOK)
		LOG_INFO(log, "Created new cloud.");
	else if (code == ZNODEEXISTS)
		LOG_INFO(log, "Adding server to existing cloud.");
	else
		throw zkutil::KeeperException(code);

	zookeeper->createIfNotExists(zookeeper_path + "/tables/" + name);
	zookeeper->createIfNotExists(zookeeper_path + "/nodes/" + hostname);
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


String DatabaseCloud::getNameOfNodeWithTables(const String & table_name)
{
	SipHash hash;
	hash.update(name.data(), name.size());
	hash.update(table_name.data(), table_name.size());
	/// Коллизии из-за конкатенации строк не имеют значения.

	UInt64 value = hash.get64();
	String res;
	WriteBufferFromString out(res);
	writeText(value % TABLE_TO_NODE_DIVISOR, out);
	out.next();
	return res;
}


/** Описание одной таблицы в списке таблиц в ZooKeeper.
  * Без имени таблицы (правая часть отображения).
  */
struct TableDescription
{
	/// Хэш от структуры таблицы. Сама структура хранится отдельно.
	UInt128 definition_hash;
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


bool DatabaseCloud::isTableExist(const String & table_name) const
{
	/// Ищем локальную таблицу в path.
	/// Если не нашли - ищем облачную таблицу в ZooKeeper.

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

}

}
