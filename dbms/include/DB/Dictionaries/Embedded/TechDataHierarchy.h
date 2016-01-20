#pragma once

#include <string.h>

#include <common/logger_useful.h>
#include <common/singleton.h>

#include <mysqlxx/PoolWithFailover.h>


/** @brief Класс, позволяющий узнавать, принадлежит ли поисковая система или операционная система
  * другой поисковой или операционной системе, соответственно.
  * Информацию об иерархии регионов загружает из БД.
  */
class TechDataHierarchy
{
private:
	Logger * log;

	UInt8 os_parent[256];
	UInt8 se_parent[256];
	
public:
	static constexpr auto required_key = "mysql_metrica";

	TechDataHierarchy()
		: log(&Logger::get("TechDataHierarchy"))
	{
		LOG_DEBUG(log, "Loading tech data hierarchy.");
		
		memset(os_parent, 0, sizeof(os_parent));
		memset(se_parent, 0, sizeof(se_parent));

		mysqlxx::PoolWithFailover pool(required_key);
		mysqlxx::Pool::Entry conn = pool.Get();

		{
			mysqlxx::Query q = conn->query("SELECT Id, COALESCE(Parent_Id, 0) FROM OS2");
			LOG_TRACE(log, q.str());
			mysqlxx::UseQueryResult res = q.use();
			while (mysqlxx::Row row = res.fetch())
			{
				UInt64 child = row[0].getUInt();
				UInt64 parent = row[1].getUInt();

				if (child > 255 || parent > 255)
					throw Poco::Exception("Too large OS id (> 255).");

				os_parent[child] = parent;
			}
		}

		{
			mysqlxx::Query q = conn->query("SELECT Id, COALESCE(ParentId, 0) FROM SearchEngines");
			LOG_TRACE(log, q.str());
			mysqlxx::UseQueryResult res = q.use();
			while (mysqlxx::Row row = res.fetch())
			{
				UInt64 child = row[0].getUInt();
				UInt64 parent = row[1].getUInt();

				if (child > 255 || parent > 255)
					throw Poco::Exception("Too large search engine id (> 255).");

				se_parent[child] = parent;
			}
		}
	}


	/// Отношение "принадлежит".
	bool isOSIn(UInt8 lhs, UInt8 rhs) const
	{
		while (lhs != rhs && os_parent[lhs])
			lhs = os_parent[lhs];
		
		return lhs == rhs;
	}

	bool isSEIn(UInt8 lhs, UInt8 rhs) const
	{
		while (lhs != rhs && se_parent[lhs])
			lhs = se_parent[lhs];

		return lhs == rhs;
	}
	
	
	UInt8 OSToParent(UInt8 x) const
	{
		return os_parent[x];
	}

	UInt8 SEToParent(UInt8 x) const
	{
		return se_parent[x];
	}
	

	/// К самому верхнему предку.
	UInt8 OSToMostAncestor(UInt8 x) const
	{
		while (os_parent[x])
			x = os_parent[x];
		return x;
	}

	UInt8 SEToMostAncestor(UInt8 x) const
	{
		while (se_parent[x])
			x = se_parent[x];
		return x;
	}
};


class TechDataHierarchySingleton : public Singleton<TechDataHierarchySingleton>, public TechDataHierarchy {};
