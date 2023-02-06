//
// Utility.h
//
// Library: Data/SQLite
// Package: SQLite
// Module:  Utility
//
// Definition of Utility.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SQLite_Utility_INCLUDED
#define SQLite_Utility_INCLUDED


#include "Poco/Data/SQLite/SQLite.h"
#include "Poco/Data/MetaColumn.h"
#include "Poco/Data/Session.h"
#include "Poco/Mutex.h"
#include "Poco/Types.h"
#include <map>


extern "C"
{
	typedef struct sqlite3 sqlite3;
	typedef struct sqlite3_stmt sqlite3_stmt;
	typedef struct sqlite3_mutex* _pMutex;
}


namespace Poco {
namespace Data {
namespace SQLite {


class SQLite_API Utility
	/// Various utility functions for SQLite.
{
public:
	static const std::string SQLITE_DATE_FORMAT;
	static const std::string SQLITE_TIME_FORMAT;
	typedef std::map<std::string, MetaColumn::ColumnDataType> TypeMap;

	static const int THREAD_MODE_SINGLE;
	static const int THREAD_MODE_MULTI;
	static const int THREAD_MODE_SERIAL;

	static const int OPERATION_INSERT;
	static const int OPERATION_DELETE;
	static const int OPERATION_UPDATE;

	static sqlite3* dbHandle(const Session& session);
		/// Returns native DB handle.

	static std::string lastError(sqlite3* pDB);
		/// Retreives the last error code from sqlite and converts it to a string.

	static std::string lastError(const Session& session);
		/// Retreives the last error code from sqlite and converts it to a string.

	static void throwException(sqlite3* pDB, int rc, const std::string& addErrMsg = std::string());
		/// Throws for an error code the appropriate exception

	static MetaColumn::ColumnDataType getColumnType(sqlite3_stmt* pStmt, std::size_t pos);
		/// Returns column data type.

	static bool fileToMemory(sqlite3* pInMemory, const std::string& fileName);
		/// Loads the contents of a database file on disk into an opened
		/// database in memory.
		/// 
		/// Returns true if succesful.

	static bool fileToMemory(const Session& session, const std::string& fileName);
		/// Loads the contents of a database file on disk into an opened
		/// database in memory.
		/// 
		/// Returns true if succesful.

	static bool memoryToFile(const std::string& fileName, sqlite3* pInMemory);
		/// Saves the contents of an opened database in memory to the
		/// database on disk.
		/// 
		/// Returns true if succesful.

	static bool memoryToFile(const std::string& fileName, const Session& session);
		/// Saves the contents of an opened database in memory to the
		/// database on disk.
		/// 
		/// Returns true if succesful.

	static bool isThreadSafe();
		/// Returns true if SQLite was compiled in multi-thread or serialized mode.
		/// See http://www.sqlite.org/c3ref/threadsafe.html for details.
		/// 
		/// Returns true if succesful

	static bool setThreadMode(int mode);
		/// Sets the threading mode to single, multi or serialized.
		/// See http://www.sqlite.org/threadsafe.html for details.
		/// 
		/// Returns true if succesful

	static int getThreadMode();
		/// Returns the thread mode.

	typedef void(*UpdateCallbackType)(void*, int, const char*, const char*, Poco::Int64);
		/// Update callback function type.
 
	typedef int(*CommitCallbackType)(void*);
		/// Commit callback function type.

	typedef void(*RollbackCallbackType)(void*);
		/// Rollback callback function type.

	template <typename T, typename CBT>
	static bool registerUpdateHandler(sqlite3* pDB, CBT callbackFn, T* pParam)
		/// Registers the callback for (1)(insert, delete, update), (2)(commit) or 
		/// or (3)(rollback) events. Only one function per group can be registered
		/// at a time. Registration is not thread-safe. Storage pointed to by pParam
		/// must remain valid as long as registration is active. Registering with
		/// callbackFn set to zero disables notifications.
		/// 
		/// See http://www.sqlite.org/c3ref/update_hook.html and 
		/// http://www.sqlite.org/c3ref/commit_hook.html for details.
	{
		typedef std::pair<CBT, T*> CBPair;
		typedef std::multimap<sqlite3*, CBPair> CBMap;
		typedef typename CBMap::iterator CBMapIt;
		typedef std::pair<CBMapIt, CBMapIt> CBMapItPair;

		static CBMap retMap;
		T* pRet = reinterpret_cast<T*>(eventHookRegister(pDB, callbackFn, pParam));

		if (pRet == 0)
		{
			if (retMap.find(pDB) == retMap.end())
			{
				retMap.insert(std::make_pair(pDB, CBPair(callbackFn, pParam)));
				return true;
			}
		}
		else
		{
			CBMapItPair retMapRange = retMap.equal_range(pDB);
			for (CBMapIt it = retMapRange.first; it != retMapRange.second; ++it)
			{
				poco_assert (it->second.first != 0);
				if ((callbackFn == 0) && (*pRet == *it->second.second))
				{
					retMap.erase(it);
					return true;
				}

				if ((callbackFn == it->second.first) && (*pRet == *it->second.second))
				{
					it->second.second = pParam;
					return true;
				}
			}
		}

		return false;
	}

	template <typename T, typename CBT>
	static bool registerUpdateHandler(const Session& session, CBT callbackFn, T* pParam)
		/// Registers the callback by calling registerUpdateHandler(sqlite3*, CBT, T*).
	{
		return registerUpdateHandler(dbHandle(session), callbackFn, pParam);
	}

	class SQLiteMutex
	{
	public:
		SQLiteMutex(sqlite3* pDB);
		~SQLiteMutex();

	private:
		SQLiteMutex();
		sqlite3_mutex* _pMutex;
	};

private:
	Utility();
		/// Maps SQLite column declared types to Poco::Data types through
		/// static TypeMap member.
		/// 
		/// Note: SQLite is type-agnostic and it is the end-user responsibility
		/// to ensure that column declared data type corresponds to the type of 
		/// data actually held in the database.
		/// 
		/// Column types are case-insensitive.

	Utility(const Utility&);
	Utility& operator = (const Utility&);

	static void* eventHookRegister(sqlite3* pDB, UpdateCallbackType callbackFn, void* pParam);
	static void* eventHookRegister(sqlite3* pDB, CommitCallbackType callbackFn, void* pParam);
	static void* eventHookRegister(sqlite3* pDB, RollbackCallbackType callbackFn, void* pParam);

	static TypeMap     _types;
	static Poco::Mutex _mutex;
	static int         _threadMode;
};


//
// inlines
//
inline std::string Utility::lastError(const Session& session)
{
	poco_assert_dbg ((0 == icompare(session.connector(), 0, 6, "sqlite")));
	return lastError(dbHandle(session));
}


inline bool Utility::memoryToFile(const std::string& fileName, const Session& session)
{
	poco_assert_dbg ((0 == icompare(session.connector(), 0, 6, "sqlite")));
	return memoryToFile(fileName, dbHandle(session));
}


inline bool Utility::fileToMemory(const Session& session, const std::string& fileName)
{
	poco_assert_dbg ((0 == icompare(session.connector(), 0, 6, "sqlite")));
	return fileToMemory(dbHandle(session), fileName);
}


} } } // namespace Poco::Data::SQLite


#endif // SQLite_Utility_INCLUDED
