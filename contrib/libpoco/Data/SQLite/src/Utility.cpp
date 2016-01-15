//
// Utility.cpp
//
// $Id: //poco/Main/Data/SQLite/src/Utility.cpp#5 $
//
// Library: SQLite
// Package: SQLite
// Module:  Utility
//
// Implementation of Utility
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/SQLite/Utility.h"
#include "Poco/Data/SQLite/SQLiteException.h"
#include "Poco/NumberFormatter.h"
#include "Poco/String.h"
#include "Poco/Any.h"
#include "Poco/Exception.h"
#if defined(POCO_UNBUNDLED)
#include <sqlite3.h>
#else
#include "sqlite3.h"
#endif


#ifndef SQLITE_OPEN_URI
#define SQLITE_OPEN_URI 0
#endif


namespace Poco {
namespace Data {
namespace SQLite {


const int Utility::THREAD_MODE_SINGLE = SQLITE_CONFIG_SINGLETHREAD;
const int Utility::THREAD_MODE_MULTI = SQLITE_CONFIG_MULTITHREAD;
const int Utility::THREAD_MODE_SERIAL = SQLITE_CONFIG_SERIALIZED;
int Utility::_threadMode =
#if (SQLITE_THREADSAFE == 0)
	SQLITE_CONFIG_SINGLETHREAD;
#elif (SQLITE_THREADSAFE == 1)
	SQLITE_CONFIG_SERIALIZED;
#elif (SQLITE_THREADSAFE == 2)
	SQLITE_CONFIG_MULTITHREAD;
#endif

const int Utility::OPERATION_INSERT = SQLITE_INSERT;
const int Utility::OPERATION_DELETE = SQLITE_DELETE;
const int Utility::OPERATION_UPDATE = SQLITE_UPDATE;

const std::string Utility::SQLITE_DATE_FORMAT = "%Y-%m-%d";
const std::string Utility::SQLITE_TIME_FORMAT = "%H:%M:%S";
Utility::TypeMap Utility::_types;
Poco::Mutex Utility::_mutex;


Utility::Utility()
{
	if (_types.empty())
	{
		_types.insert(TypeMap::value_type("", MetaColumn::FDT_STRING));
		_types.insert(TypeMap::value_type("BOOL", MetaColumn::FDT_BOOL));
		_types.insert(TypeMap::value_type("BOOLEAN", MetaColumn::FDT_BOOL));
		_types.insert(TypeMap::value_type("BIT", MetaColumn::FDT_BOOL));
		_types.insert(TypeMap::value_type("UINT8", MetaColumn::FDT_UINT8));
		_types.insert(TypeMap::value_type("UTINY", MetaColumn::FDT_UINT8));
		_types.insert(TypeMap::value_type("UINTEGER8", MetaColumn::FDT_UINT8));
		_types.insert(TypeMap::value_type("INT8", MetaColumn::FDT_INT8));
		_types.insert(TypeMap::value_type("TINY", MetaColumn::FDT_INT8));
		_types.insert(TypeMap::value_type("INTEGER8", MetaColumn::FDT_INT8));
		_types.insert(TypeMap::value_type("UINT16", MetaColumn::FDT_UINT16));
		_types.insert(TypeMap::value_type("USHORT", MetaColumn::FDT_UINT16));
		_types.insert(TypeMap::value_type("UINTEGER16", MetaColumn::FDT_UINT16));
		_types.insert(TypeMap::value_type("INT16", MetaColumn::FDT_INT16));
		_types.insert(TypeMap::value_type("SHORT", MetaColumn::FDT_INT16));
		_types.insert(TypeMap::value_type("INTEGER16", MetaColumn::FDT_INT16));
		_types.insert(TypeMap::value_type("UINT", MetaColumn::FDT_UINT32));
		_types.insert(TypeMap::value_type("UINT32", MetaColumn::FDT_UINT32));
		_types.insert(TypeMap::value_type("UINTEGER32", MetaColumn::FDT_UINT32));
		_types.insert(TypeMap::value_type("INT", MetaColumn::FDT_INT32));
		_types.insert(TypeMap::value_type("INT32", MetaColumn::FDT_INT32));
		_types.insert(TypeMap::value_type("INTEGER", MetaColumn::FDT_INT32));
		_types.insert(TypeMap::value_type("INTEGER32", MetaColumn::FDT_INT32));
		_types.insert(TypeMap::value_type("UINT64", MetaColumn::FDT_UINT64));
		_types.insert(TypeMap::value_type("ULONG", MetaColumn::FDT_INT64));
		_types.insert(TypeMap::value_type("UINTEGER64", MetaColumn::FDT_UINT64));
		_types.insert(TypeMap::value_type("INT64", MetaColumn::FDT_INT64));
		_types.insert(TypeMap::value_type("LONG", MetaColumn::FDT_INT64));
		_types.insert(TypeMap::value_type("INTEGER64", MetaColumn::FDT_INT64));
		_types.insert(TypeMap::value_type("TINYINT", MetaColumn::FDT_INT8));
		_types.insert(TypeMap::value_type("SMALLINT", MetaColumn::FDT_INT16));
		_types.insert(TypeMap::value_type("BIGINT", MetaColumn::FDT_INT64));
		_types.insert(TypeMap::value_type("COUNTER", MetaColumn::FDT_UINT64));
		_types.insert(TypeMap::value_type("AUTOINCREMENT", MetaColumn::FDT_UINT64));
		_types.insert(TypeMap::value_type("REAL", MetaColumn::FDT_DOUBLE));
		_types.insert(TypeMap::value_type("FLOA", MetaColumn::FDT_DOUBLE));
		_types.insert(TypeMap::value_type("FLOAT", MetaColumn::FDT_DOUBLE));
		_types.insert(TypeMap::value_type("DOUB", MetaColumn::FDT_DOUBLE));
		_types.insert(TypeMap::value_type("DOUBLE", MetaColumn::FDT_DOUBLE));
		_types.insert(TypeMap::value_type("DECIMAL", MetaColumn::FDT_DOUBLE));
		_types.insert(TypeMap::value_type("NUMERIC", MetaColumn::FDT_DOUBLE));
		_types.insert(TypeMap::value_type("CHAR", MetaColumn::FDT_STRING));
		_types.insert(TypeMap::value_type("CLOB", MetaColumn::FDT_STRING));
		_types.insert(TypeMap::value_type("TEXT", MetaColumn::FDT_STRING));
		_types.insert(TypeMap::value_type("VARCHAR", MetaColumn::FDT_STRING));
		_types.insert(TypeMap::value_type("NCHAR", MetaColumn::FDT_STRING));
		_types.insert(TypeMap::value_type("NCLOB", MetaColumn::FDT_STRING));
		_types.insert(TypeMap::value_type("NTEXT", MetaColumn::FDT_STRING));
		_types.insert(TypeMap::value_type("NVARCHAR", MetaColumn::FDT_STRING));
		_types.insert(TypeMap::value_type("BLOB", MetaColumn::FDT_BLOB));
		_types.insert(TypeMap::value_type("DATE", MetaColumn::FDT_DATE));
		_types.insert(TypeMap::value_type("TIME", MetaColumn::FDT_TIME));
		_types.insert(TypeMap::value_type("DATETIME", MetaColumn::FDT_TIMESTAMP));
		_types.insert(TypeMap::value_type("TIMESTAMP", MetaColumn::FDT_TIMESTAMP));
	}
}


std::string Utility::lastError(sqlite3* pDB)
{
	return std::string(sqlite3_errmsg(pDB));
}


MetaColumn::ColumnDataType Utility::getColumnType(sqlite3_stmt* pStmt, std::size_t pos)
{
	poco_assert_dbg (pStmt);

	// Ensure statics are initialized
	{
		Poco::Mutex::ScopedLock lock(_mutex);
		static Utility u;
	}
	
	const char* pc = sqlite3_column_decltype(pStmt, (int) pos);
	std::string sqliteType = pc ? pc : "";
	Poco::toUpperInPlace(sqliteType);
	sqliteType = sqliteType.substr(0, sqliteType.find_first_of(" ("));

	TypeMap::const_iterator it = _types.find(Poco::trimInPlace(sqliteType));
	if (_types.end() == it)	throw Poco::NotFoundException();

	return it->second;
}


void Utility::throwException(int rc, const std::string& addErrMsg)
{
	switch (rc)
	{
	case SQLITE_OK:
		break;
	case SQLITE_ERROR:
		throw InvalidSQLStatementException(std::string("SQL error or missing database"), addErrMsg);
	case SQLITE_INTERNAL:
		throw InternalDBErrorException(std::string("An internal logic error in SQLite"), addErrMsg);
	case SQLITE_PERM:
		throw DBAccessDeniedException(std::string("Access permission denied"), addErrMsg);
	case SQLITE_ABORT:
		throw ExecutionAbortedException(std::string("Callback routine requested an abort"), addErrMsg);
	case SQLITE_BUSY:
	case SQLITE_BUSY_RECOVERY:
	case SQLITE_BUSY_SNAPSHOT:
		throw DBLockedException(std::string("The database file is locked"), addErrMsg);
	case SQLITE_LOCKED:
		throw TableLockedException(std::string("A table in the database is locked"), addErrMsg);
	case SQLITE_NOMEM:
		throw NoMemoryException(std::string("A malloc() failed"), addErrMsg);
	case SQLITE_READONLY:
		throw ReadOnlyException(std::string("Attempt to write a readonly database"), addErrMsg);
	case SQLITE_INTERRUPT:
		throw InterruptException(std::string("Operation terminated by sqlite_interrupt()"), addErrMsg);
	case SQLITE_IOERR:
		throw IOErrorException(std::string("Some kind of disk I/O error occurred"), addErrMsg);
	case SQLITE_CORRUPT:
		throw CorruptImageException(std::string("The database disk image is malformed"), addErrMsg);
	case SQLITE_NOTFOUND:
		throw TableNotFoundException(std::string("Table or record not found"), addErrMsg);
	case SQLITE_FULL:
		throw DatabaseFullException(std::string("Insertion failed because database is full"), addErrMsg);
	case SQLITE_CANTOPEN:
		throw CantOpenDBFileException(std::string("Unable to open the database file"), addErrMsg);
	case SQLITE_PROTOCOL:
		throw LockProtocolException(std::string("Database lock protocol error"), addErrMsg);
	case SQLITE_EMPTY:
		throw InternalDBErrorException(std::string("(Internal Only) Database table is empty"), addErrMsg);
	case SQLITE_SCHEMA:
		throw SchemaDiffersException(std::string("The database schema changed"), addErrMsg);
	case SQLITE_TOOBIG:
		throw RowTooBigException(std::string("Too much data for one row of a table"), addErrMsg);
	case SQLITE_CONSTRAINT:
		throw ConstraintViolationException(std::string("Abort due to constraint violation"), addErrMsg);
	case SQLITE_MISMATCH:
		throw DataTypeMismatchException(std::string("Data type mismatch"), addErrMsg);
	case SQLITE_MISUSE:
		throw InvalidLibraryUseException(std::string("Library used incorrectly"), addErrMsg);
	case SQLITE_NOLFS:
		throw OSFeaturesMissingException(std::string("Uses OS features not supported on host"), addErrMsg);
	case SQLITE_AUTH:
		throw AuthorizationDeniedException(std::string("Authorization denied"), addErrMsg);
	case SQLITE_FORMAT:
		throw CorruptImageException(std::string("Auxiliary database format error"), addErrMsg);
	case SQLITE_NOTADB:
		throw CorruptImageException(std::string("File opened that is not a database file"), addErrMsg);
	case SQLITE_RANGE:
		throw InvalidSQLStatementException(std::string("Bind Parameter out of range (Access of invalid position 0? bind starts with 1!)"), addErrMsg);
	case SQLITE_ROW:
		break; // sqlite_step() has another row ready
	case SQLITE_DONE:
		break; // sqlite_step() has finished executing
	default:
		throw SQLiteException(std::string("Unknown error code: ") + Poco::NumberFormatter::format(rc), addErrMsg);
	}
}


bool Utility::fileToMemory(sqlite3* pInMemory, const std::string& fileName)
{
	int rc;
	sqlite3* pFile;
	sqlite3_backup* pBackup;

	rc = sqlite3_open_v2(fileName.c_str(), &pFile, SQLITE_OPEN_READONLY | SQLITE_OPEN_URI, NULL);
	if(rc == SQLITE_OK )
	{
		pBackup = sqlite3_backup_init(pInMemory, "main", pFile, "main");
		if( pBackup )
		{
			sqlite3_backup_step(pBackup, -1);
			sqlite3_backup_finish(pBackup);
		}
		rc = sqlite3_errcode(pFile);
	}

	sqlite3_close(pFile);
	return SQLITE_OK == rc;
}


bool Utility::memoryToFile(const std::string& fileName, sqlite3* pInMemory)
{
	int rc;
	sqlite3* pFile;
	sqlite3_backup* pBackup;

	rc = sqlite3_open_v2(fileName.c_str(), &pFile, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI, NULL);
	if(rc == SQLITE_OK )
	{
		pBackup = sqlite3_backup_init(pFile, "main", pInMemory, "main");
		if( pBackup )
		{
			sqlite3_backup_step(pBackup, -1);
			sqlite3_backup_finish(pBackup);
		}
		rc = sqlite3_errcode(pFile);
	}

	sqlite3_close(pFile);
	return SQLITE_OK == rc;
}


bool Utility::isThreadSafe()
{
	return 0 != sqlite3_threadsafe();
}


int Utility::getThreadMode()
{
	return _threadMode;
}


bool Utility::setThreadMode(int mode)
{
#if (SQLITE_THREADSAFE != 0)
	if (SQLITE_OK == sqlite3_shutdown())
	{
		if (SQLITE_OK == sqlite3_config(mode))
		{
			_threadMode = mode;
			if (SQLITE_OK == sqlite3_initialize())
				return true;
		}
		sqlite3_initialize();
	}
#endif
	return false;
}


void* Utility::eventHookRegister(sqlite3* pDB, UpdateCallbackType callbackFn, void* pParam)
{
	typedef void(*pF)(void*, int, const char*, const char*, sqlite3_int64);
	return sqlite3_update_hook(pDB, reinterpret_cast<pF>(callbackFn), pParam);
}


void* Utility::eventHookRegister(sqlite3* pDB, CommitCallbackType callbackFn, void* pParam)
{
	return sqlite3_commit_hook(pDB, callbackFn, pParam);
}


void* Utility::eventHookRegister(sqlite3* pDB, RollbackCallbackType callbackFn, void* pParam)
{
	return sqlite3_rollback_hook(pDB, callbackFn, pParam);
}


// NOTE: Utility::dbHandle() has been moved to SessionImpl.cpp,
// as a workaround for a failing AnyCast with Clang.
// See <https://github.com/pocoproject/poco/issues/578>
// for a discussion.


} } } // namespace Poco::Data::SQLite
