//
// SQLiteException.h
//
// Library: Data/SQLite
// Package: SQLite
// Module:  SQLiteException
//
// Definition of SQLiteException.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef SQLite_SQLiteException_INCLUDED
#define SQLite_SQLiteException_INCLUDED


#include "Poco/Data/SQLite/SQLite.h"
#include "Poco/Data/DataException.h"


namespace Poco {
namespace Data {
namespace SQLite {


POCO_DECLARE_EXCEPTION(SQLite_API, SQLiteException, Poco::Data::DataException)
POCO_DECLARE_EXCEPTION(SQLite_API, InvalidSQLStatementException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, InternalDBErrorException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, DBAccessDeniedException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, ExecutionAbortedException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, DBLockedException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, TableLockedException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, NoMemoryException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, ReadOnlyException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, InterruptException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, IOErrorException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, CorruptImageException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, TableNotFoundException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, DatabaseFullException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, CantOpenDBFileException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, LockProtocolException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, SchemaDiffersException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, RowTooBigException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, ConstraintViolationException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, DataTypeMismatchException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, ParameterCountMismatchException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, InvalidLibraryUseException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, OSFeaturesMissingException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, AuthorizationDeniedException, SQLiteException)
POCO_DECLARE_EXCEPTION(SQLite_API, TransactionException, SQLiteException)


} } } // namespace Poco::Data::SQLite


#endif
