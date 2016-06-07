//
// SQLiteException.cpp
//
// $Id: //poco/Main/Data/SQLite/src/SQLiteException.cpp#2 $
//
// Library: SQLite
// Package: SQLite
// Module:  SQLiteException
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Data/SQLite/SQLiteException.h"
#include <typeinfo>


namespace Poco {
namespace Data {
namespace SQLite {


POCO_IMPLEMENT_EXCEPTION(SQLiteException, Poco::Data::DataException, "Generic SQLite error")
POCO_IMPLEMENT_EXCEPTION(InvalidSQLStatementException, SQLiteException, "SQL Statement invalid")
POCO_IMPLEMENT_EXCEPTION(InternalDBErrorException, SQLiteException, "Internal error")
POCO_IMPLEMENT_EXCEPTION(DBAccessDeniedException, SQLiteException, "Access permission denied")
POCO_IMPLEMENT_EXCEPTION(ExecutionAbortedException, SQLiteException, "Execution of SQL statement aborted")
POCO_IMPLEMENT_EXCEPTION(DBLockedException, SQLiteException, "The database is locked")
POCO_IMPLEMENT_EXCEPTION(TableLockedException, SQLiteException, "A table in the database is locked")
POCO_IMPLEMENT_EXCEPTION(NoMemoryException, SQLiteException, "Out of Memory")
POCO_IMPLEMENT_EXCEPTION(ReadOnlyException, SQLiteException, "Attempt to write a readonly database")
POCO_IMPLEMENT_EXCEPTION(InterruptException, SQLiteException, "Operation terminated by an interrupt")
POCO_IMPLEMENT_EXCEPTION(IOErrorException, SQLiteException, "Some kind of disk I/O error occurred")
POCO_IMPLEMENT_EXCEPTION(CorruptImageException, SQLiteException, "The database disk image is malformed")
POCO_IMPLEMENT_EXCEPTION(TableNotFoundException, SQLiteException, "Table not found")
POCO_IMPLEMENT_EXCEPTION(DatabaseFullException, SQLiteException, "Insertion failed because database is full")
POCO_IMPLEMENT_EXCEPTION(CantOpenDBFileException, SQLiteException, "Unable to open the database file")
POCO_IMPLEMENT_EXCEPTION(LockProtocolException, SQLiteException, "Database lock protocol error")
POCO_IMPLEMENT_EXCEPTION(SchemaDiffersException, SQLiteException, "The database schema changed")
POCO_IMPLEMENT_EXCEPTION(RowTooBigException, SQLiteException, "Too much data for one row of a table")
POCO_IMPLEMENT_EXCEPTION(ConstraintViolationException, SQLiteException, "Abort due to constraint violation")
POCO_IMPLEMENT_EXCEPTION(DataTypeMismatchException, SQLiteException, "Data type mismatch")
POCO_IMPLEMENT_EXCEPTION(ParameterCountMismatchException, SQLiteException, "Parameter count mismatch")
POCO_IMPLEMENT_EXCEPTION(InvalidLibraryUseException, SQLiteException, "Library used incorrectly")
POCO_IMPLEMENT_EXCEPTION(OSFeaturesMissingException, SQLiteException, "Uses OS features not supported on host")
POCO_IMPLEMENT_EXCEPTION(AuthorizationDeniedException, SQLiteException, "Authorization denied")
POCO_IMPLEMENT_EXCEPTION(TransactionException, SQLiteException, "Transaction exception")


} } } // namespace Poco::Data::SQLite
