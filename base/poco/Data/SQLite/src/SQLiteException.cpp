//
// SQLiteException.cpp
//
// Library: Data/SQLite
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


POCO_IMPLEMENT_EXCEPTION(SQLiteException, Poco::Data::DataException, "SQLite exception")
POCO_IMPLEMENT_EXCEPTION(InvalidSQLStatementException, SQLiteException, "Invalid SQL statement")
POCO_IMPLEMENT_EXCEPTION(InternalDBErrorException, SQLiteException, "Internal DB error")
POCO_IMPLEMENT_EXCEPTION(DBAccessDeniedException, SQLiteException, "DB access denied")
POCO_IMPLEMENT_EXCEPTION(ExecutionAbortedException, SQLiteException, "Execution aborted")
POCO_IMPLEMENT_EXCEPTION(DBLockedException, SQLiteException, "DB locked")
POCO_IMPLEMENT_EXCEPTION(TableLockedException, SQLiteException, "Table locked")
POCO_IMPLEMENT_EXCEPTION(NoMemoryException, SQLiteException, "Out of Memory")
POCO_IMPLEMENT_EXCEPTION(ReadOnlyException, SQLiteException, "Read only")
POCO_IMPLEMENT_EXCEPTION(InterruptException, SQLiteException, "Interrupt")
POCO_IMPLEMENT_EXCEPTION(IOErrorException, SQLiteException, "I/O error")
POCO_IMPLEMENT_EXCEPTION(CorruptImageException, SQLiteException, "Corrupt image")
POCO_IMPLEMENT_EXCEPTION(TableNotFoundException, SQLiteException, "Table not found")
POCO_IMPLEMENT_EXCEPTION(DatabaseFullException, SQLiteException, "Database full")
POCO_IMPLEMENT_EXCEPTION(CantOpenDBFileException, SQLiteException, "Can't open DB file")
POCO_IMPLEMENT_EXCEPTION(LockProtocolException, SQLiteException, "Lock protocol")
POCO_IMPLEMENT_EXCEPTION(SchemaDiffersException, SQLiteException, "Schema differs")
POCO_IMPLEMENT_EXCEPTION(RowTooBigException, SQLiteException, "Row too big")
POCO_IMPLEMENT_EXCEPTION(ConstraintViolationException, SQLiteException, "Constraint violation")
POCO_IMPLEMENT_EXCEPTION(DataTypeMismatchException, SQLiteException, "Data type mismatch")
POCO_IMPLEMENT_EXCEPTION(ParameterCountMismatchException, SQLiteException, "Parameter count mismatch")
POCO_IMPLEMENT_EXCEPTION(InvalidLibraryUseException, SQLiteException, "Invalid library use")
POCO_IMPLEMENT_EXCEPTION(OSFeaturesMissingException, SQLiteException, "OS features missing")
POCO_IMPLEMENT_EXCEPTION(AuthorizationDeniedException, SQLiteException, "Authorization denied")
POCO_IMPLEMENT_EXCEPTION(TransactionException, SQLiteException, "Transaction exception")


} } } // namespace Poco::Data::SQLite
