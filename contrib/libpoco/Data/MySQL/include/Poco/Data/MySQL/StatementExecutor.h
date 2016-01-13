//
// StatementExecutor.h
//
// $Id: //poco/1.4/Data/MySQL/include/Poco/Data/MySQL/StatementExecutor.h#1 $
//
// Library: Data
// Package: MySQL
// Module:  StatementExecutor
//
// Definition of the StatementExecutor class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_MySQL_StatementHandle_INCLUDED
#define Data_MySQL_StatementHandle_INCLUDED

#include <mysql.h>
#include "Poco/Data/MySQL/MySQLException.h"

namespace Poco {
namespace Data {
namespace MySQL {

class StatementExecutor
	/// MySQL statement executor.
{
public:
	enum State
	{
		STMT_INITED,
		STMT_COMPILED,
		STMT_EXECUTED
	};

	explicit StatementExecutor(MYSQL* mysql);
		/// Creates the StatementExecutor.

	~StatementExecutor();
		/// Destroys the StatementExecutor.

	int state() const;
		/// Returns the current state.

	void prepare(const std::string& query);
		/// Prepares the statement for execution.

	void bindParams(MYSQL_BIND* params, std::size_t count);
		/// Binds the params.

	void bindResult(MYSQL_BIND* result);
		/// Binds result.

	void execute();
		/// Executes the statement.

	bool fetch();
		/// Fetches the data.

	bool fetchColumn(std::size_t n, MYSQL_BIND *bind);
		/// Fetches the column.

	int getAffectedRowCount() const;
		
	operator MYSQL_STMT* ();
		/// Cast operator to native handle type.

private:

	StatementExecutor(const StatementExecutor&);
	StatementExecutor& operator=(const StatementExecutor&);

private:
	MYSQL*      _pSessionHandle;
	MYSQL_STMT* _pHandle;
	int         _state;
	int         _affectedRowCount;
	std::string _query;
};


//
// inlines
//

inline StatementExecutor::operator MYSQL_STMT* ()
{
	return _pHandle;
}


}}}


#endif // Data_MySQL_StatementHandle_INCLUDED
