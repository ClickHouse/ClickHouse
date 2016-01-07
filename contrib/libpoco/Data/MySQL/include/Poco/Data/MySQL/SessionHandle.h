//
// SesssionHandle.h
//
// $Id: //poco/1.4/Data/MySQL/include/Poco/Data/MySQL/SessionHandle.h#1 $
//
// Library: Data
// Package: MySQL
// Module:  SessionHandle
//
// Definition of the SessionHandle class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_MySQL_SessionHandle_INCLUDED
#define Data_MySQL_SessionHandle_INCLUDED

#include <mysql.h>
#include "Poco/Data/MySQL/MySQLException.h"

namespace Poco {
namespace Data {
namespace MySQL {

class SessionHandle
	/// MySQL session handle
{
public:

	explicit SessionHandle(MYSQL* mysql);
		/// Creates session handle

	~SessionHandle();
		/// Destroy handle, close connection

	void init(MYSQL* mysql = 0);
		/// Initializes the handle iff not initialized.

	void options(mysql_option opt);
		/// Set connection options

	void options(mysql_option opt, bool b);
		/// Set connection options

	void options(mysql_option opt, const char* c);
		/// Set connection options

	void options(mysql_option opt, unsigned int i);
		/// Set connection options

	void connect(const char* host, const char* user, const char* password, const char* db, unsigned int port);
		/// Connect to server

	void close();
		/// Close connection

	void startTransaction();
		/// Start transaction

	void commit();
		/// Commit transaction

	void rollback();
		/// Rollback transaction

	operator MYSQL* ();

private:

	SessionHandle(const SessionHandle&);
	SessionHandle& operator=(const SessionHandle&);

private:

	MYSQL* _pHandle;
};


//
// inlines
//

inline SessionHandle::operator MYSQL* ()
{
	return _pHandle;
}


}}}

#endif // Data_MySQL_SessionHandle_INCLUDED
