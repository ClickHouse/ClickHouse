//
// SQLChannel.h
//
// Library: Data
// Package: Logging
// Module:  SQLChannel
//
// Definition of the SQLChannel class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_SQLChannel_INCLUDED
#define Data_SQLChannel_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/Connector.h"
#include "Poco/Data/Session.h"
#include "Poco/Data/Statement.h"
#include "Poco/Data/ArchiveStrategy.h"
#include "Poco/Channel.h"
#include "Poco/Message.h"
#include "Poco/AutoPtr.h"
#include "Poco/String.h"


namespace Poco {
namespace Data {


class Data_API SQLChannel: public Poco::Channel
	/// This Channel implements logging to a SQL database.
	/// The channel is dependent on the schema. The DDL for
	/// table creation (subject to target DDL dialect dependent 
	/// modifications) is:
	///
	/// "CREATE TABLE T_POCO_LOG (Source VARCHAR, 
	///		Name VARCHAR, 
	///		ProcessId INTEGER, 
	///		Thread VARCHAR, 
	///		ThreadId INTEGER, 
	///		Priority INTEGER, 
	///		Text VARCHAR, 
	///		DateTime DATE)"
	///
	/// The table name is configurable through "table" property. 
	/// Other than DateTime filed name used for optiona time-based archiving purposes, currently the 
	/// field names are not mandated. However, it is recomended to use names as specified above.
	/// 
	/// To provide as non-intrusive operation as possbile, the log entries are cached and 
	/// inserted into the target database asynchronously by default . The blocking, however, will occur 
	/// before the next entry insertion with default timeout of 1 second. The default settings can be 
	/// overriden (see async, timeout and throw properties for details).
	/// If throw property is false, insertion timeouts are ignored, otherwise a TimeoutException is thrown.
	/// To force insertion of every entry, set timeout to 0. This setting, however, introduces
	/// a risk of long blocking periods in case of remote server communication delays.
{
public:
	SQLChannel();
		/// Creates SQLChannel.

	SQLChannel(const std::string& connector, 
		const std::string& connect,
		const std::string& name = "-");
		/// Creates a SQLChannel with the given connector, connect string, timeout, table and name.
		/// The connector must be already registered.
	
	void open();
		/// Opens the SQLChannel.
		
	void close();
		/// Closes the SQLChannel.
		
	void log(const Message& msg);
		/// Sends the message's text to the syslog service.
		
	void setProperty(const std::string& name, const std::string& value);
		/// Sets the property with the given value.
		///
		/// The following properties are supported:
		///     * name:      The name used to identify the source of log messages.
		///                  Defaults to "-".
		///
		///     * target:    The target data storage type ("SQLite", "ODBC", ...).
		///
		///     * connector: The target data storage connector name.
		///
		///     * connect:   The target data storage connection string.
		///
		///     * table:     Destination log table name. Defaults to "T_POCO_LOG".
		///                  Table must exist in the target database.
		///
		///     * keep:      Max row age for the log table. To disable archiving,
		///                  set this property to empty string or "forever".
		///
		///     * archive:   Archive table name. Defaults to "T_POCO_LOG_ARCHIVE".
		///                  Table must exist in the target database. To disable archiving,
		///                  set this property to empty string.
		///
		///     * async:     Indicates asynchronous execution. When excuting asynchronously,
		///                  messages are sent to the target using asyncronous execution.
		///                  However, prior to the next message being processed and sent to
		///                  the target, the previous operation must have been either completed 
		///                  or timed out (see timeout and throw properties for details on
		///                  how abnormal conditos are handled).
		///
		///     * timeout:   Timeout (ms) to wait for previous log operation completion.
		///                  Values "0" and "" mean no timeout. Only valid when logging
		///                  is asynchronous, otherwise ignored.
		///
		///     * throw:     Boolean value indicating whether to throw in case of timeout.
		///                  Setting this property to false may result in log entries being lost.
		///                  True values are (case insensitive) "true", "t", "yes", "y".
		///                  Anything else yields false.
		
	std::string getProperty(const std::string& name) const;
		/// Returns the value of the property with the given name.

	std::size_t wait();
		/// Waits for the completion of the previous operation and returns
		/// the result. If chanel is in synchronous mode, returns 0 immediately.

	static void registerChannel();
		/// Registers the channel with the global LoggingFactory.

	static const std::string PROP_CONNECT;
	static const std::string PROP_CONNECTOR;
	static const std::string PROP_NAME;
	static const std::string PROP_TABLE;
	static const std::string PROP_ARCHIVE_TABLE;
	static const std::string PROP_MAX_AGE;
	static const std::string PROP_ASYNC;
	static const std::string PROP_TIMEOUT;
	static const std::string PROP_THROW;

protected:
	~SQLChannel();

private:
	typedef Poco::SharedPtr<Session>         SessionPtr;
	typedef Poco::SharedPtr<Statement>       StatementPtr;
	typedef Poco::Message::Priority          Priority;
	typedef Poco::SharedPtr<ArchiveStrategy> StrategyPtr;

	void initLogStatement();
		/// Initiallizes the log statement.

	void initArchiveStatements();
	/// Initiallizes the archive statement.

	void logAsync(const Message& msg);
		/// Waits for previous operation completion and
		/// calls logSync(). If the previous operation times out,
		/// and _throw is true, TimeoutException is thrown, oterwise
		/// the timeout is ignored and log entry is lost.

	void logSync(const Message& msg);
		/// Inserts the message in the target database.

	bool isTrue(const std::string& value) const;
		/// Returns true is value is "true", "t", "yes" or "y".
		/// Case insensitive.

	std::string  _connector;
	std::string  _connect;
	SessionPtr   _pSession;
	StatementPtr _pLogStatement;
	std::string  _name;
	std::string  _table;
	int          _timeout;
	bool         _throw;
	bool         _async;
	
	// members for log entry cache (needed for async mode)
	std::string _source;
	long        _pid;
	std::string _thread;
	long        _tid;
	int         _priority;
	std::string _text;
	DateTime    _dateTime;

	StrategyPtr _pArchiveStrategy;
};


//
// inlines
//

inline std::size_t SQLChannel::wait()
{
	if (_async && _pLogStatement) 
		return _pLogStatement->wait(_timeout);
	
	return 0;
}


inline bool SQLChannel::isTrue(const std::string& value) const
{
	return ((0 == icompare(value, "true")) ||
			(0 == icompare(value, "t")) ||
			(0 == icompare(value, "yes")) ||
			(0 == icompare(value, "y")));
}


} } // namespace Poco::Data


#endif // Data_SQLChannel_INCLUDED
