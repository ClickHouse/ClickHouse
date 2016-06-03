//
// Session.h
//
// $Id: //poco/Main/Data/include/Poco/Data/Session.h#9 $
//
// Library: Data
// Package: DataCore
// Module:  Session
//
// Definition of the Session class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_Session_INCLUDED
#define Data_Session_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/SessionImpl.h"
#include "Poco/Data/Statement.h"
#include "Poco/Data/StatementCreator.h"
#include "Poco/Data/Binding.h"
#include "Poco/AutoPtr.h"
#include "Poco/Any.h"
#include <algorithm>


namespace Poco {
namespace Data {


class StatementImpl;


class Data_API Session
	/// A Session holds a connection to a Database and creates Statement objects.
	///
	/// Sessions are always created via the SessionFactory:
	///    
	///     Session ses(SessionFactory::instance().create(connectorKey, connectionString));
	///    
	/// where the first param presents the type of session one wants to create (e.g., for SQLite one would choose "SQLite",
	/// for ODBC the key is "ODBC") and the second param is the connection string that the session implementation 
	/// requires to connect to the database. The format of the connection string is specific to the actual connector.
	///
	/// A simpler form to create the session is to pass the connector key and connection string directly to
	/// the Session constructor.
	///
	/// A concrete example to open an SQLite database stored in the file "dummy.db" would be
	///    
	///     Session ses("SQLite", "dummy.db");
	///    
	/// Via a Session one can create two different types of statements. First, statements that should only be executed once and immediately, and
	/// second, statements that should be executed multiple times, using a separate execute() call.
	/// The simple one is immediate execution:
	///    
	///     ses << "CREATE TABLE Dummy (data INTEGER(10))", now;
	///
	/// The now at the end of the statement is required, otherwise the statement
	/// would not be executed.
	///    
	/// If one wants to reuse a Statement (and avoid the overhead of repeatedly parsing an SQL statement)
	/// one uses an explicit Statement object and its execute() method:
	///    
	///     int i = 0;
	///     Statement stmt = (ses << "INSERT INTO Dummy VALUES(:data)", use(i));
	///    
	///     for (i = 0; i < 100; ++i)
	///     {
	///         stmt.execute();
	///     }
	///    
	/// The above example assigns the variable i to the ":data" placeholder in the SQL query. The query is parsed and compiled exactly
	/// once, but executed 100 times. At the end the values 0 to 99 will be present in the Table "DUMMY".
	///
	/// A faster implementaton of the above code will simply create a vector of int
	/// and use the vector as parameter to the use clause (you could also use set or multiset instead):
	///    
	///     std::vector<int> data;
	///     for (int i = 0; i < 100; ++i)
	///     {
	///         data.push_back(i);
	///     }
	///     ses << "INSERT INTO Dummy VALUES(:data)", use(data);
	///
	/// NEVER try to bind to an empty collection. This will give a BindingException at run-time!
	///
	/// Retrieving data from a database works similar, you could use simple data types, vectors, sets or multiset as your targets:
	///
	///     std::set<int> retData;
	///     ses << "SELECT * FROM Dummy", into(retData));
	///
	/// Due to the blocking nature of the above call it is possible to partition the data retrieval into chunks by setting a limit to
	/// the maximum number of rows retrieved from the database:
	///
	///     std::set<int> retData;
	///     Statement stmt = (ses << "SELECT * FROM Dummy", into(retData), limit(50));
	///     while (!stmt.done())
	///     {
	///         stmt.execute();
	///     }
	///
	/// The "into" keyword is used to inform the statement where output results should be placed. The limit value ensures
	/// that during each run at most 50 rows are retrieved. Assuming Dummy contains 100 rows, retData will contain 50 
	/// elements after the first run and 100 after the second run, i.e.
	/// the collection is not cleared between consecutive runs. After the second execute stmt.done() will return true.
	///
	/// A prepared Statement will behave exactly the same but a further call to execute() will simply reset the Statement, 
	/// execute it again and append more data to the result set.
	///
	/// Note that it is possible to append several "bind" or "into" clauses to the statement. Theoretically, one could also have several
	/// limit clauses but only the last one that was added will be effective. 
	/// Also several preconditions must be met concerning binds and intos.
	/// Take the following example:
	///
	///     ses << "CREATE TABLE Person (LastName VARCHAR(30), FirstName VARCHAR, Age INTEGER(3))";
	///     std::vector<std::string> nameVec; // [...] add some elements
	///     std::vector<int> ageVec; // [...] add some elements
	///     ses << "INSERT INTO Person (LastName, Age) VALUES(:ln, :age)", use(nameVec), use(ageVec);
	///
	/// The size of all use parameters MUST be the same, otherwise an exception is thrown. Furthermore,
	/// the amount of use clauses must match the number of wildcards in the query (to be more precise: 
	/// each binding has a numberOfColumnsHandled() value which defaults to 1. The sum of all these values 
	/// must match the wildcard count in the query.
	/// However, this is only important if you have written your own TypeHandler specializations.
	/// If you plan to map complex object types to tables see the TypeHandler documentation.
	/// For now, we simply assume we have written one TypeHandler for Person objects. Instead of having n different vectors,
	/// we have one collection:
	///
	///     std::vector<Person> people; // [...] add some elements
	///     ses << "INSERT INTO Person (LastName, FirstName, Age) VALUES(:ln, :fn, :age)", use(people);
	///
	/// which will insert all Person objects from the people vector to the database (and again, you can use set, multiset too,
	/// even map and multimap if Person provides an operator() which returns the key for the map).
	/// The same works for a SELECT statement with "into" clauses:
	///
	///     std::vector<Person> people;
	///     ses << "SELECT * FROM PERSON", into(people);
	/// 
	/// Mixing constants or variables with manipulators is allowed provided there are corresponding placeholders for the constants provided in
	/// the SQL string, such as in following example:
	///
	///     std::vector<Person> people;
	///     ses << "SELECT * FROM %s", into(people), "PERSON";
	/// 
	/// Formatting only kicks in if there are values to be injected into the SQL string, otherwise it is skipped.
	/// If the formatting will occur and the percent sign is part of the query itself, it can be passed to the query by entering it twice (%%).
	/// However, if no formatting is used, one percent sign is sufficient as the string will be passed unaltered.
	/// For complete list of supported data types with their respective specifications, see the documentation for format in Foundation.
{
public:
	static const std::size_t LOGIN_TIMEOUT_DEFAULT = SessionImpl::LOGIN_TIMEOUT_DEFAULT;
	static const Poco::UInt32 TRANSACTION_READ_UNCOMMITTED = 0x00000001L;
	static const Poco::UInt32 TRANSACTION_READ_COMMITTED   = 0x00000002L;
	static const Poco::UInt32 TRANSACTION_REPEATABLE_READ  = 0x00000004L;
	static const Poco::UInt32 TRANSACTION_SERIALIZABLE     = 0x00000008L;

	Session(Poco::AutoPtr<SessionImpl> ptrImpl);
		/// Creates the Session.

	Session(const std::string& connector,
		const std::string& connectionString,
		std::size_t timeout = LOGIN_TIMEOUT_DEFAULT);
		/// Creates a new session, using the given connector (which must have
		/// been registered), and connectionString.

	Session(const std::string& connection,
		std::size_t timeout = LOGIN_TIMEOUT_DEFAULT);
		/// Creates a new session, using the given connection (must be in
		/// "connection:///connectionString" format).

	Session(const Session&);
		/// Creates a session by copying another one.

	Session& operator = (const Session&);
		/// Assignment operator.

	~Session();
		/// Destroys the Session.

	void swap(Session& other);
		/// Swaps the session with another one.

	template <typename T>
	Statement operator << (const T& t)
		/// Creates a Statement with the given data as SQLContent
	{
		return _statementCreator << t;
	}

	StatementImpl* createStatementImpl();
		/// Creates a StatementImpl.

	void open(const std::string& connect = "");
		/// Opens the session using the supplied string.
		/// Can also be used with default empty string to 
		/// reconnect a disconnected session.
		/// If the connection is not established, 
		/// a ConnectionFailedException is thrown. 
		/// Zero timout means indefinite

	void close();
		/// Closes the session.

	bool isConnected();
		/// Returns true iff session is connected, false otherwise.

	void reconnect();
		/// Closes the session and opens it.

	void setLoginTimeout(std::size_t timeout);
		/// Sets the session login timeout value.

	std::size_t getLoginTimeout() const;
		/// Returns the session login timeout value.

	void setConnectionTimeout(std::size_t timeout);
		/// Sets the session connection timeout value.

	std::size_t getConnectionTimeout();
		/// Returns the session connection timeout value.

	void begin();
		/// Starts a transaction.

	void commit();
		/// Commits and ends a transaction.

	void rollback();
		/// Rolls back and ends a transaction.

	bool canTransact();
		/// Returns true if session has transaction capabilities.

	bool isTransaction();
		/// Returns true iff a transaction is in progress, false otherwise.

	void setTransactionIsolation(Poco::UInt32);
		/// Sets the transaction isolation level.

	Poco::UInt32 getTransactionIsolation();
		/// Returns the transaction isolation level.

	bool hasTransactionIsolation(Poco::UInt32 ti);
		/// Returns true iff the transaction isolation level corresponding
		/// to the supplied bitmask is supported.

	bool isTransactionIsolation(Poco::UInt32 ti);
		/// Returns true iff the transaction isolation level corresponds
		/// to the supplied bitmask.

	std::string connector() const;
		/// Returns the connector name for this session.

	std::string uri() const;
		/// Returns the URI for this session.

	static std::string uri(const std::string& connector,
		const std::string& connectionString);
		/// Utility function that teturns the URI formatted from supplied 
		/// arguments as "connector:///connectionString".

	void setFeature(const std::string& name, bool state);
		/// Set the state of a feature.
		///
		/// Features are a generic extension mechanism for session implementations.
		/// and are defined by the underlying SessionImpl instance.
		///
		/// Throws a NotSupportedException if the requested feature is
		/// not supported by the underlying implementation.
	
	bool getFeature(const std::string& name) const;
		/// Look up the state of a feature.
		///
		/// Features are a generic extension mechanism for session implementations.
		/// and are defined by the underlying SessionImpl instance.
		///
		/// Throws a NotSupportedException if the requested feature is
		/// not supported by the underlying implementation.

	void setProperty(const std::string& name, const Poco::Any& value);
		/// Set the value of a property.
		///
		/// Properties are a generic extension mechanism for session implementations.
		/// and are defined by the underlying SessionImpl instance.
		///
		/// Throws a NotSupportedException if the requested property is
		/// not supported by the underlying implementation.

	Poco::Any getProperty(const std::string& name) const;
		/// Look up the value of a property.
		///
		/// Properties are a generic extension mechanism for session implementations.
		/// and are defined by the underlying SessionImpl instance.
		///
		/// Throws a NotSupportedException if the requested property is
		/// not supported by the underlying implementation.

	SessionImpl* impl();
		/// Returns a pointer to the underlying SessionImpl.

private:
	Session();

	Poco::AutoPtr<SessionImpl> _pImpl;
	StatementCreator           _statementCreator;
};


//
// inlines
//
inline StatementImpl* Session::createStatementImpl()
{
	return _pImpl->createStatementImpl();
}


inline void Session::open(const std::string& connect)
{
	_pImpl->open(connect);
}


inline void Session::close()
{
	_pImpl->close();
}


inline bool Session::isConnected()
{
	return _pImpl->isConnected();
}


inline void Session::reconnect()
{
	_pImpl->reconnect();
}


inline void Session::setLoginTimeout(std::size_t timeout)
{
	_pImpl->setLoginTimeout(timeout);
}


inline std::size_t Session::getLoginTimeout() const
{
	return _pImpl->getLoginTimeout();
}


inline void Session::setConnectionTimeout(std::size_t timeout)
{
	_pImpl->setConnectionTimeout(timeout);
}


inline std::size_t Session::getConnectionTimeout()
{
	return _pImpl->getConnectionTimeout();
}


inline void Session::begin()
{
	return _pImpl->begin();
}


inline void Session::commit()
{
	return _pImpl->commit();
}


inline void Session::rollback()
{
	return _pImpl->rollback();
}


inline bool Session::canTransact()
{
	return _pImpl->canTransact();
}


inline bool Session::isTransaction()
{
	return _pImpl->isTransaction();
}


inline void Session::setTransactionIsolation(Poco::UInt32 ti)
{
	_pImpl->setTransactionIsolation(ti);
}


inline Poco::UInt32 Session::getTransactionIsolation()
{
	return _pImpl->getTransactionIsolation();
}


inline bool Session::hasTransactionIsolation(Poco::UInt32 ti)
{
	return _pImpl->hasTransactionIsolation(ti);
}


inline bool Session::isTransactionIsolation(Poco::UInt32 ti)
{
	return _pImpl->isTransactionIsolation(ti);
}


inline std::string Session::connector() const
{
	return _pImpl->connectorName();
}


inline std::string Session::uri(const std::string& connector,
	const std::string& connectionString)
{
	return SessionImpl::uri(connector, connectionString);
}


inline std::string Session::uri() const
{
	return _pImpl->uri();
}


inline void Session::setFeature(const std::string& name, bool state)
{
	_pImpl->setFeature(name, state);
}


inline bool Session::getFeature(const std::string& name) const
{
	return const_cast<SessionImpl*>(_pImpl.get())->getFeature(name);
}


inline void Session::setProperty(const std::string& name, const Poco::Any& value)
{
	_pImpl->setProperty(name, value);
}


inline Poco::Any Session::getProperty(const std::string& name) const
{
	return const_cast<SessionImpl*>(_pImpl.get())->getProperty(name);
}


inline SessionImpl* Session::impl()
{
	return _pImpl;
}


inline void swap(Session& s1, Session& s2)
{
	s1.swap(s2);
}


} } // namespace Poco::Data


namespace std
{
	template<>
	inline void swap<Poco::Data::Session>(Poco::Data::Session& s1, 
		Poco::Data::Session& s2)
		/// Full template specalization of std:::swap for Session
	{
		s1.swap(s2);
	}
}


#endif // Data_Session_INCLUDED
