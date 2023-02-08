//
// Transaction.h
//
// Library: Data
// Package: DataCore
// Module:  Transaction
//
// Definition of the Transaction class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//



#ifndef Data_Transaction_INCLUDED
#define Data_Transaction_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/Session.h"
#include "Poco/Logger.h"


namespace Poco {
namespace Data {


class Data_API Transaction
	/// Transaction helps with transactions in domain logic.
	/// When an Transaction object is created, it first checks whether a
	/// transaction is in progress. If not, a new transaction is created.
	/// When the Transaction is destroyed, and commit() has been called,
	/// nothing is done. Otherwise, the current transaction is rolled back.
	/// See Transaction for more details and purpose of this template.
{
public:
	Transaction(Poco::Data::Session& session, Poco::Logger* pLogger = 0);
		/// Creates the Transaction and starts it, using the given database session and logger.

	Transaction(Poco::Data::Session& session, bool start);
		/// Creates the Transaction, using the given database session.
		/// If start is true, transaction is started, otherwise begin() must be called
		/// to start the transaction.
		
	template <typename T>
	Transaction(Poco::Data::Session& rSession, T& t, Poco::Logger* pLogger = 0): 
		_rSession(rSession),
		_pLogger(pLogger)
		/// Creates the Transaction, using the given database session, transactor and logger.
		/// The transactor type must provide operator () overload taking non-const Session 
		/// reference as an argument.
		///
		/// When transaction is created using this constructor, it is executed and
		/// commited automatically. If no error occurs, rollback is disabled and does
		/// not occur at destruction time. If an error occurs resulting in exception being
		/// thrown, the transaction is rolled back and exception propagated to calling code.
		/// 
		/// Example usage:
		/// 
		/// struct Transactor
		/// {
		///		void operator () (Session& session) const
		///		{
		///			// do something ...
		///		}
		/// };
		/// 
		/// Transactor tr;
		/// Transaction tn(session, tr); 
	{
		try { transact(t); }
		catch (...)
		{
			if (_pLogger) _pLogger->error("Error executing transaction.");
			rollback();
			throw;
		}
	}

	~Transaction();
		/// Destroys the Transaction.
		/// Rolls back the current database transaction if it has not been commited
		/// (by calling commit()), or rolled back (by calling rollback()).
		///
		/// If an exception is thrown during rollback, the exception is logged
		/// and no further action is taken. 

	void setIsolation(Poco::UInt32 ti);
		/// Sets the transaction isolation level.

	Poco::UInt32 getIsolation();
		/// Returns the transaction isolation level.

	bool hasIsolation(Poco::UInt32 ti);
		/// Returns true iff the transaction isolation level corresponding
		/// to the supplied bitmask is supported.

	bool isIsolation(Poco::UInt32 ti);
		/// Returns true iff the transaction isolation level corresponds
		/// to the supplied bitmask.

	void execute(const std::string& sql, bool doCommit = true);
		/// Executes and, if doCommit is true, commits the transaction.
		/// Passing true value for commit disables rollback during destruction
		/// of this Transaction object.

	void execute(const std::vector<std::string>& sql);
		/// Executes all the SQL statements supplied in the vector and, after the last
		/// one is sucesfully executed, commits the transaction.
		/// If an error occurs during execution, transaction is rolled back.
		/// Passing true value for commit disables rollback during destruction
		/// of this Transaction object.

	template <typename T>
	void transact(T& t)
		/// Executes the transactor and, unless transactor throws an exception, 
		/// commits the transaction.
	{
		if (!isActive()) begin();
		t(_rSession);
		commit();
	}

	void commit();
		/// Commits the current transaction.
		
	void rollback();
		/// Rolls back the current transaction.
		
	bool isActive();
		/// Returns false after the transaction has been committed or rolled back,
		/// true if the transaction is ongoing.

	void setLogger(Poco::Logger* pLogger);
		/// Sets the logger for this transaction.
		/// Transaction does not take the ownership of the pointer.

private:
	Transaction();
	Transaction(const Transaction&);
	Transaction& operator = (const Transaction&);
	
	void begin();
		/// Begins the transaction if the session is already not in transaction.
		/// Otherwise does nothing.

	Session _rSession;
	Logger* _pLogger;
};


inline bool Transaction::isActive()
{
	return _rSession.isTransaction();
}


inline void Transaction::setIsolation(Poco::UInt32 ti)
{
	_rSession.setTransactionIsolation(ti);
}


inline Poco::UInt32 Transaction::getIsolation()
{
	return _rSession.getTransactionIsolation();
}


inline bool Transaction::hasIsolation(Poco::UInt32 ti)
{
	return _rSession.isTransactionIsolation(ti);
}


inline bool Transaction::isIsolation(Poco::UInt32 ti)
{
	return _rSession.isTransactionIsolation(ti);
}


inline void Transaction::setLogger(Poco::Logger* pLogger)
{
	_pLogger = pLogger;
}


} } // namespace Poco::Data


#endif // Data_Transaction_INCLUDED
