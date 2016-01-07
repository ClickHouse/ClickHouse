//
// PooledSessionImpl.h
//
// $Id: //poco/Main/Data/include/Poco/Data/PooledSessionImpl.h#3 $
//
// Library: Data
// Package: SessionPooling
// Module:  PooledSessionImpl
//
// Definition of the PooledSessionImpl class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_PooledSessionImpl_INCLUDED
#define Data_PooledSessionImpl_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/SessionImpl.h"
#include "Poco/Data/PooledSessionHolder.h"
#include "Poco/AutoPtr.h"


namespace Poco {
namespace Data {


class SessionPool;


class Data_API PooledSessionImpl: public SessionImpl
	/// PooledSessionImpl is a decorator created by
	/// SessionPool that adds session pool
	/// management to SessionImpl objects.
{
public:
	PooledSessionImpl(PooledSessionHolder* pHolder);
		/// Creates the PooledSessionImpl.

	~PooledSessionImpl();
		/// Destroys the PooledSessionImpl.

	// SessionImpl
	StatementImpl* createStatementImpl();
	void begin();
	void commit();
	void rollback();
	void open(const std::string& connect = "");
	void close();
	bool isConnected();
	void setConnectionTimeout(std::size_t timeout);
	std::size_t getConnectionTimeout();
	bool canTransact();
	bool isTransaction();
	void setTransactionIsolation(Poco::UInt32);
	Poco::UInt32 getTransactionIsolation();
	bool hasTransactionIsolation(Poco::UInt32);
	bool isTransactionIsolation(Poco::UInt32);
	const std::string& connectorName() const;
	void setFeature(const std::string& name, bool state);	
	bool getFeature(const std::string& name);
	void setProperty(const std::string& name, const Poco::Any& value);
	Poco::Any getProperty(const std::string& name);
	
protected:
	SessionImpl* access() const;
		/// Updates the last access timestamp,
		/// verifies validity of the session
		/// and returns the session if it is valid.
		///
		/// Throws an SessionUnavailableException if the
		/// session is no longer valid.
		
	SessionImpl* impl() const;
		/// Returns a pointer to the SessionImpl.
				
private:	
	mutable Poco::AutoPtr<PooledSessionHolder> _pHolder;
};


//
// inlines
//
inline SessionImpl* PooledSessionImpl::impl() const
{
	return _pHolder->session();
}


} } // namespace Poco::Data


#endif // Data_PooledSessionImpl_INCLUDED
