//
// PooledSessionHolder.h
//
// Library: Data
// Package: SessionPooling
// Module:  PooledSessionHolder
//
// Definition of the PooledSessionHolder class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Data_PooledSessionHolder_INCLUDED
#define Data_PooledSessionHolder_INCLUDED


#include "Poco/Data/Data.h"
#include "Poco/Data/SessionImpl.h"
#include "Poco/AutoPtr.h"
#include "Poco/Timestamp.h"
#include "Poco/Mutex.h"


namespace Poco {
namespace Data {


class SessionPool;


class Data_API PooledSessionHolder: public Poco::RefCountedObject
	/// This class is used by SessionPool to manage SessionImpl objects.
{
public:
	PooledSessionHolder(SessionPool& owner, SessionImpl* pSessionImpl);
		/// Creates the PooledSessionHolder.

	~PooledSessionHolder();
		/// Destroys the PooledSessionHolder.

	SessionImpl* session();
		/// Returns a pointer to the SessionImpl.

	SessionPool& owner();
		/// Returns a reference to the SessionHolder's owner.

	void access();
		/// Updates the last access timestamp.

	int idle() const;
		/// Returns the number of seconds the session has not been used.

private:
	SessionPool& _owner;
	Poco::AutoPtr<SessionImpl> _pImpl;
	Poco::Timestamp _lastUsed;
	mutable Poco::FastMutex _mutex;
};


//
// inlines
//
inline SessionImpl* PooledSessionHolder::session()
{
	return _pImpl;
}


inline SessionPool& PooledSessionHolder::owner()
{
	return _owner;
}


inline void PooledSessionHolder::access()
{
	Poco::FastMutex::ScopedLock lock(_mutex);
	
	_lastUsed.update();
}


inline int PooledSessionHolder::idle() const
{
	Poco::FastMutex::ScopedLock lock(_mutex);

	return (int) (_lastUsed.elapsed()/Poco::Timestamp::resolution());
}


} } // namespace Poco::Data


#endif // Data_PooledSessionHolder_INCLUDED
