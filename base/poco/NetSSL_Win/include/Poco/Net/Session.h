//
// Session.h
//
// Library: NetSSL_Win
// Package: SSLCore
// Module:  Session
//
// Definition of the Session class.
//
// Copyright (c) 2010-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_Session_INCLUDED
#define NetSSL_Session_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/RefCountedObject.h"
#include "Poco/AutoPtr.h"


namespace Poco {
namespace Net {


class NetSSL_Win_API Session: public Poco::RefCountedObject
	/// This class encapsulates a SSL session object
	/// used with session caching on the client side.
	///
	/// For session caching to work, a client must
	/// save the session object from an existing connection,
	/// if it wants to reuse it with a future connection.
{
public:
	typedef Poco::AutoPtr<Session> Ptr;

protected:	
	Session();
		/// Creates a new Session object, using the given
		/// SSL_SESSION object. 
		/// 
		/// The SSL_SESSION's reference count is not changed.

	~Session();
		/// Destroys the Session.
		///
		/// Calls SSL_SESSION_free() on the stored
		/// SSL_SESSION object.

private:
	friend class SecureSocketImpl;
};


//
// inlines
//


} } // namespace Poco::Net


#endif // NetSSL_Session_INCLUDED
