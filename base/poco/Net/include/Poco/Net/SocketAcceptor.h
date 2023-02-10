//
// SocketAcceptor.h
//
// Library: Net
// Package: Reactor
// Module:  SocketAcceptor
//
// Definition of the SocketAcceptor class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_SocketAcceptor_INCLUDED
#define Net_SocketAcceptor_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/SocketNotification.h"
#include "Poco/Net/SocketReactor.h"
#include "Poco/Net/ServerSocket.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/Observer.h"


namespace Poco {
namespace Net {


template <class ServiceHandler>
class SocketAcceptor
	/// This class implements the Acceptor part of the
	/// Acceptor-Connector design pattern.
	///
	/// The Acceptor-Connector pattern has been described in the book
	/// "Pattern Languages of Program Design 3", edited by Robert Martin,
	/// Frank Buschmann and Dirk Riehle (Addison Wesley, 1997).
	///
	/// The Acceptor-Connector design pattern decouples connection
	/// establishment and service initialization in a distributed system
	/// from the processing performed once a service is initialized.
	/// This decoupling is achieved with three components: Acceptors, 
	/// Connectors and Service Handlers.
	/// The SocketAcceptor passively waits for connection requests (usually
	/// from a remote Connector) and establishes a connection upon
	/// arrival of a connection requests. Also, a Service Handler is
	/// initialized to process the data arriving via the connection in
	/// an application-specific way.
	///
	/// The SocketAcceptor sets up a ServerSocket and registers itself
	/// for a ReadableNotification, denoting an incoming connection request.
	///
	/// When the ServerSocket becomes readable the SocketAcceptor accepts
	/// the connection request and creates a ServiceHandler to
	/// service the connection.
	///
	/// The ServiceHandler class must provide a constructor that
	/// takes a StreamSocket and a SocketReactor as arguments,
	/// e.g.:
	///     MyServiceHandler(const StreamSocket& socket, ServiceReactor& reactor)
	///
	/// When the ServiceHandler is done, it must destroy itself.
	///
	/// Subclasses can override the createServiceHandler() factory method
	/// if special steps are necessary to create a ServiceHandler object.
{
public:
	explicit SocketAcceptor(ServerSocket& socket):
		_socket(socket),
		_pReactor(0)
		/// Creates a SocketAcceptor, using the given ServerSocket.
	{
	}

	SocketAcceptor(ServerSocket& socket, SocketReactor& reactor):
		_socket(socket),
		_pReactor(&reactor)
		/// Creates a SocketAcceptor, using the given ServerSocket.
		/// The SocketAcceptor registers itself with the given SocketReactor.
	{
		_pReactor->addEventHandler(_socket, Poco::Observer<SocketAcceptor,
			ReadableNotification>(*this, &SocketAcceptor::onAccept));
	}

	virtual ~SocketAcceptor()
		/// Destroys the SocketAcceptor.
	{
		try
		{
			if (_pReactor)
			{
				_pReactor->removeEventHandler(_socket, Poco::Observer<SocketAcceptor,
					ReadableNotification>(*this, &SocketAcceptor::onAccept));
			}
		}
		catch (...)
		{
			poco_unexpected();
		}
	}

	void setReactor(SocketReactor& reactor)
		/// Sets the reactor for this acceptor.
	{
		_pReactor = &reactor;
		if (!_pReactor->hasEventHandler(_socket, Poco::Observer<SocketAcceptor,
			ReadableNotification>(*this, &SocketAcceptor::onAccept)))
		{
			registerAcceptor(reactor);
		}
	}

	virtual void registerAcceptor(SocketReactor& reactor)
		/// Registers the SocketAcceptor with a SocketReactor.
		///
		/// A subclass can override this function to e.g.
		/// register an event handler for timeout event.
		/// 
		/// If acceptor was constructed without providing reactor to it,
		/// the override of this method must either call the base class
		/// implementation or directly register the accept handler with
		/// the reactor.
	{
		if (_pReactor)
			throw Poco::InvalidAccessException("Acceptor already registered.");

		_pReactor = &reactor;
		_pReactor->addEventHandler(_socket, Poco::Observer<SocketAcceptor, ReadableNotification>(*this, &SocketAcceptor::onAccept));
	}
	
	virtual void unregisterAcceptor()
		/// Unregisters the SocketAcceptor.
		///
		/// A subclass can override this function to e.g.
		/// unregister its event handler for a timeout event.
		/// 
		/// If the accept handler was registered with the reactor,
		/// the overriding method must either call the base class
		/// implementation or directly unregister the accept handler.
	{
		if (_pReactor)
		{
			_pReactor->removeEventHandler(_socket, Poco::Observer<SocketAcceptor, ReadableNotification>(*this, &SocketAcceptor::onAccept));
		}
	}
	
	void onAccept(ReadableNotification* pNotification)
		/// Accepts connection and creates event handler.
	{
		pNotification->release();
		StreamSocket sock = _socket.acceptConnection();
		_pReactor->wakeUp();
		createServiceHandler(sock);
	}
	
protected:
	virtual ServiceHandler* createServiceHandler(StreamSocket& socket)
		/// Create and initialize a new ServiceHandler instance.
		///
		/// Subclasses can override this method.
	{
		return new ServiceHandler(socket, *_pReactor);
	}

	SocketReactor* reactor()
		/// Returns a pointer to the SocketReactor where
		/// this SocketAcceptor is registered.
		///
		/// The pointer may be null.
	{
		return _pReactor;
	}

	Socket& socket()
		/// Returns a reference to the SocketAcceptor's socket.
	{
		return _socket;
	}

private:
	SocketAcceptor();
	SocketAcceptor(const SocketAcceptor&);
	SocketAcceptor& operator = (const SocketAcceptor&);
	
	ServerSocket   _socket;
	SocketReactor* _pReactor;
};


} } // namespace Poco::Net


#endif // Net_SocketAcceptor_INCLUDED
