//
// SocketConnector.h
//
// Library: Net
// Package: Reactor
// Module:  SocketConnector
//
// Definition of the SocketConnector class.
//
// Copyright (c) 2005-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_SocketConnector_INCLUDED
#define Net_SocketConnector_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/SocketNotification.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/Net/StreamSocket.h"
#include "Poco/Observer.h"


namespace Poco {
namespace Net {


template <class ServiceHandler>
class SocketConnector
	/// This class implements the Connector part of the
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
	/// The Connector actively establishes a connection with a remote
	/// server socket (usually managed by an Acceptor) and initializes
	/// a Service Handler to manage the connection.
	///
	/// The SocketConnector sets up a StreamSocket, initiates a non-blocking
	/// connect operation and registers itself for ReadableNotification, WritableNotification
	/// and ErrorNotification. ReadableNotification or WritableNotification denote the successful 
	/// establishment of the connection.
	///
	/// When the StreamSocket becomes readable or writeable, the SocketConnector 
	/// creates a ServiceHandler to service the connection and unregisters
	/// itself.
	///
	/// In case of an error (ErrorNotification), the SocketConnector unregisters itself
	/// and calls the onError() method, which can be overridden by subclasses
	/// to perform custom error handling.
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
	explicit SocketConnector(SocketAddress& address):
		_pReactor(0)
		/// Creates a SocketConnector, using the given Socket.
	{
		_socket.connectNB(address);
	}

	SocketConnector(SocketAddress& address, SocketReactor& reactor):
		_pReactor(0)
		/// Creates an acceptor, using the given ServerSocket.
		/// The SocketConnector registers itself with the given SocketReactor.
	{
		_socket.connectNB(address);
		registerConnector(reactor);
	}

	virtual ~SocketConnector()
		/// Destroys the SocketConnector.
	{
		try
		{
			unregisterConnector();
		}
		catch (...)
		{
			poco_unexpected();
		}
	}
	
	virtual void registerConnector(SocketReactor& reactor)
		/// Registers the SocketConnector with a SocketReactor.
		///
		/// A subclass can override this and, for example, also register
		/// an event handler for a timeout event.
		///
		/// The overriding method must call the baseclass implementation first.
	{
		_pReactor = &reactor;
		_pReactor->addEventHandler(_socket, Poco::Observer<SocketConnector, ReadableNotification>(*this, &SocketConnector::onReadable));
		_pReactor->addEventHandler(_socket, Poco::Observer<SocketConnector, WritableNotification>(*this, &SocketConnector::onWritable));
		_pReactor->addEventHandler(_socket, Poco::Observer<SocketConnector, ErrorNotification>(*this, &SocketConnector::onError));
	}
	
	virtual void unregisterConnector()
		/// Unregisters the SocketConnector.
		///
		/// A subclass can override this and, for example, also unregister
		/// its event handler for a timeout event.
		///
		/// The overriding method must call the baseclass implementation first.
	{
		if (_pReactor)
		{
			_pReactor->removeEventHandler(_socket, Poco::Observer<SocketConnector, ReadableNotification>(*this, &SocketConnector::onReadable));
			_pReactor->removeEventHandler(_socket, Poco::Observer<SocketConnector, WritableNotification>(*this, &SocketConnector::onWritable));
			_pReactor->removeEventHandler(_socket, Poco::Observer<SocketConnector, ErrorNotification>(*this, &SocketConnector::onError));
		}
	}
	
	void onReadable(ReadableNotification* pNotification)
	{
		pNotification->release();
		int err = _socket.impl()->socketError(); 
		if (err)
		{
			onError(err);
			unregisterConnector();
		}
		else
		{
			onConnect();
		}
	}
	
	void onWritable(WritableNotification* pNotification)
	{
		pNotification->release();
		onConnect();
	}
	
	void onConnect()
	{
		_socket.setBlocking(true);
		createServiceHandler();
		unregisterConnector();
	}
	
	void onError(ErrorNotification* pNotification)
	{
		pNotification->release();
		onError(_socket.impl()->socketError());
		unregisterConnector();
	}
	
protected:
	virtual ServiceHandler* createServiceHandler()
		/// Create and initialize a new ServiceHandler instance.
		///
		/// Subclasses can override this method.
	{
		return new ServiceHandler(_socket, *_pReactor);
	}

	virtual void onError(int errorCode)
		/// Called when the socket cannot be connected.
		///
		/// Subclasses can override this method.
	{
	}
	
	SocketReactor* reactor()
		/// Returns a pointer to the SocketReactor where
		/// this SocketConnector is registered.
		///
		/// The pointer may be null.
	{
		return _pReactor;
	}
	
	StreamSocket& socket()
		/// Returns a reference to the SocketConnector's socket.
	{
		return _socket;
	}

private:
	SocketConnector();
	SocketConnector(const SocketConnector&);
	SocketConnector& operator = (const SocketConnector&);
	
	StreamSocket   _socket;
	SocketReactor* _pReactor;
};


} } // namespace Poco::Net


#endif // Net_SocketConnector_INCLUDED
