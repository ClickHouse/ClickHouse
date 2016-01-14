//
// RemoteSyslogListener.h
//
// $Id: //poco/1.4/Net/include/Poco/Net/RemoteSyslogListener.h#5 $
//
// Library: Net
// Package: Logging
// Module:  RemoteSyslogListener
//
// Definition of the RemoteSyslogListener class.
//
// Copyright (c) 2007-2011, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_RemoteSyslogListener_INCLUDED
#define Net_RemoteSyslogListener_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Net/SocketAddress.h"
#include "Poco/ThreadPool.h"
#include "Poco/SplitterChannel.h"
#include "Poco/NotificationQueue.h"


namespace Poco {
namespace Net {


class RemoteUDPListener;
class SyslogParser;


class Net_API RemoteSyslogListener: public Poco::SplitterChannel
	/// RemoteSyslogListener implements listening for syslog messages
	/// sent over UDP, according to RFC 5424 "The Syslog Protocol" 
	/// and RFC 5426 "Transmission of syslog messages over UDP".
	///
	/// In addition, RemoteSyslogListener also supports the "old" BSD syslog
	/// protocol, as described in RFC 3164.
	///
	/// The RemoteSyslogListener is a subclass of Poco::SplitterChannel.
	/// Every received log message is sent to the channels registered
	/// with addChannel() or the "channel" property.
	///
	/// Poco::Message objects created by RemoteSyslogListener will have
	/// the following named parameters:
	///   - addr: IP address of the host/interface sending the message.
	///   - host: host name; only for "new" syslog messages.
	///   - app:  application name; only for "new" syslog messages.
{
public:
	RemoteSyslogListener();
		/// Creates the RemoteSyslogListener.

	RemoteSyslogListener(Poco::UInt16 port);
		/// Creates the RemoteSyslogListener, listening on the given port number.

	RemoteSyslogListener(Poco::UInt16 port, int threads);
		/// Creates the RemoteSyslogListener, listening on the given port number
		/// and using the number of threads for message processing.

	void setProperty(const std::string& name, const std::string& value);
		/// Sets the property with the given value.
		///
		/// The following properties are supported:
		///     * port: The UDP port number where to listen for UDP packets
		///       containing syslog messages. If 0 is specified, does not
		///       listen for UDP messages.
		///     * threads: The number of parser threads processing
		///       received syslog messages. Defaults to 1. A maximum
		///       of 16 threads is supported.
		
	std::string getProperty(const std::string& name) const;
		/// Returns the value of the property with the given name.

	void open();
		/// Starts the listener.

	void close();
		/// Stops the listener.
		
	void processMessage(const std::string& messageText);
		/// Parses a single line of text containing a syslog message
		/// and sends it down the filter chain.

	void enqueueMessage(const std::string& messageText, const Poco::Net::SocketAddress& senderAddress);
		/// Enqueues a single line of text containing a syslog message
		/// for asynchronous processing by a parser thread.

	static void registerChannel();
		/// Registers the channel with the global LoggingFactory.

	static const std::string PROP_PORT;
	static const std::string PROP_THREADS;

    static const std::string LOG_PROP_APP;
    static const std::string LOG_PROP_HOST;

protected:
	~RemoteSyslogListener();
		/// Destroys the RemoteSyslogListener.

private:
	RemoteUDPListener*      _pListener;
	SyslogParser*           _pParser;
	Poco::ThreadPool        _threadPool;
	Poco::NotificationQueue _queue;
	Poco::UInt16            _port;
	int                     _threads;
};


} } // namespace Poco::Net


#endif // Net_RemoteSyslogListener_INCLUDED
