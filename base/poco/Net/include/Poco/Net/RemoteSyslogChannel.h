//
// RemoteSyslogChannel.h
//
// Library: Net
// Package: Logging
// Module:  RemoteSyslogChannel
//
// Definition of the RemoteSyslogChannel class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Net_RemoteSyslogChannel_INCLUDED
#define Net_RemoteSyslogChannel_INCLUDED


#include "Poco/Net/Net.h"
#include "Poco/Channel.h"
#include "Poco/Mutex.h"
#include "Poco/Net/DatagramSocket.h"
#include "Poco/Net/SocketAddress.h"


namespace Poco {
namespace Net {


class Net_API RemoteSyslogChannel: public Poco::Channel
	/// This Channel implements remote syslog logging over UDP according
	/// to RFC 5424 "The Syslog Protocol"
	/// and RFC 5426 "Transmission of syslog messages over UDP".
	///
	/// In addition, RemoteSyslogListener also supports the "old" BSD syslog
	/// protocol, as described in RFC 3164.
	///
	/// RFC 5425 structured data can be passed via the "structured-data"
	/// property of the log Message. The content of the "structured-data"
	/// property must be correct according to RFC 5425.
	///
	/// Example:
	///     msg.set("structured-data", "[exampleSDID@32473 iut=\"3\" eventSource=\"Application\" eventID=\"1011\"]");
{
public:
	static const std::string BSD_TIMEFORMAT;
	static const std::string SYSLOG_TIMEFORMAT;

	enum Severity
	{
		SYSLOG_EMERGENCY     = 0, /// Emergency: system is unusable
		SYSLOG_ALERT         = 1, /// Alert: action must be taken immediately
		SYSLOG_CRITICAL      = 2, /// Critical: critical conditions
		SYSLOG_ERROR         = 3, /// Error: error conditions
		SYSLOG_WARNING       = 4, /// Warning: warning conditions
		SYSLOG_NOTICE        = 5, /// Notice: normal but significant condition
		SYSLOG_INFORMATIONAL = 6, /// Informational: informational messages
		SYSLOG_DEBUG         = 7  /// Debug: debug-level messages
	};

	enum Facility
	{
		SYSLOG_KERN     = ( 0<<3), /// kernel messages
		SYSLOG_USER     = ( 1<<3), /// random user-level messages
		SYSLOG_MAIL     = ( 2<<3), /// mail system
		SYSLOG_DAEMON   = ( 3<<3), /// system daemons
		SYSLOG_AUTH     = ( 4<<3), /// security/authorization messages
		SYSLOG_SYSLOG   = ( 5<<3), /// messages generated internally by syslogd
		SYSLOG_LPR      = ( 6<<3), /// line printer subsystem
		SYSLOG_NEWS     = ( 7<<3), /// network news subsystem
		SYSLOG_UUCP     = ( 8<<3), /// UUCP subsystem
		SYSLOG_CRON     = ( 9<<3), /// clock daemon
		SYSLOG_AUTHPRIV = (10<<3), /// security/authorization messages (private)
		SYSLOG_FTP      = (11<<3), /// ftp daemon
		SYSLOG_NTP      = (12<<3), /// ntp subsystem
		SYSLOG_LOGAUDIT = (13<<3), /// log audit
		SYSLOG_LOGALERT = (14<<3), /// log alert
		SYSLOG_CLOCK    = (15<<3), /// clock daemon
		SYSLOG_LOCAL0   = (16<<3), /// reserved for local use
		SYSLOG_LOCAL1   = (17<<3), /// reserved for local use
		SYSLOG_LOCAL2   = (18<<3), /// reserved for local use
		SYSLOG_LOCAL3   = (19<<3), /// reserved for local use
		SYSLOG_LOCAL4   = (20<<3), /// reserved for local use
		SYSLOG_LOCAL5   = (21<<3), /// reserved for local use
		SYSLOG_LOCAL6   = (22<<3), /// reserved for local use
		SYSLOG_LOCAL7   = (23<<3)  /// reserved for local use
	};

	enum
	{
		SYSLOG_PORT = 514
	};

	RemoteSyslogChannel();
		/// Creates a RemoteSyslogChannel.

	RemoteSyslogChannel(const std::string& address, const std::string& name, int facility = SYSLOG_USER, bool bsdFormat = false);
		/// Creates a RemoteSyslogChannel with the given target address, name, and facility.
		/// If bsdFormat is true, messages are formatted according to RFC 3164.

	void open();
		/// Opens the RemoteSyslogChannel.

	void close();
		/// Closes the RemoteSyslogChannel.

	void log(const Message& msg);
		/// Sends the message's text to the syslog service.

	void setProperty(const std::string& name, const std::string& value);
		/// Sets the property with the given value.
		///
		/// The following properties are supported:
		///     * name:      The name used to identify the source of log messages.
		///     * facility:  The facility added to each log message. See the Facility enumeration for a list of supported values.
		///                  The LOG_ prefix can be omitted and values are case insensitive (e.g. a facility value "mail" is recognized as SYSLOG_MAIL)
		///     * format:    "bsd"/"rfc3164" (RFC 3164 format) or "new"/"rfc5424" (default)
		///     * loghost:   The target IP address or host name where log messages are sent. Optionally, a port number (separated
		///                  by a colon) can also be specified.
		///     * host:      (optional) Host name included in syslog messages. If not specified, the host's real domain name or
		///                  IP address will be used.

	std::string getProperty(const std::string& name) const;
		/// Returns the value of the property with the given name.

	static void registerChannel();
		/// Registers the channel with the global LoggingFactory.

	static const std::string PROP_NAME;
	static const std::string PROP_FACILITY;
	static const std::string PROP_FORMAT;
	static const std::string PROP_LOGHOST;
	static const std::string PROP_HOST;
	static const std::string STRUCTURED_DATA;

protected:
	~RemoteSyslogChannel();
	static int getPrio(const Message& msg);

private:
	std::string _logHost;
	std::string _name;
	std::string _host;
	int  _facility;
	bool _bsdFormat;
	DatagramSocket _socket;
	SocketAddress _socketAddress;
	bool _open;
	mutable Poco::FastMutex _mutex;
};


} } // namespace Poco::Net


#endif // Net_RemoteSyslogChannel_INCLUDED
