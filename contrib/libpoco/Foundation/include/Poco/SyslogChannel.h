//
// SyslogChannel.h
//
// $Id: //poco/1.4/Foundation/include/Poco/SyslogChannel.h#1 $
//
// Library: Foundation
// Package: Logging
// Module:  SyslogChannel
//
// Definition of the SyslogChannel class specific to UNIX.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_SyslogChannel_INCLUDED
#define Foundation_SyslogChannel_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Channel.h"


namespace Poco {


class Foundation_API SyslogChannel: public Channel
	/// This Unix-only channel works with the Unix syslog service.
{
public:
	enum Option
	{
		SYSLOG_PID    = 0x01, /// log the pid with each message
		SYSLOG_CONS   = 0x02, /// log on the console if errors in sending
		SYSLOG_NDELAY = 0x08, /// don't delay open
		SYSLOG_PERROR = 0x20  /// log to stderr as well (not supported on all platforms)
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
		SYSLOG_LOCAL0   = (16<<3), /// reserved for local use
		SYSLOG_LOCAL1   = (17<<3), /// reserved for local use
		SYSLOG_LOCAL2   = (18<<3), /// reserved for local use
		SYSLOG_LOCAL3   = (19<<3), /// reserved for local use
		SYSLOG_LOCAL4   = (20<<3), /// reserved for local use
		SYSLOG_LOCAL5   = (21<<3), /// reserved for local use
		SYSLOG_LOCAL6   = (22<<3), /// reserved for local use
		SYSLOG_LOCAL7   = (23<<3)  /// reserved for local use
	};
	
	SyslogChannel();
		/// Creates a SyslogChannel.
		
	SyslogChannel(const std::string& name, int options = SYSLOG_CONS, int facility = SYSLOG_USER);
		/// Creates a SyslogChannel with the given name, options and facility.
	
	void open();
		/// Opens the SyslogChannel.
		
	void close();
		/// Closes the SyslogChannel.
		
	void log(const Message& msg);
		/// Sens the message's text to the syslog service.
		
	void setProperty(const std::string& name, const std::string& value);
		/// Sets the property with the given value.
		///
		/// The following properties are supported:
		///     * name:     The name used to identify the source of log messages.
		///     * facility: The facility added to each log message. See the Facility enumeration for a list of supported values.
		///     * options:  The logging options. See the Option enumeration for a list of supported values.
		
	std::string getProperty(const std::string& name) const;
		/// Returns the value of the property with the given name.

	static const std::string PROP_NAME;
	static const std::string PROP_FACILITY;
	static const std::string PROP_OPTIONS;

protected:
	~SyslogChannel();
	static int getPrio(const Message& msg);

private:
	std::string _name;
	int  _options;
	int  _facility;
	bool _open;
};


} // namespace Poco


#endif // Foundation_SyslogChannel_INCLUDED
