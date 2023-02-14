//
// EventLogChannel.h
//
// Library: Foundation
// Package: Logging
// Module:  EventLogChannel
//
// Definition of the EventLogChannel class specific to WIN32.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_EventLogChannel_INCLUDED
#define Foundation_EventLogChannel_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Channel.h"
#include "Poco/UnWindows.h"


namespace Poco {


class Foundation_API EventLogChannel: public Channel
	/// This Windows-only channel works with the Windows NT Event Log
	/// service.
	///
	/// To work properly, the EventLogChannel class requires that either
	/// the PocoFoundation.dll or the PocoMsg.dll Dynamic Link Library
	/// containing the message definition resources can be found in $PATH.
{
public:
	EventLogChannel();
		/// Creates the EventLogChannel.
		/// The name of the current application (or more correctly,
		/// the name of its executable) is taken as event source name.
		
	EventLogChannel(const std::string& name);
		/// Creates the EventLogChannel with the given event source name.
		
	EventLogChannel(const std::string& name, const std::string& host);
		/// Creates an EventLogChannel with the given event source
		/// name that routes messages to the given host.
		
	void open();
		/// Opens the EventLogChannel. If necessary, the
		/// required registry entries to register a
		/// message resource DLL are made.
		
	void close();
		/// Closes the EventLogChannel.
	
	void log(const Message& msg);
		/// Logs the given message to the Windows Event Log.
		///
		/// The message type and priority are mapped to
		/// appropriate values for Event Log type and category.
		
	void setProperty(const std::string& name, const std::string& value);
		/// Sets or changes a configuration property. 
		///
		/// The following properties are supported:
		///
		///   * name:    The name of the event source.
		///   * loghost: The name of the host where the Event Log service is running.
		///              The default is "localhost".
		///   * host:    same as host.
		///   * logfile: The name of the log file. The default is "Application".
		
	std::string getProperty(const std::string& name) const;
		/// Returns the value of the given property.

	static const std::string PROP_NAME;
	static const std::string PROP_HOST;
	static const std::string PROP_LOGHOST;
	static const std::string PROP_LOGFILE;

protected:
	~EventLogChannel();
	static int getType(const Message& msg);
	static int getCategory(const Message& msg);
	void setUpRegistry() const;
#if defined(POCO_WIN32_UTF8)
	static std::wstring findLibrary(const wchar_t* name);
#else
	static std::string findLibrary(const char* name);
#endif

private:
	std::string _name;
	std::string _host;
	std::string _logFile;
	HANDLE      _h;
};


} // namespace Poco


#endif // Foundation_EventLogChannel_INCLUDED
