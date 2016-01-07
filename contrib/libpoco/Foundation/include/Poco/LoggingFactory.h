//
// LoggingFactory.h
//
// $Id: //poco/1.4/Foundation/include/Poco/LoggingFactory.h#1 $
//
// Library: Foundation
// Package: Logging
// Module:  LoggingFactory
//
// Definition of the LoggingFactory class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_LoggingFactory_INCLUDED
#define Foundation_LoggingFactory_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/DynamicFactory.h"
#include "Poco/Channel.h"
#include "Poco/Formatter.h"


namespace Poco {


class Foundation_API LoggingFactory
	/// An extensible factory for channels and formatters.
	///
	/// The following channel classes are pre-registered:
	///   - AsyncChannel
	///   - ConsoleChannel
	///   - EventLogChannel (Windows platforms only)
	///   - FileChannel
	///   - FormattingChannel
	///   - NullChannel
	///   - OpcomChannel (OpenVMS only)
	///   - SplitterChannel
	///   - SyslogChannel (Unix platforms only)
	///
	/// The following formatter classes are pre-registered:
	///   - PatternFormatter
{
public:
	typedef AbstractInstantiator<Channel>   ChannelInstantiator;
	typedef AbstractInstantiator<Formatter> FormatterFactory;

	LoggingFactory();
		/// Creates the LoggingFactory.
		///
		/// Automatically registers class factories for the
		/// built-in channel and formatter classes.

	~LoggingFactory();
		/// Destroys the LoggingFactory.
		
	void registerChannelClass(const std::string& className, ChannelInstantiator* pFactory);
		/// Registers a channel class with the LoggingFactory.
		
	void registerFormatterClass(const std::string& className, FormatterFactory* pFactory);
		/// Registers a formatter class with the LoggingFactory.

	Channel* createChannel(const std::string& className) const;
		/// Creates a new Channel instance from specified class.
		///
		/// Throws a NotFoundException if the specified channel class 
		/// has not been registered.
		
	Formatter* createFormatter(const std::string& className) const;
		/// Creates a new Formatter instance from specified class.
		///
		/// Throws a NotFoundException if the specified formatter class 
		/// has not been registered.

	static LoggingFactory& defaultFactory();
		/// Returns a reference to the default
		/// LoggingFactory.

private:
	void registerBuiltins();
	
	DynamicFactory<Channel>   _channelFactory;
	DynamicFactory<Formatter> _formatterFactory;
};


} // namespace Poco


#endif // Foundation_LoggingFactory_INCLUDED
