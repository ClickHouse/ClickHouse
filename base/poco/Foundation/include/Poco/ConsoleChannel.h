//
// ConsoleChannel.h
//
// Library: Foundation
// Package: Logging
// Module:  ConsoleChannel
//
// Definition of the ConsoleChannel class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_ConsoleChannel_INCLUDED
#define Foundation_ConsoleChannel_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Channel.h"
#include "Poco/Mutex.h"
#include <ostream>


namespace Poco {


class Foundation_API ConsoleChannel: public Channel
	/// A channel that writes to an ostream.
	///
	/// Only the message's text is written, followed
	/// by a newline.
	///
	/// Chain this channel to a FormattingChannel with an
	/// appropriate Formatter to control what is contained 
	/// in the text.
	///
	/// Similar to StreamChannel, except that a static
	/// mutex is used to protect against multiple
	/// console channels concurrently writing to the
	/// same stream.
{
public:
	ConsoleChannel();
		/// Creates the channel and attaches std::clog.
		
	ConsoleChannel(std::ostream& str);
		/// Creates the channel using the given stream.

	void log(const Message& msg);
		/// Logs the given message to the channel's stream.
		
protected:
	~ConsoleChannel();

private:
	std::ostream& _str;
	static FastMutex _mutex;
};


class Foundation_API ColorConsoleChannel: public Channel
	/// A channel that writes to an ostream.
	///
	/// Only the message's text is written, followed
	/// by a newline.
	///
	/// Messages can be colored depending on priority.
	/// The console device must support ANSI escape codes
	/// in order to display colored messages.
	///
	/// To enable message coloring, set the "enableColors"
	/// property to true (default). Furthermore, colors can be
	/// configured by setting the following properties
	/// (default values are given in parenthesis):
	/// 
	///   * traceColor (gray)
	///   * debugColor (gray)
	///   * informationColor (default)
	///   * noticeColor (default)
	///   * warningColor (yellow)
	///   * errorColor (lightRed)
	///   * criticalColor (lightRed)
	///   * fatalColor (lightRed)
	///
	/// The following color values are supported:
	/// 
	///   * default
	///   * black
	///   * red
	///   * green
	///   * brown
	///   * blue
	///   * magenta
	///   * cyan
	///   * gray
	///   * darkgray
	///   * lightRed
	///   * lightGreen
	///   * yellow
	///   * lightBlue
	///   * lightMagenta
	///   * lightCyan
	///   * white
	///
	/// Chain this channel to a FormattingChannel with an
	/// appropriate Formatter to control what is contained 
	/// in the text.
	///
	/// Similar to StreamChannel, except that a static
	/// mutex is used to protect against multiple
	/// console channels concurrently writing to the
	/// same stream.
{
public:	
	ColorConsoleChannel();
		/// Creates the channel and attaches std::clog.
		
	ColorConsoleChannel(std::ostream& str);
		/// Creates the channel using the given stream.

	void log(const Message& msg);
		/// Logs the given message to the channel's stream.
	
	void setProperty(const std::string& name, const std::string& value);
		/// Sets the property with the given name. 
		/// 
		/// The following properties are supported:
		///   * enableColors:      Enable or disable colors.
		///   * traceColor:        Specify color for trace messages.
		///   * debugColor:        Specify color for debug messages.
		///   * informationColor:  Specify color for information messages.
		///   * noticeColor:       Specify color for notice messages.
		///   * warningColor:      Specify color for warning messages.
		///   * errorColor:        Specify color for error messages.
		///   * criticalColor:     Specify color for critical messages.
		///   * fatalColor:        Specify color for fatal messages.
		///
		/// See the class documentation for a list of supported color values.

	std::string getProperty(const std::string& name) const;
		/// Returns the value of the property with the given name.
		/// See setProperty() for a description of the supported
		/// properties.

protected:	
	enum Color
	{
		CC_DEFAULT      = 0x0027,
		CC_BLACK        = 0x001e,
		CC_RED          = 0x001f,
		CC_GREEN        = 0x0020,
		CC_BROWN        = 0x0021,
		CC_BLUE         = 0x0022,
		CC_MAGENTA      = 0x0023,
		CC_CYAN         = 0x0024,
		CC_GRAY         = 0x0025,
		CC_DARKGRAY     = 0x011e,
		CC_LIGHTRED     = 0x011f,
		CC_LIGHTGREEN   = 0x0120,
		CC_YELLOW       = 0x0121,
		CC_LIGHTBLUE    = 0x0122,
		CC_LIGHTMAGENTA = 0x0123,
		CC_LIGHTCYAN    = 0x0124,
		CC_WHITE        = 0x0125
	};

	~ColorConsoleChannel();
	Color parseColor(const std::string& color) const;
	std::string formatColor(Color color) const;
	void initColors();

private:
	std::ostream& _str;
	bool _enableColors;
	Color _colors[9];
	static FastMutex _mutex;
	static const std::string CSI;
};


} // namespace Poco


#endif // Foundation_ConsoleChannel_INCLUDED
