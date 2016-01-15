//
// WindowsConsoleChannel.h
//
// $Id: //poco/1.4/Foundation/include/Poco/WindowsConsoleChannel.h#2 $
//
// Library: Foundation
// Package: Logging
// Module:  WindowsConsoleChannel
//
// Definition of the WindowsConsoleChannel class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_WindowsConsoleChannel_INCLUDED
#define Foundation_WindowsConsoleChannel_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Channel.h"
#include "Poco/Mutex.h"
#include "Poco/UnWindows.h"


namespace Poco {


class Foundation_API WindowsConsoleChannel: public Channel
	/// A channel that writes to the Windows console.
	///
	/// Only the message's text is written, followed
	/// by a newline.
	///
	/// If POCO has been compiled with POCO_WIN32_UTF8,
	/// log messages are assumed to be UTF-8 encoded, and
	/// are converted to UTF-16 prior to writing them to the
	/// console. This is the main difference to the ConsoleChannel
	/// class, which cannot handle UTF-8 encoded messages on Windows.
	///
	/// Chain this channel to a FormattingChannel with an
	/// appropriate Formatter to control what is contained 
	/// in the text.
	///
	/// Only available on Windows platforms.
{
public:
	WindowsConsoleChannel();
		/// Creates the WindowsConsoleChannel.

	void log(const Message& msg);
		/// Logs the given message to the channel's stream.
		
protected:
	~WindowsConsoleChannel();

private:
	HANDLE _hConsole;
	bool   _isFile;
};


class Foundation_API WindowsColorConsoleChannel: public Channel
	/// A channel that writes to the Windows console.
	///
	/// Only the message's text is written, followed
	/// by a newline.
	///
	/// If POCO has been compiled with POCO_WIN32_UTF8,
	/// log messages are assumed to be UTF-8 encoded, and
	/// are converted to UTF-16 prior to writing them to the
	/// console. This is the main difference to the ConsoleChannel
	/// class, which cannot handle UTF-8 encoded messages on Windows.
	///
	/// Messages can be colored depending on priority.
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
	/// Only available on Windows platforms.
{
public:
	WindowsColorConsoleChannel();
		/// Creates the WindowsConsoleChannel.

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
		CC_BLACK        = 0x0000,
		CC_RED          = 0x0004,
		CC_GREEN        = 0x0002,
		CC_BROWN        = 0x0006,
		CC_BLUE         = 0x0001,
		CC_MAGENTA      = 0x0005,
		CC_CYAN         = 0x0003,
		CC_GRAY         = 0x0007,
		CC_DARKGRAY     = 0x0008,
		CC_LIGHTRED     = 0x000C,
		CC_LIGHTGREEN   = 0x000A,
		CC_YELLOW       = 0x000E,
		CC_LIGHTBLUE    = 0x0009,
		CC_LIGHTMAGENTA = 0x000D,
		CC_LIGHTCYAN    = 0x000B,
		CC_WHITE        = 0x000F
	};

	~WindowsColorConsoleChannel();
	WORD parseColor(const std::string& color) const;
	std::string formatColor(WORD color) const;
	void initColors();

private:
	bool _enableColors;
	HANDLE _hConsole;
	bool   _isFile;
	WORD   _colors[9];
};


} // namespace Poco


#endif // Foundation_WindowsConsoleChannel_INCLUDED
