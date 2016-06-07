//
// ConsoleChannel.cpp
//
// $Id: //poco/1.4/Foundation/src/ConsoleChannel.cpp#2 $
//
// Library: Foundation
// Package: Logging
// Module:  ConsoleChannel
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/ConsoleChannel.h"
#include "Poco/Message.h"
#include "Poco/String.h"
#include "Poco/Exception.h"
#include <iostream>


namespace Poco {


FastMutex ConsoleChannel::_mutex;


ConsoleChannel::ConsoleChannel(): _str(std::clog)
{
}


ConsoleChannel::ConsoleChannel(std::ostream& str): _str(str)
{
}


ConsoleChannel::~ConsoleChannel()
{
}


void ConsoleChannel::log(const Message& msg)
{
	FastMutex::ScopedLock lock(_mutex);
	
	_str << msg.getText() << std::endl;
}


FastMutex ColorConsoleChannel::_mutex;
const std::string ColorConsoleChannel::CSI("\033[");


ColorConsoleChannel::ColorConsoleChannel(): 
	_str(std::clog),
	_enableColors(true)
{
	initColors();
}


ColorConsoleChannel::ColorConsoleChannel(std::ostream& str): 
	_str(str),
	_enableColors(true)
{
	initColors();
}


ColorConsoleChannel::~ColorConsoleChannel()
{
}


void ColorConsoleChannel::log(const Message& msg)
{
	FastMutex::ScopedLock lock(_mutex);
	
	if (_enableColors)
	{
		int color = _colors[msg.getPriority()];
		if (color & 0x100)
		{
			_str << CSI << "1m";
		}
		color &= 0xff;
		_str << CSI << color << "m";
	}
	
	_str << msg.getText();
	
	if (_enableColors)
	{
		_str << CSI << "0m";
	}
	
	_str << std::endl;
}


void ColorConsoleChannel::setProperty(const std::string& name, const std::string& value)
{
	if (name == "enableColors")
	{
		_enableColors = icompare(value, "true") == 0;
	}
	else if (name == "traceColor")
	{
		_colors[Message::PRIO_TRACE] = parseColor(value);
	}
	else if (name == "debugColor")
	{
		_colors[Message::PRIO_DEBUG] = parseColor(value);
	}
	else if (name == "informationColor")
	{
		_colors[Message::PRIO_INFORMATION] = parseColor(value);
	}
	else if (name == "noticeColor")
	{
		_colors[Message::PRIO_NOTICE] = parseColor(value);
	}
	else if (name == "warningColor")
	{
		_colors[Message::PRIO_WARNING] = parseColor(value);
	}
	else if (name == "errorColor")
	{
		_colors[Message::PRIO_ERROR] = parseColor(value);
	}
	else if (name == "criticalColor")
	{
		_colors[Message::PRIO_CRITICAL] = parseColor(value);
	}
	else if (name == "fatalColor")
	{
		_colors[Message::PRIO_FATAL] = parseColor(value);
	}
	else
	{
		Channel::setProperty(name, value);
	}
}


std::string ColorConsoleChannel::getProperty(const std::string& name) const
{
	if (name == "enableColors")
	{
		return _enableColors ? "true" : "false";
	}
	else if (name == "traceColor")
	{
		return formatColor(_colors[Message::PRIO_TRACE]);
	}
	else if (name == "debugColor")
	{
		return formatColor(_colors[Message::PRIO_DEBUG]);
	}
	else if (name == "informationColor")
	{
		return formatColor(_colors[Message::PRIO_INFORMATION]);
	}
	else if (name == "noticeColor")
	{
		return formatColor(_colors[Message::PRIO_NOTICE]);
	}
	else if (name == "warningColor")
	{
		return formatColor(_colors[Message::PRIO_WARNING]);
	}
	else if (name == "errorColor")
	{
		return formatColor(_colors[Message::PRIO_ERROR]);
	}
	else if (name == "criticalColor")
	{
		return formatColor(_colors[Message::PRIO_CRITICAL]);
	}
	else if (name == "fatalColor")
	{
		return formatColor(_colors[Message::PRIO_FATAL]);
	}
	else
	{
		return Channel::getProperty(name);
	}
}


ColorConsoleChannel::Color ColorConsoleChannel::parseColor(const std::string& color) const
{
	if (icompare(color, "default") == 0)
		return CC_DEFAULT;
	else if (icompare(color, "black") == 0)
		return CC_BLACK;
	else if (icompare(color, "red") == 0)
		return CC_RED;
	else if (icompare(color, "green") == 0)
		return CC_GREEN;
	else if (icompare(color, "brown") == 0)
		return CC_BROWN;
	else if (icompare(color, "blue") == 0)
		return CC_BLUE;
	else if (icompare(color, "magenta") == 0)
		return CC_MAGENTA;
	else if (icompare(color, "cyan") == 0)
		return CC_CYAN;
	else if (icompare(color, "gray") == 0)
		return CC_GRAY;
	else if (icompare(color, "darkGray") == 0)
		return CC_DARKGRAY;
	else if (icompare(color, "lightRed") == 0)
		return CC_LIGHTRED;
	else if (icompare(color, "lightGreen") == 0)
		return CC_LIGHTGREEN;
	else if (icompare(color, "yellow") == 0)
		return CC_YELLOW;
	else if (icompare(color, "lightBlue") == 0)
		return CC_LIGHTBLUE;
	else if (icompare(color, "lightMagenta") == 0)
		return CC_LIGHTMAGENTA;
	else if (icompare(color, "lightCyan") == 0)
		return CC_LIGHTCYAN;
	else if (icompare(color, "white") == 0)
		return CC_WHITE;
	else throw InvalidArgumentException("Invalid color value", color);
}


std::string ColorConsoleChannel::formatColor(Color color) const
{
	switch (color)
	{
	case CC_DEFAULT:      return "default";
	case CC_BLACK:        return "black";
	case CC_RED:          return "red";
	case CC_GREEN:        return "green";
	case CC_BROWN:        return "brown";
	case CC_BLUE:         return "blue";
	case CC_MAGENTA:      return "magenta";
	case CC_CYAN:         return "cyan";
	case CC_GRAY:         return "gray";
	case CC_DARKGRAY:     return "darkGray";
	case CC_LIGHTRED:     return "lightRed";
	case CC_LIGHTGREEN:   return "lightGreen";
	case CC_YELLOW:       return "yellow";
	case CC_LIGHTBLUE:    return "lightBlue";
	case CC_LIGHTMAGENTA: return "lightMagenta";
	case CC_LIGHTCYAN:    return "lightCyan";
	case CC_WHITE:        return "white";
	default:              return "invalid";
	}
}


void ColorConsoleChannel::initColors()
{
	_colors[0] = CC_DEFAULT; // unused
	_colors[Message::PRIO_FATAL]       = CC_LIGHTRED;
	_colors[Message::PRIO_CRITICAL]    = CC_LIGHTRED;
	_colors[Message::PRIO_ERROR]       = CC_LIGHTRED;
	_colors[Message::PRIO_WARNING]     = CC_YELLOW;
	_colors[Message::PRIO_NOTICE]      = CC_DEFAULT;
	_colors[Message::PRIO_INFORMATION] = CC_DEFAULT;
	_colors[Message::PRIO_DEBUG]       = CC_GRAY;
	_colors[Message::PRIO_TRACE]       = CC_GRAY;
}


} // namespace Poco
