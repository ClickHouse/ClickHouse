//
// WindowsConsoleChannel.cpp
//
// $Id: //poco/1.4/Foundation/src/WindowsConsoleChannel.cpp#2 $
//
// Library: Foundation
// Package: Logging
// Module:  WindowsConsoleChannel
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/WindowsConsoleChannel.h"
#include "Poco/Message.h"
#if defined(POCO_WIN32_UTF8)
#include "Poco/UnicodeConverter.h"
#endif
#include "Poco/String.h"
#include "Poco/Exception.h"


namespace Poco {


WindowsConsoleChannel::WindowsConsoleChannel():
	_isFile(false),
	_hConsole(INVALID_HANDLE_VALUE)
{
	_hConsole = GetStdHandle(STD_OUTPUT_HANDLE);
	// check whether the console has been redirected
	DWORD mode;	
	_isFile = (GetConsoleMode(_hConsole, &mode) == 0);
}


WindowsConsoleChannel::~WindowsConsoleChannel()
{
}


void WindowsConsoleChannel::log(const Message& msg)
{
	std::string text = msg.getText();
	text += "\r\n";
	
#if defined(POCO_WIN32_UTF8)
	if (_isFile)
	{
		DWORD written;
		WriteFile(_hConsole, text.data(), static_cast<DWORD>(text.size()), &written, NULL);	
	}
	else
	{
		std::wstring utext;
		UnicodeConverter::toUTF16(text, utext);
		DWORD written;
		WriteConsoleW(_hConsole, utext.data(), static_cast<DWORD>(utext.size()), &written, NULL);
	}
#else
	DWORD written;
	WriteFile(_hConsole, text.data(), text.size(), &written, NULL);	
#endif
}


WindowsColorConsoleChannel::WindowsColorConsoleChannel():
	_enableColors(true),
	_isFile(false),
	_hConsole(INVALID_HANDLE_VALUE)
{
	_hConsole = GetStdHandle(STD_OUTPUT_HANDLE);
	// check whether the console has been redirected
	DWORD mode;	
	_isFile = (GetConsoleMode(_hConsole, &mode) == 0);
	initColors();
}


WindowsColorConsoleChannel::~WindowsColorConsoleChannel()
{
}


void WindowsColorConsoleChannel::log(const Message& msg)
{
	std::string text = msg.getText();
	text += "\r\n";

	if (_enableColors && !_isFile)
	{
		WORD attr = _colors[0];
		attr &= 0xFFF0;
		attr |= _colors[msg.getPriority()];
		SetConsoleTextAttribute(_hConsole, attr);
	}

#if defined(POCO_WIN32_UTF8)
	if (_isFile)
	{
		DWORD written;
		WriteFile(_hConsole, text.data(), static_cast<DWORD>(text.size()), &written, NULL);	
	}
	else
	{
		std::wstring utext;
		UnicodeConverter::toUTF16(text, utext);
		DWORD written;
		WriteConsoleW(_hConsole, utext.data(), static_cast<DWORD>(utext.size()), &written, NULL);
	}
#else
	DWORD written;
	WriteFile(_hConsole, text.data(), text.size(), &written, NULL);	
#endif

	if (_enableColors && !_isFile)
	{
		SetConsoleTextAttribute(_hConsole, _colors[0]);
	}
}


void WindowsColorConsoleChannel::setProperty(const std::string& name, const std::string& value)
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


std::string WindowsColorConsoleChannel::getProperty(const std::string& name) const
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


WORD WindowsColorConsoleChannel::parseColor(const std::string& color) const
{
	if (icompare(color, "default") == 0)
		return _colors[0];
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


std::string WindowsColorConsoleChannel::formatColor(WORD color) const
{
	switch (color)
	{
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


void WindowsColorConsoleChannel::initColors()
{
	if (!_isFile)
	{
		CONSOLE_SCREEN_BUFFER_INFO csbi;
		GetConsoleScreenBufferInfo(_hConsole, &csbi);
		_colors[0] = csbi.wAttributes;
	}
	else
	{
		_colors[0] = CC_WHITE;
	}
	_colors[Message::PRIO_FATAL]       = CC_LIGHTRED;
	_colors[Message::PRIO_CRITICAL]    = CC_LIGHTRED;
	_colors[Message::PRIO_ERROR]       = CC_LIGHTRED;
	_colors[Message::PRIO_WARNING]     = CC_YELLOW;
	_colors[Message::PRIO_NOTICE]      = _colors[0];
	_colors[Message::PRIO_INFORMATION] = _colors[0];
	_colors[Message::PRIO_DEBUG]       = CC_GRAY;
	_colors[Message::PRIO_TRACE]       = CC_GRAY;
}


} // namespace Poco
