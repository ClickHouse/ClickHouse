//
// Bugcheck.cpp
//
// Library: Foundation
// Package: Core
// Module:  Bugcheck
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Bugcheck.h"
#include "Poco/Debugger.h"
#include "Poco/Exception.h"
#include <sstream>


namespace Poco {


void Bugcheck::assertion(const char* cond, const char* file, int line, const char* text)
{
	std::string message("Assertion violation: ");
	message += cond;
	if (text)
	{
		message += " (";
		message += text;
		message += ")";
	}
	Debugger::enter(message, file, line);
	throw AssertionViolationException(what(cond, file, line, text));
}


void Bugcheck::nullPointer(const char* ptr, const char* file, int line)
{
	Debugger::enter(std::string("NULL pointer: ") + ptr, file, line);
	throw NullPointerException(what(ptr, file, line));
}


void Bugcheck::bugcheck(const char* file, int line)
{
	Debugger::enter("Bugcheck", file, line);
	throw BugcheckException(what(0, file, line));
}


void Bugcheck::bugcheck(const char* msg, const char* file, int line)
{
	std::string m("Bugcheck");
	if (msg)
	{
		m.append(": ");
		m.append(msg);
	}
	Debugger::enter(m, file, line);
	throw BugcheckException(what(msg, file, line));
}


void Bugcheck::unexpected(const char* file, int line)
{
#ifdef _DEBUG
	try
	{
		std::string msg("Unexpected exception in noexcept function or destructor: ");
		try
		{
			throw;
		}
		catch (Poco::Exception& exc)
		{
			msg += exc.displayText();
		}
		catch (std::exception& exc)
		{
			msg += exc.what();
		}
		catch (...)
		{
			msg += "unknown exception";
		}
		Debugger::enter(msg, file, line);
	}
	catch (...)
	{
	}
#endif	
}


void Bugcheck::debugger(const char* file, int line)
{
	Debugger::enter(file, line);
}


void Bugcheck::debugger(const char* msg, const char* file, int line)
{
	Debugger::enter(msg, file, line);
}


std::string Bugcheck::what(const char* msg, const char* file, int line, const char* text)
{
	std::ostringstream str;
	if (msg) str << msg << " ";
   if (text != NULL) str << "(" << text << ") ";
	str << "in file \"" << file << "\", line " << line;
	return str.str();
}


} // namespace Poco
