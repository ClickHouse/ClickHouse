//
// NestedDiagnosticContext.cpp
//
// Library: Foundation
// Package: Core
// Module:  NestedDiagnosticContext
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/NestedDiagnosticContext.h"
#include "Poco/SingletonHolder.h"
#include "Poco/ThreadLocal.h"


namespace Poco {


NestedDiagnosticContext::NestedDiagnosticContext()
{
}


NestedDiagnosticContext::NestedDiagnosticContext(const NestedDiagnosticContext& ctx)
{
	_stack = ctx._stack;
}


NestedDiagnosticContext::~NestedDiagnosticContext()
{
}


NestedDiagnosticContext& NestedDiagnosticContext::operator = (const NestedDiagnosticContext& ctx)
{
	if (&ctx != this)
		_stack = ctx._stack;
	return *this;
}

	
void NestedDiagnosticContext::push(const std::string& info)
{
	Context ctx;
	ctx.info = info;
	ctx.line = -1;
	ctx.file = 0;
	_stack.push_back(ctx);
}

	
void NestedDiagnosticContext::push(const std::string& info, int line, const char* filename)
{
	Context ctx;
	ctx.info = info;
	ctx.line = line;
	ctx.file = filename;
	_stack.push_back(ctx);
}


void NestedDiagnosticContext::pop()
{
	if (!_stack.empty())
		_stack.pop_back();
}

	
int NestedDiagnosticContext::depth() const
{
	return int(_stack.size());
}


std::string NestedDiagnosticContext::toString() const
{
	std::string result;
	for (Stack::const_iterator it = _stack.begin(); it != _stack.end(); ++it)
	{
		if (!result.empty())
			result.append(":");
		result.append(it->info);
	}
	return result;
}

	
void NestedDiagnosticContext::dump(std::ostream& ostr) const
{
	dump(ostr, "\n");
}


void NestedDiagnosticContext::dump(std::ostream& ostr, const std::string& delimiter) const
{
	for (Stack::const_iterator it = _stack.begin(); it != _stack.end(); ++it)
	{
		ostr << it->info;
		if (it->file)
			ostr << " (in \"" << it->file << "\", line " << it->line << ")";
		ostr << delimiter;
	}
}


void NestedDiagnosticContext::clear()
{
	_stack.clear();
}


namespace
{
	static ThreadLocal<NestedDiagnosticContext> ndc;
}


NestedDiagnosticContext& NestedDiagnosticContext::current()
{
	return ndc.get();
}


} // namespace Poco
