//
// HelpFormatter.cpp
//
// $Id: //poco/1.4/Util/src/HelpFormatter.cpp#2 $
//
// Library: Util
// Package: Options
// Module:  HelpFormatter
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Util/HelpFormatter.h"
#include "Poco/Util/OptionSet.h"
#include "Poco/Util/Option.h"


namespace Poco {
namespace Util {


const int HelpFormatter::TAB_WIDTH  = 4;
const int HelpFormatter::LINE_WIDTH = 78;


HelpFormatter::HelpFormatter(const OptionSet& options): 
	_options(options),
	_width(LINE_WIDTH),
	_indent(0),
	_unixStyle(true)
{
#if !defined(POCO_OS_FAMILY_UNIX)
	_unixStyle = false;
#endif
	_indent = calcIndent();
}


HelpFormatter::~HelpFormatter()
{
}


void HelpFormatter::setCommand(const std::string& command)
{
	_command = command;
}


void HelpFormatter::setUsage(const std::string& usage)
{
	_usage = usage;
}


void HelpFormatter::setHeader(const std::string& header)
{
	_header = header;
}


void HelpFormatter::setFooter(const std::string& footer)
{
	_footer = footer;
}


void HelpFormatter::format(std::ostream& ostr) const
{
	ostr << "usage: " << _command;
	if (!_usage.empty())
	{
		ostr << ' ';
		formatText(ostr, _usage, (int) _command.length() + 1);
	}
	ostr << '\n';
	if (!_header.empty())
	{
		formatText(ostr, _header, 0);
		ostr << "\n\n";
	}
	formatOptions(ostr);
	if (!_footer.empty())
	{
		ostr << '\n';
		formatText(ostr, _footer, 0);
		ostr << '\n';
	}
}


void HelpFormatter::setWidth(int width)
{
	poco_assert (width > 0);
	
	_width = width;
}


void HelpFormatter::setIndent(int indent)
{
	poco_assert (indent >= 0 && indent < _width);
	
	_indent = indent;
}


void HelpFormatter::setAutoIndent()
{
	_indent = calcIndent();
}


void HelpFormatter::setUnixStyle(bool flag)
{
	_unixStyle = flag;
}


int HelpFormatter::calcIndent() const
{
	int indent = 0;
	for (OptionSet::Iterator it = _options.begin(); it != _options.end(); ++it)
	{
		int shortLen = (int) it->shortName().length();
		int fullLen  = (int) it->fullName().length();
		int n = 0;
		if (_unixStyle && shortLen > 0)
		{
			n += shortLen + (int) shortPrefix().length();
			if (it->takesArgument())
				n += (int) it->argumentName().length() + (it->argumentRequired() ? 0 : 2);
			if (fullLen > 0) n += 2;
		}
		if (fullLen > 0)
		{
			n += fullLen + (int) longPrefix().length();
			if (it->takesArgument())
				n += 1 + (int) it->argumentName().length() + (it->argumentRequired() ? 0 : 2);
		}
		n += 2;
		if (n > indent)
			indent = n;
	}
	return indent;
}


void HelpFormatter::formatOptions(std::ostream& ostr) const
{
	int optWidth = calcIndent();
	for (OptionSet::Iterator it = _options.begin(); it != _options.end(); ++it)
	{
		formatOption(ostr, *it, optWidth);
		if (_indent < optWidth)
		{
			ostr << '\n' << std::string(_indent, ' ');
			formatText(ostr, it->description(), _indent, _indent);
		}
		else
		{
			formatText(ostr, it->description(), _indent, optWidth);
		}
		ostr << '\n';
	}
}


void HelpFormatter::formatOption(std::ostream& ostr, const Option& option, int width) const
{
	int shortLen = (int) option.shortName().length();
	int fullLen  = (int) option.fullName().length();

	int n = 0;
	if (_unixStyle && shortLen > 0)
	{
		ostr << shortPrefix() << option.shortName();
		n += (int) shortPrefix().length() + (int) option.shortName().length();
		if (option.takesArgument())
		{
			if (!option.argumentRequired()) { ostr << '['; ++n; }
			ostr << option.argumentName();
			n += (int) option.argumentName().length();
			if (!option.argumentRequired()) { ostr << ']'; ++n; }
		}
		if (fullLen > 0) { ostr << ", "; n += 2; }
	}
	if (fullLen > 0)
	{
		ostr << longPrefix() << option.fullName();
		n += (int) longPrefix().length() + (int) option.fullName().length();
		if (option.takesArgument())
		{
			if (!option.argumentRequired()) { ostr << '['; ++n; }
			ostr << '=';
			++n;
			ostr << option.argumentName();
			n += (int) option.argumentName().length();
			if (!option.argumentRequired()) { ostr << ']'; ++n; }
		}
	}
	while (n < width) { ostr << ' '; ++n; }
}


void HelpFormatter::formatText(std::ostream& ostr, const std::string& text, int indent) const
{
	formatText(ostr, text, indent, indent);
}


void HelpFormatter::formatText(std::ostream& ostr, const std::string& text, int indent, int firstIndent) const
{
	int pos = firstIndent;
	int maxWordLen = _width - indent;
	std::string word;
	for (std::string::const_iterator it = text.begin(); it != text.end(); ++it)
	{
		if (*it == '\n')
		{
			clearWord(ostr, pos, word, indent);
			ostr << '\n';
			pos = 0;
			while (pos < indent) { ostr << ' '; ++pos; }
		}
		else if (*it == '\t')
		{
			clearWord(ostr, pos, word, indent);
			if (pos < _width) ++pos;
			while (pos < _width && pos % TAB_WIDTH != 0)
			{
				ostr << ' ';
				++pos;
			}
		}
		else if (*it == ' ')
		{
			clearWord(ostr, pos, word, indent);
			if (pos < _width) { ostr << ' '; ++pos; }
		}
		else 
		{
			if (word.length() == maxWordLen)
			{
				clearWord(ostr, pos, word, indent);
			}
			else word += *it;
		}
	}
	clearWord(ostr, pos, word, indent);
}


void HelpFormatter::formatWord(std::ostream& ostr, int& pos, const std::string& word, int indent) const
{
	if (pos + word.length() > _width)
	{
		ostr << '\n';
		pos = 0;
		while (pos < indent) { ostr << ' '; ++pos; }
	}
	ostr << word;
	pos += (int) word.length();
}


void HelpFormatter::clearWord(std::ostream& ostr, int& pos, std::string& word, int indent) const
{
	formatWord(ostr, pos, word, indent);
	word.clear();
}


std::string HelpFormatter::shortPrefix() const
{
#if defined(POCO_OS_FAMILY_UNIX)
	return "-";
#else
	return _unixStyle ? "-" : "/";
#endif
}


std::string HelpFormatter::longPrefix() const
{
#if defined(POCO_OS_FAMILY_UNIX)
	return "--";
#else
	return _unixStyle ? "--" : "/";
#endif
}


} } // namespace Poco::Util
