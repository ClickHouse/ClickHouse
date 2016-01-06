//
// PatternFormatter.cpp
//
// $Id: //poco/1.4/Foundation/src/PatternFormatter.cpp#2 $
//
// Library: Foundation
// Package: Logging
// Module:  PatternFormatter
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/PatternFormatter.h"
#include "Poco/Message.h"
#include "Poco/NumberFormatter.h"
#include "Poco/DateTimeFormat.h"
#include "Poco/DateTimeFormatter.h"
#include "Poco/DateTime.h"
#include "Poco/Timestamp.h"
#include "Poco/Timezone.h"
#include "Poco/Environment.h"
#include "Poco/NumberParser.h"


namespace Poco {


const std::string PatternFormatter::PROP_PATTERN = "pattern";
const std::string PatternFormatter::PROP_TIMES   = "times";


PatternFormatter::PatternFormatter():
	_localTime(false)
{
}


PatternFormatter::PatternFormatter(const std::string& format):
	_localTime(false),
	_pattern(format)
{
	parsePattern();
}


PatternFormatter::~PatternFormatter()
{
}


void PatternFormatter::format(const Message& msg, std::string& text)
{
	Timestamp timestamp = msg.getTime();
	bool localTime = _localTime;
	if (localTime)
	{
		timestamp += Timezone::utcOffset()*Timestamp::resolution();
		timestamp += Timezone::dst()*Timestamp::resolution();
	}
	DateTime dateTime = timestamp;
	for (std::vector<PatternAction>::iterator ip = _patternActions.begin(); ip != _patternActions.end(); ++ip)
	{
		text.append(ip->prepend);
		switch (ip->key)
		{
		case 's': text.append(msg.getSource()); break;
		case 't': text.append(msg.getText()); break;
		case 'l': NumberFormatter::append(text, (int) msg.getPriority()); break;
		case 'p': text.append(getPriorityName((int) msg.getPriority())); break;
		case 'q': text += getPriorityName((int) msg.getPriority()).at(0); break;
		case 'P': NumberFormatter::append(text, msg.getPid()); break;
		case 'T': text.append(msg.getThread()); break;
		case 'I': NumberFormatter::append(text, msg.getTid()); break;
		case 'N': text.append(Environment::nodeName()); break;
		case 'U': text.append(msg.getSourceFile() ? msg.getSourceFile() : ""); break;
		case 'u': NumberFormatter::append(text, msg.getSourceLine()); break;
		case 'w': text.append(DateTimeFormat::WEEKDAY_NAMES[dateTime.dayOfWeek()], 0, 3); break;
		case 'W': text.append(DateTimeFormat::WEEKDAY_NAMES[dateTime.dayOfWeek()]); break;
		case 'b': text.append(DateTimeFormat::MONTH_NAMES[dateTime.month() - 1], 0, 3); break;
		case 'B': text.append(DateTimeFormat::MONTH_NAMES[dateTime.month() - 1]); break;
		case 'd': NumberFormatter::append0(text, dateTime.day(), 2); break;
		case 'e': NumberFormatter::append(text, dateTime.day()); break;
		case 'f': NumberFormatter::append(text, dateTime.day(), 2); break;
		case 'm': NumberFormatter::append0(text, dateTime.month(), 2); break;
		case 'n': NumberFormatter::append(text, dateTime.month()); break;
		case 'o': NumberFormatter::append(text, dateTime.month(), 2); break;
		case 'y': NumberFormatter::append0(text, dateTime.year() % 100, 2); break;
		case 'Y': NumberFormatter::append0(text, dateTime.year(), 4); break;
		case 'H': NumberFormatter::append0(text, dateTime.hour(), 2); break;
		case 'h': NumberFormatter::append0(text, dateTime.hourAMPM(), 2); break;
		case 'a': text.append(dateTime.isAM() ? "am" : "pm"); break;
		case 'A': text.append(dateTime.isAM() ? "AM" : "PM"); break;
		case 'M': NumberFormatter::append0(text, dateTime.minute(), 2); break;
		case 'S': NumberFormatter::append0(text, dateTime.second(), 2); break;
		case 'i': NumberFormatter::append0(text, dateTime.millisecond(), 3); break;
		case 'c': NumberFormatter::append(text, dateTime.millisecond()/100); break;
		case 'F': NumberFormatter::append0(text, dateTime.millisecond()*1000 + dateTime.microsecond(), 6); break;
		case 'z': text.append(DateTimeFormatter::tzdISO(localTime ? Timezone::tzd() : DateTimeFormatter::UTC)); break;
		case 'Z': text.append(DateTimeFormatter::tzdRFC(localTime ? Timezone::tzd() : DateTimeFormatter::UTC)); break;
		case 'E': NumberFormatter::append(text, msg.getTime().epochTime()); break;
		case 'v':
			if (ip->length > msg.getSource().length())	//append spaces
				text.append(msg.getSource()).append(ip->length - msg.getSource().length(), ' ');
			else if (ip->length && ip->length < msg.getSource().length()) // crop
				text.append(msg.getSource(), msg.getSource().length()-ip->length, ip->length);
			else
				text.append(msg.getSource());
			break;
		case 'x':
			try
			{
				text.append(msg[ip->property]);
			}
			catch (...)
			{
			}
			break;
		case 'L':
			if (!localTime)
			{
				localTime = true;
				timestamp += Timezone::utcOffset()*Timestamp::resolution();
				timestamp += Timezone::dst()*Timestamp::resolution();
				dateTime = timestamp;
			}
			break;
		}
	}
}


void PatternFormatter::parsePattern()
{
	_patternActions.clear();
	std::string::const_iterator it  = _pattern.begin();
	std::string::const_iterator end = _pattern.end();
	PatternAction endAct;
	while (it != end)
	{
		if (*it == '%')
		{
			if (++it != end)
			{
				PatternAction act;
				act.prepend = endAct.prepend;
				endAct.prepend.clear();

				if (*it == '[')
				{
					act.key = 'x';
					++it;
					std::string prop;
					while (it != end && *it != ']') prop += *it++;
					if (it == end) --it;
					act.property = prop;
				}
				else
				{
					act.key = *it;
					if ((it + 1) != end && *(it + 1) == '[')
					{
						it += 2;
						std::string number;
						while (it != end && *it != ']') number += *it++;
						if (it == end) --it;
						try
						{
							act.length = NumberParser::parse(number);
						}
						catch (...)
						{
						}
					}
				}
				_patternActions.push_back(act);
				++it;
			}
		}
		else
		{
			endAct.prepend += *it++;
		}
	}
	if (endAct.prepend.size())
	{
		_patternActions.push_back(endAct);
	}
}

	
void PatternFormatter::setProperty(const std::string& name, const std::string& value)
{
	if (name == PROP_PATTERN)
	{
		_pattern = value;
		parsePattern();
	}
	else if (name == PROP_TIMES)
	{
		_localTime = (value == "local");
	}
	else 
	{
		Formatter::setProperty(name, value);
	}
}


std::string PatternFormatter::getProperty(const std::string& name) const
{
	if (name == PROP_PATTERN)
		return _pattern;
	else if (name == PROP_TIMES)
		return _localTime ? "local" : "UTC";
	else
		return Formatter::getProperty(name);
}


namespace
{
	static std::string priorities[] = 
	{
		"",
		"Fatal",
		"Critical",
		"Error",
		"Warning",
		"Notice",
		"Information",
		"Debug",
		"Trace"
	};
}


const std::string& PatternFormatter::getPriorityName(int prio)
{
	poco_assert (1 <= prio && prio <= 8);	
	return priorities[prio];
}


} // namespace Poco
