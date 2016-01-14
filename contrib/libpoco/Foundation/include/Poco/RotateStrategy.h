//
// RotateStrategy.h
//
// $Id: //poco/1.4/Foundation/include/Poco/RotateStrategy.h#1 $
//
// Library: Foundation
// Package: Logging
// Module:  FileChannel
//
// Definition of the RotateStrategy class and subclasses.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_RotateStrategy_INCLUDED
#define Foundation_RotateStrategy_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Timespan.h"
#include "Poco/Timestamp.h"
#include "Poco/Exception.h"
#include "Poco/LogFile.h"
#include "Poco/StringTokenizer.h"
#include "Poco/DateTimeParser.h"
#include "Poco/NumberParser.h"


namespace Poco {


class Foundation_API RotateStrategy
	/// The RotateStrategy is used by LogFile to determine when
	/// a file must be rotated.
{
public:
	RotateStrategy();
	virtual ~RotateStrategy();

	virtual bool mustRotate(LogFile* pFile) = 0;
		/// Returns true if the given log file must
		/// be rotated, false otherwise.
		
private:
	RotateStrategy(const RotateStrategy&);
	RotateStrategy& operator = (const RotateStrategy&);
};


template <class DT>
class RotateAtTimeStrategy: public RotateStrategy
	/// The file is rotated at specified [day,][hour]:minute
{
public:
	RotateAtTimeStrategy(const std::string& rtime):
		_day(-1), 
		_hour(-1), 
		_minute(0)
	{
		if (rtime.empty()) 
			throw InvalidArgumentException("Rotation time must be specified.");

		if ((rtime.find(',') != rtime.npos) && (rtime.find(':') == rtime.npos)) 
			throw InvalidArgumentException("Invalid rotation time specified.");

		StringTokenizer timestr(rtime, ",:", StringTokenizer::TOK_TRIM | StringTokenizer::TOK_IGNORE_EMPTY);
		int index = 0;

		switch (timestr.count())
		{
		case 3: // day,hh:mm
			{
				std::string::const_iterator it = timestr[index].begin();
				_day = DateTimeParser::parseDayOfWeek(it, timestr[index].end());
				++index;
			}
		case 2: // hh:mm
			_hour = NumberParser::parse(timestr[index]);
			++index;
		case 1: // mm
			_minute = NumberParser::parse(timestr[index]);
			break;
		default:
			throw InvalidArgumentException("Invalid rotation time specified.");
		}
		getNextRollover();
	}
	
	~RotateAtTimeStrategy()
	{
	}
	
	bool mustRotate(LogFile* /*pFile*/)
	{
		if (DT() >= _threshold)
		{
			getNextRollover();
			return true;
		}
		return false;
	}

private:
	void getNextRollover()
	{
		Timespan tsp(0, 0, 1, 0, 1000); // 0,00:01:00.001
		do
		{
			_threshold += tsp;
		}
		while (!(_threshold.minute() == _minute &&
		        (-1 == _hour || _threshold.hour() == _hour) && 
		        (-1 == _day  || _threshold.dayOfWeek() == _day)));
		// round to :00.0 seconds
		_threshold.assign(_threshold.year(), _threshold.month(), _threshold.day(), _threshold.hour(), _threshold.minute());
	}

	DT  _threshold;
	int _day;
	int _hour;
	int _minute;
};


class Foundation_API RotateByIntervalStrategy: public RotateStrategy
	/// The file is rotated when the log file 
	/// exceeds a given age.
	///
	/// For this to work reliably across all platforms and file systems
	/// (there are severe issues on most platforms finding out the real
	/// creation date of a file), the creation date of the file is
	/// written into the log file as the first entry.
{
public:
	RotateByIntervalStrategy(const Timespan& span);
	~RotateByIntervalStrategy();
	bool mustRotate(LogFile* pFile);

private:
	Timespan _span;
	Timestamp _lastRotate;
	static const std::string ROTATE_TEXT;
};


class Foundation_API RotateBySizeStrategy: public RotateStrategy
	/// The file is rotated when the log file
	/// exceeds a given size.
{
public:
	RotateBySizeStrategy(UInt64 size);
	~RotateBySizeStrategy();
	bool mustRotate(LogFile* pFile);

private:
	UInt64 _size;
};


} // namespace Poco


#endif // Foundation_RotateStrategy_INCLUDED
