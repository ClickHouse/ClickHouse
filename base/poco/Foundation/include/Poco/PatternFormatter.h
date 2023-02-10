//
// PatternFormatter.h
//
// Library: Foundation
// Package: Logging
// Module:  PatternFormatter
//
// Definition of the PatternFormatter class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_PatternFormatter_INCLUDED
#define Foundation_PatternFormatter_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Formatter.h"
#include "Poco/Message.h"

#include <vector>

namespace Poco {


class Foundation_API PatternFormatter: public Formatter
	/// This Formatter allows for custom formatting of
	/// log messages based on format patterns.
	///
	/// The format pattern is used as a template to format the message and
	/// is copied character by character except for the following special characters,
	/// which are replaced by the corresponding value.
	///
	///   * %s - message source
	///   * %t - message text
	///   * %l - message priority level (1 .. 7)
	///   * %p - message priority (Fatal, Critical, Error, Warning, Notice, Information, Debug, Trace)
	///   * %q - abbreviated message priority (F, C, E, W, N, I, D, T)
	///   * %P - message process identifier
	///   * %T - message thread name
	///   * %I - message thread identifier (numeric)
	///   * %N - node or host name
	///   * %U - message source file path (empty string if not set)
	///   * %u - message source line number (0 if not set)
	///   * %w - message date/time abbreviated weekday (Mon, Tue, ...)
	///   * %W - message date/time full weekday (Monday, Tuesday, ...)
	///   * %b - message date/time abbreviated month (Jan, Feb, ...)
	///   * %B - message date/time full month (January, February, ...)
	///   * %d - message date/time zero-padded day of month (01 .. 31)
	///   * %e - message date/time day of month (1 .. 31)
	///   * %f - message date/time space-padded day of month ( 1 .. 31)
	///   * %m - message date/time zero-padded month (01 .. 12)
	///   * %n - message date/time month (1 .. 12)
	///   * %o - message date/time space-padded month ( 1 .. 12)
	///   * %y - message date/time year without century (70)
	///   * %Y - message date/time year with century (1970)
	///   * %H - message date/time hour (00 .. 23)
	///   * %h - message date/time hour (00 .. 12)
	///   * %a - message date/time am/pm
	///   * %A - message date/time AM/PM
	///   * %M - message date/time minute (00 .. 59)
	///   * %S - message date/time second (00 .. 59)
	///   * %i - message date/time millisecond (000 .. 999)
	///   * %c - message date/time centisecond (0 .. 9)
	///   * %F - message date/time fractional seconds/microseconds (000000 - 999999)
	///   * %z - time zone differential in ISO 8601 format (Z or +NN.NN)
	///   * %Z - time zone differential in RFC format (GMT or +NNNN)
	///   * %L - convert time to local time (must be specified before any date/time specifier; does not itself output anything)
	///   * %E - epoch time (UTC, seconds since midnight, January 1, 1970)
	///   * %v[width] - the message source (%s) but text length is padded/cropped to 'width'
	///   * %[name] - the value of the message parameter with the given name
	///   * %% - percent sign

{
public:
	PatternFormatter();
		/// Creates a PatternFormatter.
		/// The format pattern must be specified with
		/// a call to setProperty.

	PatternFormatter(const std::string& format);
		/// Creates a PatternFormatter that uses the
		/// given format pattern.

	~PatternFormatter();
		/// Destroys the PatternFormatter.

	void format(const Message& msg, std::string& text);
		/// Formats the message according to the specified
		/// format pattern and places the result in text. 
		
	void setProperty(const std::string& name, const std::string& value);
		/// Sets the property with the given name to the given value.
		///
		/// The following properties are supported:
		/// 
		///     * pattern: The format pattern. See the PatternFormatter class
		///       for details.
		///     * times: Specifies whether times are adjusted for local time
		///       or taken as they are in UTC. Supported values are "local" and "UTC".
		///
		/// If any other property name is given, a PropertyNotSupported
		/// exception is thrown.

	std::string getProperty(const std::string& name) const;
		/// Returns the value of the property with the given name or
		/// throws a PropertyNotSupported exception if the given
		/// name is not recognized.

	static const std::string PROP_PATTERN;
	static const std::string PROP_TIMES;

protected:
	static const std::string& getPriorityName(int);
		/// Returns a string for the given priority value.
	
private:
	struct PatternAction
	{
		PatternAction(): key(0), length(0) 
		{
		}

		char key;
		int length;
		std::string property;
		std::string prepend;
	};

	void parsePattern();
		/// Will parse the _pattern string into the vector of PatternActions,
		/// which contains the message key, any text that needs to be written first
		/// a proprety in case of %[] and required length.

	std::vector<PatternAction> _patternActions;
	bool _localTime;
	std::string _pattern;
};


} // namespace Poco


#endif // Foundation_PatternFormatter_INCLUDED
