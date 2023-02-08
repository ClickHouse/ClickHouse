//
// HelpFormatter.h
//
// Library: Util
// Package: Options
// Module:  HelpFormatter
//
// Definition of the HelpFormatter class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_HelpFormatter_INCLUDED
#define Util_HelpFormatter_INCLUDED


#include "Poco/Util/Util.h"
#include <ostream>


namespace Poco {
namespace Util {


class OptionSet;
class Option;


class Util_API HelpFormatter
	/// This class formats a help message from an OptionSet.
{
public:
	HelpFormatter(const OptionSet& options);
		/// Creates the HelpFormatter, using the given
		/// options. 
		///
		/// The HelpFormatter just stores a reference
		/// to the given OptionSet, so the OptionSet must not
		/// be destroyed during the lifetime of the HelpFormatter.

	~HelpFormatter();
		/// Destroys the HelpFormatter.

	void setCommand(const std::string& command);
		/// Sets the command name.
		
	const std::string& getCommand() const;
		/// Returns the command name.
		
	void setUsage(const std::string& usage);
		/// Sets the usage string.
		
	const std::string& getUsage() const;
		/// Returns the usage string.
		
	void setHeader(const std::string& header);
		/// Sets the header string.
		
	const std::string& getHeader() const;
		/// Returns the header string.
		
	void setFooter(const std::string& footer);
		/// Sets the footer string.
		
	const std::string& getFooter() const;
		/// Returns the footer string.

	void format(std::ostream& ostr) const;
		/// Writes the formatted help text to the given stream.
	
	void setWidth(int width);
		/// Sets the line width for the formatted help text.
		
	int getWidth() const;
		/// Returns the line width for the formatted help text.
		///
		/// The default width is 72.

	void setIndent(int indent);
		/// Sets the indentation for description continuation lines.

	int getIndent() const;
		/// Returns the indentation for description continuation lines.
		
	void setAutoIndent();
		/// Sets the indentation for description continuation lines so that
		/// the description text is left-aligned. 

	void setUnixStyle(bool flag);
		/// Enables Unix-style options. Both short and long option names
		/// are printed if Unix-style is set. Otherwise, only long option
		/// names are printed.
		///
		/// After calling setUnixStyle(), setAutoIndent() should be called
		/// as well to ensure proper help text formatting.
		
	bool isUnixStyle() const;
		/// Returns if Unix-style options are set.

	std::string shortPrefix() const;
		/// Returns the platform-specific prefix for short options.
		/// "-" on Unix, "/" on Windows and OpenVMS.
	
	std::string longPrefix() const;
		/// Returns the platform-specific prefix for long options.
		/// "--" on Unix, "/" on Windows and OpenVMS.

protected:
	int calcIndent() const;
		/// Calculates the indentation for the option descriptions
		/// from the given options.

	void formatOptions(std::ostream& ostr) const;
		/// Formats all options.

	void formatOption(std::ostream& ostr, const Option& option, int width) const;
		/// Formats an option, using the platform-specific
		/// prefixes.

	void formatText(std::ostream& ostr, const std::string& text, int indent) const;
		/// Formats the given text.

	void formatText(std::ostream& ostr, const std::string& text, int indent, int firstIndent) const;
		/// Formats the given text.

	void formatWord(std::ostream& ostr, int& pos, const std::string& word, int indent) const;
		/// Formats the given word.
	
	void clearWord(std::ostream& ostr, int& pos, std::string& word, int indent) const;
		/// Formats and then clears the given word.
	
private:
	HelpFormatter(const HelpFormatter&);
	HelpFormatter& operator = (const HelpFormatter&);

	const OptionSet& _options;
	int _width;
	int _indent;
	std::string _command;
	std::string _usage;
	std::string _header;
	std::string _footer;
	bool _unixStyle;
	
	static const int TAB_WIDTH;
	static const int LINE_WIDTH;
};


//
// inlines
//
inline int HelpFormatter::getWidth() const
{
	return _width;
}


inline int HelpFormatter::getIndent() const
{
	return _indent;
}


inline const std::string& HelpFormatter::getCommand() const
{
	return _command;
}


inline const std::string& HelpFormatter::getUsage() const
{
	return _usage;
}


inline const std::string& HelpFormatter::getHeader() const
{
	return _header;
}


inline const std::string& HelpFormatter::getFooter() const
{
	return _footer;
}


inline bool HelpFormatter::isUnixStyle() const
{
	return _unixStyle;
}


} } // namespace Poco::Util


#endif // Util_HelpFormatter_INCLUDED
