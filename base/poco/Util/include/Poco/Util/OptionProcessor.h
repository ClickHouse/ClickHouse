//
// OptionProcessor.h
//
// Library: Util
// Package: Options
// Module:  OptionProcessor
//
// Definition of the OptionProcessor class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_OptionProcessor_INCLUDED
#define Util_OptionProcessor_INCLUDED


#include "Poco/Util/Util.h"
#include <set>


namespace Poco {
namespace Util {


class OptionSet;


class Util_API OptionProcessor
	/// An OptionProcessor is used to process the command line
	/// arguments of an application.
	///
	/// The process() method takes an argument from the command line.
	/// If that argument starts with an option prefix, the argument
	/// is further processed. Otherwise, the argument is ignored and
	/// false is returned. The argument must match one of the options
	/// given in the OptionSet that is passed to the OptionProcessor
	/// with the constructor. If an option is part of a group, at most
	/// one option of the group can be passed to the OptionProcessor.
	/// Otherwise an IncompatibleOptionsException is thrown.
	/// If the same option is given multiple times, but the option
	/// is not repeatable, a DuplicateOptionException is thrown.
	/// If the option is not recognized, a UnexpectedArgumentException
	/// is thrown.
	/// If the option requires an argument, but none is given, an
	/// MissingArgumentException is thrown.
	/// If no argument is expected, but one is present, a
	/// UnexpectedArgumentException is thrown.
	/// If a partial option name is ambiguous, an AmbiguousOptionException
	/// is thrown.
	///
	/// The OptionProcessor supports two modes: Unix mode and default mode.
	/// In Unix mode, the option prefix is a dash '-'. A dash must be followed
	/// by a short option name, or another dash, followed by a (partial)
	/// long option name.
	/// In default mode, the option prefix is a slash '/', followed by 
	/// a (partial) long option name.
	/// If the special option '--' is encountered in Unix mode, all following
	/// options are ignored.
	///
	/// Option arguments can be specified in three ways. If a Unix short option
	/// ("-o") is given, the argument directly follows the option name, without
	/// any delimiting character or space ("-ovalue"). In default option mode, or if a
	/// Unix long option ("--option") is given, the option argument is 
	/// delimited from the option name with either an equal sign ('=') or
	/// a colon (':'), as in "--option=value" or "/option:value". Finally,
	/// a required option argument can be specified on the command line after the
	/// option, delimited with a space, as in "--option value" or "-o value".
	/// The latter only works for required option arguments, not optional ones.
{
public:
	OptionProcessor(const OptionSet& options);
		/// Creates the OptionProcessor, using the given OptionSet.

	~OptionProcessor();
		/// Destroys the OptionProcessor.

	void setUnixStyle(bool flag);
		/// Enables (flag == true) or disables (flag == false) Unix-style
		/// option processing.
		///
		/// If Unix-style processing is enabled, options are expected to
		/// begin with a single or a double dash ('-' or '--', respectively).
		/// A single dash must be followed by a short option name. A double
		/// dash must be followed by a (partial) full option name.
		///
		/// If Unix-style processing is disabled, options are expected to
		/// begin with a slash ('/'), followed by a (partial) full option name.

	bool isUnixStyle() const;
		/// Returns true iff Unix-style option processing is enabled.

	bool process(const std::string& argument, std::string& optionName, std::string& optionArg);
		/// Examines and processes the given command line argument.
		///
		/// If the argument begins with an option prefix, the option is processed
		/// and true is returned. The full option name is stored in optionName and the 
		/// option argument, if present, is stored in optionArg.
		///
		/// If the option does not begin with an option prefix, false is returned.

	void checkRequired() const;
		/// Checks if all required options have been processed.
		///
		/// Does nothing if all required options have been processed.
		/// Throws a MissingOptionException otherwise.

private:
	bool processUnix(const std::string& argument, std::string& optionName, std::string& optionArg);
	bool processDefault(const std::string& argument, std::string& optionName, std::string& optionArg);
	bool processCommon(const std::string& option, bool isShort, std::string& optionName, std::string& optionArg);
	
	const OptionSet& _options;
	bool _unixStyle;
	bool _ignore;
	std::set<std::string> _groups;
	std::set<std::string> _specifiedOptions;
	std::string _deferredOption;
};


//
// inlines
//
inline bool OptionProcessor::isUnixStyle() const
{
	return _unixStyle;
}


} } // namespace Poco::Util


#endif // Util_OptionProcessor_INCLUDED
