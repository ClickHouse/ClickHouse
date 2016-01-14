//
// OptionSet.h
//
// $Id: //poco/1.4/Util/include/Poco/Util/OptionSet.h#1 $
//
// Library: Util
// Package: Options
// Module:  OptionSet
//
// Definition of the OptionSet class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Util_OptionSet_INCLUDED
#define Util_OptionSet_INCLUDED


#include "Poco/Util/Util.h"
#include "Poco/Util/Option.h"
#include <vector>


namespace Poco {
namespace Util {


class Util_API OptionSet
	/// A collection of Option objects.
{
public:
	typedef std::vector<Option> OptionVec;
	typedef OptionVec::const_iterator Iterator;

	OptionSet();
		/// Creates the OptionSet.

	OptionSet(const OptionSet& options);
		/// Creates an option set from another one.

	~OptionSet();
		/// Destroys the OptionSet.

	OptionSet& operator = (const OptionSet& options);
		/// Assignment operator.

	void addOption(const Option& option);
		/// Adds an option to the collection.
	
	bool hasOption(const std::string& name, bool matchShort = false) const;
		/// Returns a true iff an option with the given name exists.
		///
		/// The given name can either be a fully specified short name,
		/// or a partially specified full name. If a partial name
		/// matches more than one full name, false is returned.
		/// The name must either match the short or full name of an
		/// option. Comparison case sensitive for the short name and
		/// not case sensitive for the full name.
		
	const Option& getOption(const std::string& name, bool matchShort = false) const;
		/// Returns a reference to the option with the given name.
		///
		/// The given name can either be a fully specified short name,
		/// or a partially specified full name.
		/// The name must either match the short or full name of an
		/// option. Comparison case sensitive for the short name and
		/// not case sensitive for the full name.
		/// Throws a NotFoundException if no matching option has been found.
		/// Throws an UnknownOptionException if a partial full name matches
		/// more than one option.

	Iterator begin() const;
		/// Supports iterating over all options.
		
	Iterator end() const;
		/// Supports iterating over all options.
	
private:	
	OptionVec _options;
};


} } // namespace Poco::Util


#endif // Util_OptionSet_INCLUDED
