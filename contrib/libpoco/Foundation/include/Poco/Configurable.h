//
// Configurable.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Configurable.h#1 $
//
// Library: Foundation
// Package: Logging
// Module:  Configurable
//
// Definition of the Configurable class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Configurable_INCLUDED
#define Foundation_Configurable_INCLUDED


#include "Poco/Foundation.h"


namespace Poco {


class Foundation_API Configurable
	/// A simple interface that defines
	/// getProperty() and setProperty() methods.
	///
	/// This interface is implemented by Formatter and
	/// Channel and is used to configure arbitrary
	/// channels and formatters.
	///
	/// A property is basically a name-value pair. For
	/// simplicity, both names and values are strings.
	/// Every property controls a certain aspect of a
	/// Formatter or Channel. For example, the PatternFormatter's
	/// formatting pattern is set via a property.
	///
	/// NOTE: The following property names are use internally
	/// by the logging framework and must not be used by
	/// channels or formatters:
	///   - class
	///   - pattern (Channel)
	///   - formatter (Channel)
{
public:
	Configurable();
		/// Creates the Configurable.
		
	virtual ~Configurable();
		/// Destroys the Configurable.
		
	virtual void setProperty(const std::string& name, const std::string& value) = 0;
		/// Sets the property with the given name to the given value.
		/// If a property with the given name is not supported, a
		/// PropertyNotSupportedException is thrown.
		
	virtual std::string getProperty(const std::string& name) const = 0;
		/// Returns the value of the property with the given name.
		/// If a property with the given name is not supported, a
		/// PropertyNotSupportedException is thrown.
};


} // namespace Poco


#endif // Foundation_Configurable_INCLUDED
