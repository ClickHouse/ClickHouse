//
// Formatter.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Formatter.h#1 $
//
// Library: Foundation
// Package: Logging
// Module:  Formatter
//
// Definition of the Formatter class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Formatter_INCLUDED
#define Foundation_Formatter_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/Configurable.h"
#include "Poco/RefCountedObject.h"


namespace Poco {


class Message;


class Foundation_API Formatter: public Configurable, public RefCountedObject
	/// The base class for all Formatter classes.
	///
	/// A formatter basically takes a Message object
	/// and formats it into a string. How the formatting
	/// is exactly done is up to the implementation of
	/// Formatter. For example, a very simple implementation
	/// might simply take the message's Text (see Message::getText()).
	/// A useful implementation should at least take the Message's
	/// Time, Priority and Text fields and put them into a string.
	///
	/// The Formatter class supports the Configurable
	/// interface, so the behaviour of certain formatters
	/// is configurable.
	/// 
	/// Trivial implementations of of getProperty() and 
	/// setProperty() are provided.
	///
	/// Subclasses must at least provide a format() method.
{
public:
	Formatter();
		/// Creates the formatter.
		
	virtual ~Formatter();
		/// Destroys the formatter.

	virtual void format(const Message& msg, std::string& text) = 0;
		/// Formats the message and places the result in text. 
		/// Subclasses must override this method.
		
	void setProperty(const std::string& name, const std::string& value);
		/// Throws a PropertyNotSupportedException.

	std::string getProperty(const std::string& name) const;
		/// Throws a PropertyNotSupportedException.
};


} // namespace Poco


#endif // Foundation_Formatter_INCLUDED
