//
// Void.h
//
// Library: Foundation
// Package: Core
// Module:  Void
//
// Definition of the Void class.
//
// Copyright (c) 2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Void_INCLUDED
#define Foundation_Void_INCLUDED


#include "Poco/Foundation.h"


namespace Poco {


class Foundation_API Void
	/// A dummy class with value-type semantics, 
	/// mostly useful as a template argument.
	///
	/// This class is typically used together with ActiveMethod,
	/// if no argument or return value is needed.
{
public:
	Void();
		/// Creates the Void.

	Void(const Void& v);
		/// Creates the Void from another Void.
		///
		/// The philosophical aspects of this operation 
		/// remain undiscussed for now.
		
	~Void();
		/// Destroys the Void.

	Void& operator = (const Void& v);
		/// Assigns another void.

	bool operator ==(const Void& v) const;
		/// Will return always true due to Voids having no members.

	bool operator !=(const Void& v) const;
		/// Will return always false due to Voids having no members.
};


inline bool Void::operator ==(const Void& /*v*/) const
{
	return true;
}


inline bool Void::operator !=(const Void& /*v*/) const
{
	return false;
}


} // namespace Poco


#endif // Foundation_Void_INCLUDED
