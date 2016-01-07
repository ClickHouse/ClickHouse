//
// Instantiator.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Instantiator.h#1 $
//
// Library: Foundation
// Package: Core
// Module:  Instantiator
//
// Definition of the Instantiator class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Instantiator_INCLUDED
#define Foundation_Instantiator_INCLUDED


#include "Poco/Foundation.h"


namespace Poco {


template <class Base>
class AbstractInstantiator
	/// The common base class for all Instantiator instantiations.
	/// Used by DynamicFactory.
{
public:
	AbstractInstantiator()
		/// Creates the AbstractInstantiator.
	{
	}
	
	virtual ~AbstractInstantiator()
		/// Destroys the AbstractInstantiator.
	{
	}
	
	virtual Base* createInstance() const = 0;
		/// Creates an instance of a concrete subclass of Base.	

private:
	AbstractInstantiator(const AbstractInstantiator&);
	AbstractInstantiator& operator = (const AbstractInstantiator&);
};


template <class C, class Base>
class Instantiator: public AbstractInstantiator<Base>
	/// A template class for the easy instantiation of 
	/// instantiators. 
	///
	/// For the Instantiator to work, the class of which
	/// instances are to be instantiated must have a no-argument
	/// constructor.
{
public:
	Instantiator()
		/// Creates the Instantiator.
	{
	}
	
	virtual ~Instantiator()
		/// Destroys the Instantiator.
	{
	}

	Base* createInstance() const
	{
		return new C;
	}
};


} // namespace Poco


#endif // Foundation_Instantiator_INCLUDED
