//
// DOMObject.h
//
// $Id: //poco/1.4/XML/include/Poco/DOM/DOMObject.h#1 $
//
// Library: XML
// Package: DOM
// Module:  DOM
//
// Definition of the DOMObject class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef DOM_DOMObject_INCLUDED
#define DOM_DOMObject_INCLUDED


#include "Poco/XML/XML.h"


namespace Poco {
namespace XML {


class XML_API DOMObject
	/// The base class for all objects in the Document Object Model.
	///
	/// DOMObject defines the rules for memory management
	/// in this implementation of the DOM. Violation of these
	/// rules, which are outlined in the following, results
	/// in memory leaks or dangling pointers.
	///
	/// Every object created by new or by a factory
	/// method (for example, Document::create*) must be released
	/// with a call to release() or autoRelease() when it
	/// is no longer needed.
	///
	/// Every object created by cloning or importing another
	/// object must be released.
	/// For every call to duplicate() there must be a matching
	/// call to release().
	/// An object obtained via any other way must not be
	/// released, except ownership of it has been explicitely
	/// taken with a call to duplicate().
	///
	/// While DOMObjects are safe for use in multithreaded programs,
	/// a DOMObject or one of its subclasses must not be accessed
	/// from multiple threads simultaneously.
{
public:
	DOMObject();
		/// Creates the DOMObject.
		/// The object's reference count is initialized to one.

	void duplicate() const;
		/// Increases the object's reference count.

	void release() const;
		/// Decreases the object's reference count.
		/// If the reference count reaches zero,
		/// the object is deleted.
		
	virtual void autoRelease() = 0;
		/// Adds the object to an appropriate
		/// AutoReleasePool, which is usually the
		/// AutoReleasePool managed by the Document
		/// to which this object belongs.

protected:
	virtual ~DOMObject();
		/// Destroys the DOMObject.

private:
	DOMObject(const DOMObject&);
	DOMObject& operator = (const DOMObject&);
	
	mutable int _rc;
};


//
// inlines
//
inline void DOMObject::duplicate() const
{
	++_rc;
}


inline void DOMObject::release() const
{
	if (--_rc == 0)
		delete this;
}


} } // namespace Poco::XML


#endif // DOM_DOMObject_INCLUDED
