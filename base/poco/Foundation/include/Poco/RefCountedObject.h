//
// RefCountedObject.h
//
// Library: Foundation
// Package: Core
// Module:  RefCountedObject
//
// Definition of the RefCountedObject class.
//
// Copyright (c) 2004-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_RefCountedObject_INCLUDED
#define Foundation_RefCountedObject_INCLUDED


#include "Poco/AtomicCounter.h"
#include "Poco/Foundation.h"

#include <atomic>


namespace Poco
{


class Foundation_API RefCountedObject
/// A base class for objects that employ
/// reference counting based garbage collection.
///
/// Reference-counted objects inhibit construction
/// by copying and assignment.
{
public:
    RefCountedObject();
    /// Creates the RefCountedObject.
    /// The initial reference count is one.

    size_t duplicate() const;
    /// Increments the object's reference count, returns reference count before call.

    size_t release() const throw();
    /// Decrements the object's reference count
    /// and deletes the object if the count
    /// reaches zero, returns reference count before call.

    size_t referenceCount() const;
    /// Returns the reference count.

protected:
    virtual ~RefCountedObject();
    /// Destroys the RefCountedObject.

private:
    RefCountedObject(const RefCountedObject &);
    RefCountedObject & operator=(const RefCountedObject &);

    mutable std::atomic<size_t> _counter;
};


//
// inlines
//
inline size_t RefCountedObject::referenceCount() const
{
    return _counter.load(std::memory_order_acquire);
}


inline size_t RefCountedObject::duplicate() const
{
    return _counter.fetch_add(1, std::memory_order_acq_rel);
}


inline size_t RefCountedObject::release() const throw()
{
    size_t reference_count_before = _counter.fetch_sub(1, std::memory_order_acq_rel);

    try
    {
        if (reference_count_before == 1)
            delete this;
    }
    catch (...)
    {
        poco_unexpected();
    }

    return reference_count_before;
}


} // namespace Poco


#endif // Foundation_RefCountedObject_INCLUDED
