//
// SharedMemoryImpl.h
//
// Library: Foundation
// Package: Processes
// Module:  SharedMemoryImpl
//
// Definition of the SharedMemoryImpl class.
//
// Copyright (c) 2007, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_SharedMemoryImpl_INCLUDED
#define Foundation_SharedMemoryImpl_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/SharedMemory.h"
#include "Poco/RefCountedObject.h"


namespace Poco {


class Foundation_API SharedMemoryImpl: public RefCountedObject
	/// A dummy implementation of shared memory, for systems
	/// that do not have shared memory support.
{
public:
	SharedMemoryImpl(const std::string& id, std::size_t size, SharedMemory::AccessMode mode, const void* addr, bool server);
		/// Creates or connects to a shared memory object with the given name.
		///
		/// For maximum portability, name should be a valid Unix filename and not
		/// contain any slashes or backslashes.
		///
		/// An address hint can be passed to the system, specifying the desired
		/// start address of the shared memory area. Whether the hint
		/// is actually honored is, however, up to the system. Windows platform
		/// will generally ignore the hint.

	SharedMemoryImpl(const Poco::File& aFile, SharedMemory::AccessMode mode, const void* addr);
		/// Maps the entire contents of file into a shared memory segment.
		///
		/// An address hint can be passed to the system, specifying the desired
		/// start address of the shared memory area. Whether the hint
		/// is actually honored is, however, up to the system. Windows platform
		/// will generally ignore the hint.

	char* begin() const;
		/// Returns the start address of the shared memory segment.

	char* end() const;
		/// Returns the one-past-end end address of the shared memory segment. 

protected:
	~SharedMemoryImpl();
		/// Destroys the SharedMemoryImpl.

private:
	SharedMemoryImpl();
	SharedMemoryImpl(const SharedMemoryImpl&);
	SharedMemoryImpl& operator = (const SharedMemoryImpl&);
};


//
// inlines
//
inline char* SharedMemoryImpl::begin() const
{
	return 0;
}


inline char* SharedMemoryImpl::end() const
{
	return 0;
}


} // namespace Poco


#endif // Foundation_SharedMemoryImpl_INCLUDED
