//
// Pipe.h
//
// $Id: //poco/1.4/Foundation/include/Poco/Pipe.h#1 $
//
// Library: Foundation
// Package: Processes
// Module:  Pipe
//
// Definition of the Pipe class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_Pipe_INCLUDED
#define Foundation_Pipe_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/PipeImpl.h"


namespace Poco {


class Foundation_API Pipe
	/// This class implements an anonymous pipe.
	///
	/// Pipes are a common method of inter-process communication -
	/// on Unix, pipes are the oldest form of IPC.
	///
	/// A pipe is a half-duplex communication channel, which means
	/// that data only flows in one direction.
	/// Pipes have a read-end and a write-end. One process writes to
	/// the pipe and another process reads the data written by
	/// its peer. 
	/// Read and write operations are always synchronous. A read will
	/// block until data is available and a write will block until
	/// the reader reads the data.
	///
	/// The sendBytes() and readBytes() methods of Pipe are usually
	/// used through a PipeOutputStream or PipeInputStream and are
	/// not called directly.
	///
	/// Pipe objects have value semantics; the actual work is delegated
	/// to a reference-counted PipeImpl object.
{
public:
	typedef PipeImpl::Handle Handle; /// The read/write handle or file descriptor.
	
	enum CloseMode /// used by close()
	{
		CLOSE_READ  = 0x01, /// Close reading end of pipe.
		CLOSE_WRITE = 0x02, /// Close writing end of pipe.
		CLOSE_BOTH  = 0x03  /// Close both ends of pipe.
	};
	
	Pipe();
		/// Creates the Pipe.
		///
		/// Throws a CreateFileException if the pipe cannot be
		/// created.
		
	Pipe(const Pipe& pipe);
		/// Creates the Pipe using the PipeImpl from another one.

	~Pipe();
		/// Closes and destroys the Pipe.

	Pipe& operator = (const Pipe& pipe);
		/// Releases the Pipe's PipeImpl and assigns another one.

	int writeBytes(const void* buffer, int length);
		/// Sends the contents of the given buffer through
		/// the pipe. Blocks until the receiver is ready
		/// to read the data.
		///
		/// Returns the number of bytes sent.
		///
		/// Throws a WriteFileException if the data cannot be written.

	int readBytes(void* buffer, int length);
		/// Receives data from the pipe and stores it
		/// in buffer. Up to length bytes are received.
		/// Blocks until data becomes available.
		///
		/// Returns the number of bytes received, or 0
		/// if the pipe has been closed.
		///
		/// Throws a ReadFileException if nothing can be read.

	Handle readHandle() const;
		/// Returns the read handle or file descriptor
		/// for the Pipe. For internal use only.
		
	Handle writeHandle() const;
		/// Returns the write handle or file descriptor
		/// for the Pipe. For internal use only.

	void close(CloseMode mode = CLOSE_BOTH);
		/// Depending on the argument, closes either the
		/// reading end, the writing end, or both ends
		/// of the Pipe.
		
private:
	PipeImpl* _pImpl;
};


//
// inlines
//
inline int Pipe::writeBytes(const void* buffer, int length)
{
	return _pImpl->writeBytes(buffer, length);
}


inline int Pipe::readBytes(void* buffer, int length)
{
	return _pImpl->readBytes(buffer, length);
}


inline Pipe::Handle Pipe::readHandle() const
{
	return _pImpl->readHandle();
}


inline Pipe::Handle Pipe::writeHandle() const
{
	return _pImpl->writeHandle();
}


} // namespace Poco


#endif // Foundation_Pipe_INCLUDED
