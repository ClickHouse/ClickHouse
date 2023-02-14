//
// DigestEngine.h
//
// Library: Foundation
// Package: Crypt
// Module:  DigestEngine
//
// Definition of class DigestEngine.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_DigestEngine_INCLUDED
#define Foundation_DigestEngine_INCLUDED


#include "Poco/Foundation.h"
#include <vector>


namespace Poco {


class Foundation_API DigestEngine
	/// This class is an abstract base class
	/// for all classes implementing a message
	/// digest algorithm, like MD5Engine
	/// and SHA1Engine.
	/// Call update() repeatedly with data to
	/// compute the digest from. When done,
	/// call digest() to obtain the message
	/// digest.
{
public:
	typedef std::vector<unsigned char> Digest;

	DigestEngine();
	virtual ~DigestEngine();

	void update(const void* data, std::size_t length);
	void update(char data);
	void update(const std::string& data);
		/// Updates the digest with the given data.

	virtual std::size_t digestLength() const = 0;
		/// Returns the length of the digest in bytes.

	virtual void reset() = 0;
		/// Resets the engine so that a new
		/// digest can be computed.

	virtual const Digest& digest() = 0;
		/// Finishes the computation of the digest and
		/// returns the message digest. Resets the engine
		/// and can thus only be called once for every digest.
		/// The returned reference is valid until the next
		/// time digest() is called, or the engine object is destroyed.

	static std::string digestToHex(const Digest& bytes);
		/// Converts a message digest into a string of hexadecimal numbers.

	static Digest digestFromHex(const std::string& digest);
		/// Converts a string created by digestToHex back to its Digest presentation

	static bool constantTimeEquals(const Digest& d1, const Digest& d2);
		/// Compares two Digest values using a constant-time comparison
		/// algorithm. This can be used to prevent timing attacks
		/// (as discussed in <https://codahale.com/a-lesson-in-timing-attacks/>).

protected:
	virtual void updateImpl(const void* data, std::size_t length) = 0;
		/// Updates the digest with the given data. Must be implemented
		/// by subclasses.

private:
	DigestEngine(const DigestEngine&);
	DigestEngine& operator = (const DigestEngine&);
};


//
// inlines
//


inline void DigestEngine::update(const void* data, std::size_t length)
{
	updateImpl(data, length);
}


inline void DigestEngine::update(char data)
{
	updateImpl(&data, 1);
}


inline void DigestEngine::update(const std::string& data)
{
	updateImpl(data.data(), data.size());
}


} // namespace Poco


#endif // Foundation_DigestEngine_INCLUDED
