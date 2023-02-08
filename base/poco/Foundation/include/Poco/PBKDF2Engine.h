//
// PBKDF2Engine.h
//
// Library: Foundation
// Package: Crypt
// Module:  PBKDF2Engine
//
// Definition of the PBKDF2Engine class.
//
// Copyright (c) 2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_PBKDF2Engine_INCLUDED
#define Foundation_PBKDF2Engine_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/DigestEngine.h"
#include "Poco/ByteOrder.h"
#include <algorithm>


namespace Poco {


template <class PRF>
class PBKDF2Engine: public DigestEngine
	/// This class implementes the Password-Based Key Derivation Function 2,
	/// as specified in RFC 2898. The underlying DigestEngine (HMACEngine, etc.),
	/// which must accept the passphrase as constructor argument (std::string), 
	/// must be given as template argument. 
	///
	/// PBKDF2 (Password-Based Key Derivation Function 2) is a key derivation function 
	/// that is part of RSA Laboratories' Public-Key Cryptography Standards (PKCS) series, 
	/// specifically PKCS #5 v2.0, also published as Internet Engineering Task Force's 
	/// RFC 2898. It replaces an earlier standard, PBKDF1, which could only produce 
	/// derived keys up to 160 bits long.
	///
	/// PBKDF2 applies a pseudorandom function, such as a cryptographic hash, cipher, or 
	/// HMAC to the input password or passphrase along with a salt value and repeats the 
	/// process many times to produce a derived key, which can then be used as a 
	/// cryptographic key in subsequent operations. The added computational work makes 
	/// password cracking much more difficult, and is known as key stretching. 
	/// When the standard was written in 2000, the recommended minimum number of 
	/// iterations was 1000, but the parameter is intended to be increased over time as 
	/// CPU speeds increase. Having a salt added to the password reduces the ability to 
	/// use precomputed hashes (rainbow tables) for attacks, and means that multiple 
	/// passwords have to be tested individually, not all at once. The standard 
	/// recommends a salt length of at least 64 bits. [Wikipedia]
	///
	/// The PBKDF2 algorithm is implemented as a DigestEngine. The passphrase is specified
	/// by calling update().
	///
	/// Example (WPA2):
	///     PBKDF2Engine<HMACEngine<SHA1Engine> > pbkdf2(ssid, 4096, 256);
	///     pbkdf2.update(passphrase);
	///     DigestEngine::Digest d = pbkdf2.digest();
{
public:
	enum
	{
		PRF_DIGEST_SIZE = PRF::DIGEST_SIZE
	};
	
	PBKDF2Engine(const std::string& salt, unsigned c = 4096, Poco::UInt32 dkLen = PRF_DIGEST_SIZE):
		_s(salt),
		_c(c),
		_dkLen(dkLen)
	{		
		_result.reserve(_dkLen + PRF_DIGEST_SIZE);
	}
	
	~PBKDF2Engine()
	{
	}
		
	std::size_t digestLength() const
	{
		return _dkLen;
	}
	
	void reset()
	{
		_p.clear();
		_result.clear();
	}
	
	const DigestEngine::Digest& digest()
	{
		Poco::UInt32 i = 1;
		while (_result.size() < _dkLen)
		{
			f(i++);
		}
		_result.resize(_dkLen);
		return _result;
	}

protected:
	void updateImpl(const void* data, std::size_t length)
	{
		_p.append(reinterpret_cast<const char*>(data), length);
	}
	
	void f(Poco::UInt32 i)
	{
		PRF prf(_p);
		prf.update(_s);
		Poco::UInt32 iBE = Poco::ByteOrder::toBigEndian(i);
		prf.update(&iBE, sizeof(iBE));
		Poco::DigestEngine::Digest up = prf.digest();
		Poco::DigestEngine::Digest ux = up;
		poco_assert_dbg(ux.size() == PRF_DIGEST_SIZE);
		for (unsigned k = 1; k < _c; k++)
		{
			prf.reset();
			prf.update(&up[0], up.size());
			Poco::DigestEngine::Digest u = prf.digest();
			poco_assert_dbg(u.size() == PRF_DIGEST_SIZE);
			for (int ui = 0; ui < PRF_DIGEST_SIZE; ui++)
			{
				ux[ui] ^= u[ui];
			}
			std::swap(up, u);
		}
		_result.insert(_result.end(), ux.begin(), ux.end());
	}

private:
	PBKDF2Engine();
	PBKDF2Engine(const PBKDF2Engine&);
	PBKDF2Engine& operator = (const PBKDF2Engine&);

	std::string _p;
	std::string _s;
	unsigned _c;
	Poco::UInt32 _dkLen;
	DigestEngine::Digest _result;
};


} // namespace Poco


#endif // Foundation_PBKDF2Engine_INCLUDED
