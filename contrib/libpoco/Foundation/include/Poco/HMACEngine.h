//
// HMACEngine.h
//
// $Id: //poco/1.4/Foundation/include/Poco/HMACEngine.h#1 $
//
// Library: Foundation
// Package: Crypt
// Module:  HMACEngine
//
// Definition of the HMACEngine class.
//
// Copyright (c) 2004-2006, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Foundation_HMACEngine_INCLUDED
#define Foundation_HMACEngine_INCLUDED


#include "Poco/Foundation.h"
#include "Poco/DigestEngine.h"
#include <cstring>


namespace Poco {


template <class Engine>
class HMACEngine: public DigestEngine
	/// This class implementes the HMAC message
	/// authentication code algorithm, as specified
	/// in RFC 2104. The underlying DigestEngine
	/// (MD5Engine, SHA1Engine, etc.) must be given as 
	/// template argument.
	/// Since the HMACEngine is a DigestEngine, it can
	/// be used with the DigestStream class to create
	/// a HMAC for a stream.
{
public:
	enum
	{
		BLOCK_SIZE  = Engine::BLOCK_SIZE,
		DIGEST_SIZE = Engine::DIGEST_SIZE
	};
	
	HMACEngine(const std::string& passphrase)
	{
		init(passphrase.data(), passphrase.length());
	}
	
	HMACEngine(const char* passphrase, std::size_t length)
	{
		poco_check_ptr (passphrase);

		init(passphrase, length);
	}
	
	~HMACEngine()
	{
		std::memset(_ipad, 0, BLOCK_SIZE);
		std::memset(_opad, 0, BLOCK_SIZE);
		delete [] _ipad;
		delete [] _opad;
	}
		
	std::size_t digestLength() const
	{
		return DIGEST_SIZE;
	}
	
	void reset()
	{
		_engine.reset();
		_engine.update(_ipad, BLOCK_SIZE);
	}
	
	const DigestEngine::Digest& digest()
	{
		const DigestEngine::Digest& d = _engine.digest();
		char db[DIGEST_SIZE];
		char* pdb = db;
		for (DigestEngine::Digest::const_iterator it = d.begin(); it != d.end(); ++it)
			*pdb++ = *it;
		_engine.reset();
		_engine.update(_opad, BLOCK_SIZE);
		_engine.update(db, DIGEST_SIZE);
		const DigestEngine::Digest& result = _engine.digest();
		reset();
		return result;
	}

protected:
	void init(const char* passphrase, std::size_t length)
	{
		_ipad = new char[BLOCK_SIZE];
		_opad = new char[BLOCK_SIZE];
		std::memset(_ipad, 0, BLOCK_SIZE);
		std::memset(_opad, 0, BLOCK_SIZE);
		if (length > BLOCK_SIZE)
		{
			_engine.reset();
			_engine.update(passphrase, length);
			const DigestEngine::Digest& d = _engine.digest();
			char* ipad = _ipad;
			char* opad = _opad;
			int n = BLOCK_SIZE;
			for (DigestEngine::Digest::const_iterator it = d.begin(); it != d.end() && n-- > 0; ++it)
			{
				*ipad++ = *it;
				*opad++ = *it;
			}
		}
		else
		{
			std::memcpy(_ipad, passphrase, length);
			std::memcpy(_opad, passphrase, length);
		}
		for (int i = 0; i < BLOCK_SIZE; ++i)
		{
			_ipad[i] ^= 0x36;
			_opad[i] ^= 0x5c;
		}
		reset();
	}
	
	void updateImpl(const void* data, std::size_t length)
	{
		_engine.update(data, length);
	}

private:
	HMACEngine();
	HMACEngine(const HMACEngine&);
	HMACEngine& operator = (const HMACEngine&);

	Engine _engine;
	char*  _ipad;
	char*  _opad;
};


} // namespace Poco


#endif // Foundation_HMACEngine_INCLUDED
