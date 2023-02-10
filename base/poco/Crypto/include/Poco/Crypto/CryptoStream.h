//
// CryptoStream.h
//
// Library: Crypto
// Package: Cipher
// Module:  CryptoStream
//
// Definition of the CryptoStreamBuf, CryptoInputStream and CryptoOutputStream
// classes.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Crypto_CryptoStream_INCLUDED
#define Crypto_CryptoStream_INCLUDED


#include "Poco/Crypto/Crypto.h"
#include "Poco/BufferedStreamBuf.h"
#include "Poco/Buffer.h"
#include <iostream>


namespace Poco {
namespace Crypto {


class CryptoTransform;
class Cipher;


class Crypto_API CryptoStreamBuf: public Poco::BufferedStreamBuf
	/// This stream buffer performs cryptographic transformation on the data
	/// going through it.
{
public:
	CryptoStreamBuf(std::istream& istr, CryptoTransform* pTransform, std::streamsize bufferSize = 8192);
	CryptoStreamBuf(std::ostream& ostr, CryptoTransform* pTransform, std::streamsize bufferSize = 8192);

	virtual ~CryptoStreamBuf();

	void close();
		/// Flushes all buffers and finishes the encryption.

protected:
	int readFromDevice(char* buffer, std::streamsize length);
	int writeToDevice(const char* buffer, std::streamsize length);

private:
	CryptoTransform* _pTransform;
	std::istream*	 _pIstr;
	std::ostream*	 _pOstr;
	bool			 _eof;

	Poco::Buffer<unsigned char> _buffer;

	CryptoStreamBuf(const CryptoStreamBuf&);
	CryptoStreamBuf& operator = (const CryptoStreamBuf&);
};


class Crypto_API CryptoIOS: public virtual std::ios
	/// The base class for CryptoInputStream and CryptoOutputStream.
	///
	/// This class is needed to ensure correct initialization order of the
	/// stream buffer and base classes.
{
public:
	CryptoIOS(std::istream& istr, CryptoTransform* pTransform, std::streamsize bufferSize = 8192);
	CryptoIOS(std::ostream& ostr, CryptoTransform* pTransform, std::streamsize bufferSize = 8192);
	~CryptoIOS();
	CryptoStreamBuf* rdbuf();

protected:
	CryptoStreamBuf _buf;
};


class Crypto_API CryptoInputStream: public CryptoIOS, public std::istream
	/// This stream transforms all data passing through it using the given
	/// CryptoTransform.
	///
	/// Use a CryptoTransform object provided by Cipher::createEncrytor() or
	/// Cipher::createDecryptor() to create an encrypting or decrypting stream,
	/// respectively.
{
public:
	CryptoInputStream(std::istream& istr, CryptoTransform* pTransform, std::streamsize bufferSize = 8192);
		/// Create a new CryptoInputStream object. The CryptoInputStream takes the
		/// ownership of the given CryptoTransform object.

	CryptoInputStream(std::istream& istr, Cipher& cipher, std::streamsize bufferSize = 8192);
		/// Create a new encrypting CryptoInputStream object using the given cipher.

	~CryptoInputStream();
		/// Destroys the CryptoInputStream.
};


class Crypto_API CryptoOutputStream: public CryptoIOS, public std::ostream
	/// This stream transforms all data passing through it using the given
	/// CryptoTransform.
	///
	/// Use a CryptoTransform object provided by Cipher::createEncrytor() or
	/// Cipher::createDecryptor() to create an encrypting or decrypting stream,
	/// respectively.
	///
	/// After all data has been passed through the stream, close() must be called
	/// to ensure completion of cryptographic transformation.
{
public:
	CryptoOutputStream(std::ostream& ostr, CryptoTransform* pTransform, std::streamsize bufferSize = 8192);
		/// Create a new CryptoOutputStream object. The CryptoOutputStream takes the
		/// ownership of the given CryptoTransform object.

	CryptoOutputStream(std::ostream& ostr, Cipher& cipher, std::streamsize bufferSize = 8192);
		/// Create a new decrypting CryptoOutputStream object using the given cipher.

	~CryptoOutputStream();
		/// Destroys the CryptoOutputStream.

	void close();
		/// Flushes all buffers and finishes the encryption.
};


class Crypto_API DecryptingInputStream: public CryptoIOS, public std::istream
	/// This stream decrypts all data passing through it using the given
	/// Cipher.
{
public:
	DecryptingInputStream(std::istream& istr, Cipher& cipher, std::streamsize bufferSize = 8192);
		/// Create a new DecryptingInputStream object using the given cipher.

	~DecryptingInputStream();
		/// Destroys the DecryptingInputStream.
};


class Crypto_API DecryptingOutputStream: public CryptoIOS, public std::ostream
	/// This stream decrypts all data passing through it using the given
	/// Cipher.
{
public:
	DecryptingOutputStream(std::ostream& ostr, Cipher& cipher, std::streamsize bufferSize = 8192);
		/// Create a new DecryptingOutputStream object using the given cipher.

	~DecryptingOutputStream();
		/// Destroys the DecryptingOutputStream.

	void close();
		/// Flushes all buffers and finishes the decryption.
};


class Crypto_API EncryptingInputStream: public CryptoIOS, public std::istream
	/// This stream encrypts all data passing through it using the given
	/// Cipher.
{
public:
	EncryptingInputStream(std::istream& istr, Cipher& cipher, std::streamsize bufferSize = 8192);
		/// Create a new EncryptingInputStream object using the given cipher.

	~EncryptingInputStream();
		/// Destroys the EncryptingInputStream.
};


class Crypto_API EncryptingOutputStream: public CryptoIOS, public std::ostream
	/// This stream encrypts all data passing through it using the given
	/// Cipher.
{
public:
	EncryptingOutputStream(std::ostream& ostr, Cipher& cipher, std::streamsize bufferSize = 8192);
		/// Create a new EncryptingOutputStream object using the given cipher.

	~EncryptingOutputStream();
		/// Destroys the EncryptingOutputStream.

	void close();
		/// Flushes all buffers and finishes the encryption.
};


} } // namespace Poco::Crypto


#endif // Crypto_CryptoStream_INCLUDED
