//
// ECKeyImpl.h
//
//
// Library: Crypto
// Package: EC
// Module:  ECKeyImpl
//
// Definition of the ECKeyImpl class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Crypto_ECKeyImplImpl_INCLUDED
#define Crypto_ECKeyImplImpl_INCLUDED


#include "Poco/Crypto/Crypto.h"
#include "Poco/Crypto/EVPPKey.h"
#include "Poco/Crypto/KeyPairImpl.h"
#include "Poco/Crypto/OpenSSLInitializer.h"
#include "Poco/RefCountedObject.h"
#include "Poco/AutoPtr.h"
#include <istream>
#include <ostream>
#include <vector>
#include <openssl/objects.h>
#include <openssl/ec.h>


namespace Poco {
namespace Crypto {


class X509Certificate;
class PKCS12Container;


class ECKeyImpl: public KeyPairImpl
	/// Elliptic Curve key clas implementation.
{
public:
	typedef Poco::AutoPtr<ECKeyImpl> Ptr;
	typedef std::vector<unsigned char> ByteVec;

	ECKeyImpl(const EVPPKey& key);
		/// Constructs ECKeyImpl by extracting the EC key.

	ECKeyImpl(const X509Certificate& cert);
		/// Constructs ECKeyImpl by extracting the EC public key from the given certificate.

	ECKeyImpl(const PKCS12Container& cert);
		/// Constructs ECKeyImpl by extracting the EC private key from the given certificate.

	ECKeyImpl(int eccGroup);
		/// Creates the ECKey of the specified group. Creates a new public/private keypair using the given parameters.
		/// Can be used to sign data and verify signatures.

	ECKeyImpl(const std::string& publicKeyFile, const std::string& privateKeyFile, const std::string& privateKeyPassphrase);
		/// Creates the ECKey, by reading public and private key from the given files and
		/// using the given passphrase for the private key. Can only by used for signing if 
		/// a private key is available. 

	ECKeyImpl(std::istream* pPublicKeyStream, std::istream* pPrivateKeyStream, const std::string& privateKeyPassphrase);
		/// Creates the ECKey. Can only by used for signing if pPrivKey
		/// is not null. If a private key file is specified, you don't need to
		/// specify a public key file. OpenSSL will auto-create it from the private key.

	~ECKeyImpl();
		/// Destroys the ECKeyImpl.

	EC_KEY* getECKey();
		/// Returns the OpenSSL EC key.

	const EC_KEY* getECKey() const;
		/// Returns the OpenSSL EC key.

	int size() const;
		/// Returns the EC key length in bits.

	int groupId() const;
		/// Returns the EC key group integer Id.

	std::string groupName() const;
		/// Returns the EC key group name.

	void save(const std::string& publicKeyFile,
		const std::string& privateKeyFile = "",
		const std::string& privateKeyPassphrase = "") const;
		/// Exports the public and private keys to the given files. 
		///
		/// If an empty filename is specified, the corresponding key
		/// is not exported.

	void save(std::ostream* pPublicKeyStream,
		std::ostream* pPrivateKeyStream = 0,
		const std::string& privateKeyPassphrase = "") const;
		/// Exports the public and private key to the given streams.
		///
		/// If a null pointer is passed for a stream, the corresponding
		/// key is not exported.

	static std::string getCurveName(int nid = -1);
		/// Returns elliptical curve name corresponding to
		/// the given nid; if nid is not found, returns
		/// empty string.
		///
		/// If nid is -1, returns first curve name.
		///
		/// If no curves are found, returns empty string;

	static int getCurveNID(std::string& name);
		/// Returns the NID of the specified curve.
		///
		/// If name is empty, returns the first curve NID
		/// and updates the name accordingly.

	static bool hasCurve(const std::string& name);
		/// Returns true if the named curve is found,
		/// false otherwise.

private:
	void checkEC(const std::string& method, const std::string& func) const;
	void freeEC();

	EC_KEY* _pEC;
};


//
// inlines
//
inline EC_KEY* ECKeyImpl::getECKey()
{
	return _pEC;
}


inline const EC_KEY* ECKeyImpl::getECKey() const
{
	return _pEC;
}


inline std::string ECKeyImpl::groupName() const
{
	return OBJ_nid2sn(groupId());
}


inline void ECKeyImpl::save(const std::string& publicKeyFile,
	const std::string& privateKeyFile,
	const std::string& privateKeyPassphrase) const
{
	EVPPKey(_pEC).save(publicKeyFile, privateKeyFile, privateKeyPassphrase);
}


inline void ECKeyImpl::save(std::ostream* pPublicKeyStream,
	std::ostream* pPrivateKeyStream,
	const std::string& privateKeyPassphrase) const
{
	EVPPKey(_pEC).save(pPublicKeyStream, pPrivateKeyStream, privateKeyPassphrase);
}


} } // namespace Poco::Crypto


#endif // Crypto_ECKeyImplImpl_INCLUDED
