//
// ECKey.h
//
//
// Library: Crypto
// Package: EC
// Module:  ECKey
//
// Definition of the ECKey class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Crypto_ECKey_INCLUDED
#define Crypto_ECKey_INCLUDED


#include "Poco/Crypto/Crypto.h"
#include "Poco/Crypto/KeyPair.h"
#include "Poco/Crypto/ECKeyImpl.h"


namespace Poco {
namespace Crypto {


class X509Certificate;
class PKCS12Container;


class Crypto_API ECKey : public KeyPair
	/// This class stores an EC key pair, consisting
	/// of private and public key. Storage of the private
	/// key is optional.
	///
	/// If a private key is available, the ECKey can be
	/// used for decrypting data (encrypted with the public key)
	/// or computing secure digital signatures.
{
public:
	ECKey(const EVPPKey& key);
		/// Constructs ECKeyImpl by extracting the EC key.

	ECKey(const X509Certificate& cert);
		/// Extracts the EC public key from the given certificate.

	ECKey(const PKCS12Container& cert);
		/// Extracts the EC private key from the given certificate.

	ECKey(const std::string& eccGroup);
		/// Creates the ECKey. Creates a new public/private keypair using the given parameters.
		/// Can be used to sign data and verify signatures.

	ECKey(const std::string& publicKeyFile, const std::string& privateKeyFile, const std::string& privateKeyPassphrase = "");
		/// Creates the ECKey, by reading public and private key from the given files and
		/// using the given passphrase for the private key.
		///
		/// Cannot be used for signing or decryption unless a private key is available.
		///
		/// If a private key is specified, you don't need to specify a public key file.
		/// OpenSSL will auto-create the public key from the private key.

	ECKey(std::istream* pPublicKeyStream, std::istream* pPrivateKeyStream = 0, const std::string& privateKeyPassphrase = "");
		/// Creates the ECKey, by reading public and private key from the given streams and
		/// using the given passphrase for the private key.
		///
		/// Cannot be used for signing or decryption unless a private key is available.
		///
		/// If a private key is specified, you don't need to specify a public key file.
		/// OpenSSL will auto-create the public key from the private key.

	~ECKey();
		/// Destroys the ECKey.

	ECKeyImpl::Ptr impl() const;
		/// Returns the impl object.

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
	ECKeyImpl::Ptr _pImpl;
};


//
// inlines
//
inline ECKeyImpl::Ptr ECKey::impl() const
{
	return _pImpl;
}


inline std::string ECKey::getCurveName(int nid)
{
	return ECKeyImpl::getCurveName(nid);
}


inline int ECKey::getCurveNID(std::string& name)
{
	return ECKeyImpl::getCurveNID(name);
}


inline bool ECKey::hasCurve(const std::string& name)
{
	return ECKeyImpl::hasCurve(name);
}


} } // namespace Poco::Crypto


#endif // Crypto_ECKey_INCLUDED
