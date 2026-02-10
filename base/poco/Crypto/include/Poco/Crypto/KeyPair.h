//
// KeyPair.h
//
//
// Library: Crypto
// Package: CryptoCore
// Module:  KeyPair
//
// Definition of the KeyPair class.
//
// Copyright (c) 2008, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Crypto_KeyPair_INCLUDED
#define Crypto_KeyPair_INCLUDED


#include "Poco/Crypto/Crypto.h"
#include "Poco/Crypto/KeyPairImpl.h"


namespace Poco
{
namespace Crypto
{


    class X509Certificate;


    class Crypto_API KeyPair
    /// This is a parent class for classes storing a key pair, consisting
    /// of private and public key. Storage of the private key is optional.
    ///
    /// If a private key is available, the KeyPair can be
    /// used for decrypting data (encrypted with the public key)
    /// or computing secure digital signatures.
    {
    public:
        enum Type
        {
            KT_RSA = KeyPairImpl::KT_RSA_IMPL,
            KT_EC = KeyPairImpl::KT_EC_IMPL
        };

        explicit KeyPair(KeyPairImpl::Ptr pKeyPairImpl = 0);
        /// Extracts the RSA public key from the given certificate.

        virtual ~KeyPair();
        /// Destroys the KeyPair.

        virtual int size() const;
        /// Returns the RSA modulus size.

        KeyPairImpl::Ptr impl() const;
        /// Returns the impl object.

        const std::string & name() const;
        /// Returns key pair name

        Type type() const;
        /// Returns key pair type

    private:
        KeyPairImpl::Ptr _pImpl;
    };


    //
    // inlines
    //

    inline int KeyPair::size() const
    {
        return _pImpl->size();
    }

    inline const std::string & KeyPair::name() const
    {
        return _pImpl->name();
    }

    inline KeyPairImpl::Ptr KeyPair::impl() const
    {
        return _pImpl;
    }


    inline KeyPair::Type KeyPair::type() const
    {
        return (KeyPair::Type)impl()->type();
    }


}
} // namespace Poco::Crypto


#endif // Crypto_KeyPair_INCLUDED
