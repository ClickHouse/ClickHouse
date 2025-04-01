//
// OpenSSLInitializer.h
//
// Library: Crypto
// Package: CryptoCore
// Module:  OpenSSLInitializer
//
// Definition of the OpenSSLInitializer class.
//
// Copyright (c) 2006-2009, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef Crypto_OpenSSLInitializer_INCLUDED
#define Crypto_OpenSSLInitializer_INCLUDED


#include <openssl/crypto.h>
#include <openssl/evp.h>
#include <openssl/provider.h>

#include "Poco/AtomicCounter.h"
#include "Poco/Crypto/Crypto.h"


namespace Poco
{
namespace Crypto
{


    class Crypto_API OpenSSLInitializer
    /// Initializes the OpenSSL library.
    ///
    /// The class ensures the earliest initialization and the
    /// latest shutdown of the OpenSSL library.
    {
    public:
        OpenSSLInitializer();
        /// Automatically initialize OpenSSL on startup.

        ~OpenSSLInitializer();
        /// Automatically shut down OpenSSL on exit.

        static void initialize();
        /// Initializes the OpenSSL machinery.

        static void uninitialize();
        /// Shuts down the OpenSSL machinery.

        static bool isFIPSEnabled();
        // Returns true if FIPS mode is enabled, false otherwise.

        static void enableFIPSMode(bool enabled)
        {
            if (!enabled)
            {
                if (EVP_default_properties_enable_fips(nullptr, 0) != 1)
                    throw Exception("Failed to disable FIPS mode");
                return;
            }

            poco_assert(!_fipsProvider);

            _fipsProvider = OSSL_PROVIDER_load(nullptr, "fips");
            if (!_fipsProvider)
                throw Exception("Failed to load FIPS provider");

            if (EVP_default_properties_enable_fips(nullptr, 1) != 1)
                throw Exception("Failed to enable FIPS mode");
        }
        // Enable or disable FIPS mode.

    private:
        static Poco::AtomicCounter _rc;
        static OSSL_PROVIDER* _fipsProvider;
    };


    //
    // inlines
    //
    inline bool OpenSSLInitializer::isFIPSEnabled()
    {
        return EVP_default_properties_is_fips_enabled(nullptr);
    }

}
} // namespace Poco::Crypto


#endif // Crypto_OpenSSLInitializer_INCLUDED
