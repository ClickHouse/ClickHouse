//
// Context.h
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  Context
//
// Definition of the Context class.
//
// Copyright (c) 2006-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_Context_INCLUDED
#define NetSSL_Context_INCLUDED


#include <cstdlib>
#include <openssl/ssl.h>
#include "Poco/AutoPtr.h"
#include "Poco/Crypto/RSAKey.h"
#include "Poco/Crypto/X509Certificate.h"
#include "Poco/Net/NetSSL.h"
#include "Poco/Net/SocketDefs.h"
#include "Poco/RefCountedObject.h"


namespace Poco
{
namespace Net
{


    class NetSSL_API Context : public Poco::RefCountedObject
    /// This class encapsulates context information for
    /// an SSL server or client, such as the certificate
    /// verification mode and the location of certificates
    /// and private key files, as well as the list of
    /// supported ciphers.
    ///
    /// The Context class is also used to control
    /// SSL session caching on the server and client side.
    {
    public:
        typedef Poco::AutoPtr<Context> Ptr;

        enum Usage
        {
            CLIENT_USE, /// Context is used by a client.
            SERVER_USE, /// Context is used by a server.
            TLSV1_CLIENT_USE, /// Context is used by a client requiring TLSv1.
            TLSV1_SERVER_USE, /// Context is used by a server requiring TLSv1.
            TLSV1_1_CLIENT_USE, /// Context is used by a client requiring TLSv1.1 (OpenSSL 1.0.0 or newer).
            TLSV1_1_SERVER_USE, /// Context is used by a server requiring TLSv1.1 (OpenSSL 1.0.0 or newer).
            TLSV1_2_CLIENT_USE, /// Context is used by a client requiring TLSv1.2 (OpenSSL 1.0.1 or newer).
            TLSV1_2_SERVER_USE /// Context is used by a server requiring TLSv1.2 (OpenSSL 1.0.1 or newer).
        };

        enum VerificationMode
        {
            VERIFY_NONE = SSL_VERIFY_NONE,
            /// Server: The server will not send a client certificate
            /// request to the client, so the client will not send a certificate.
            ///
            /// Client: If not using an anonymous cipher (by default disabled),
            /// the server will send a certificate which will be checked, but
            /// the result of the check will be ignored.

            VERIFY_RELAXED = SSL_VERIFY_PEER,
            /// Server: The server sends a client certificate request to the
            /// client. The certificate returned (if any) is checked.
            /// If the verification process fails, the TLS/SSL handshake is
            /// immediately terminated with an alert message containing the
            /// reason for the verification failure.
            ///
            /// Client: The server certificate is verified, if one is provided.
            /// If the verification process fails, the TLS/SSL handshake is
            /// immediately terminated with an alert message containing the
            /// reason for the verification failure.

            VERIFY_STRICT = SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT,
            /// Server: If the client did not return a certificate, the TLS/SSL
            /// handshake is immediately terminated with a handshake failure
            /// alert.
            ///
            /// Client: Same as VERIFY_RELAXED.

            VERIFY_ONCE = SSL_VERIFY_PEER | SSL_VERIFY_CLIENT_ONCE
            /// Server: Only request a client certificate on the initial
            /// TLS/SSL handshake. Do not ask for a client certificate
            /// again in case of a renegotiation.
            ///
            /// Client: Same as VERIFY_RELAXED.
        };

        enum Protocols
        {
            PROTO_SSLV2 = 0x01,
            PROTO_SSLV3 = 0x02,
            PROTO_TLSV1 = 0x04,
            PROTO_TLSV1_1 = 0x08,
            PROTO_TLSV1_2 = 0x10
        };

        struct NetSSL_API CAPaths
        {
            std::string caDefaultDir;
            std::string caDefaultFile;
            std::string caLocation;
        };

        struct NetSSL_API Params
        {
            Params();
            /// Initializes the struct with default values.

            std::string privateKeyFile;
            /// Path to the private key file used for encryption.
            /// Can be empty if no private key file is used.

            std::string certificateFile;
            /// Path to the certificate file (in PEM format).
            /// If the private key and the certificate are stored in the same file, this
            /// can be empty if privateKeyFile is given.

            std::string caLocation;
            /// Path to the file or directory containing the CA/root certificates.
            /// Can be empty if the OpenSSL builtin CA certificates
            /// are used (see loadDefaultCAs).

            VerificationMode verificationMode;
            /// Specifies whether and how peer certificates are validated.
            /// Defaults to VERIFY_RELAXED.

            int verificationDepth;
            /// Sets the upper limit for verification chain sizes. Verification
            /// will fail if a certificate chain larger than this is encountered.
            /// Defaults to 9.

            bool loadDefaultCAs;
            /// Specifies whether the builtin CA certificates from OpenSSL are used.
            /// Defaults to false.

            std::string cipherList;
            /// Specifies the supported ciphers in OpenSSL notation.
            /// Defaults to "ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH".

            std::string dhParamsFile;
            /// Specifies a file containing Diffie-Hellman parameters.
            /// If empty, the default parameters are used.

            std::string ecdhCurve;
            /// Specifies the name of the curve to use for ECDH, based
            /// on the curve names specified in RFC 4492.
            /// Defaults to "prime256v1".
        };

        Context(Usage usage, const Params & params);
        /// Creates a Context using the given parameters.
        ///
        ///   * usage specifies whether the context is used by a client or server.
        ///   * params specifies the context parameters.

        Context(
            Usage usage,
            const std::string & privateKeyFile,
            const std::string & certificateFile,
            const std::string & caLocation,
            VerificationMode verificationMode = VERIFY_RELAXED,
            int verificationDepth = 9,
            bool loadDefaultCAs = false,
            const std::string & cipherList = "ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH");
        /// Creates a Context.
        ///
        ///   * usage specifies whether the context is used by a client or server.
        ///   * privateKeyFile contains the path to the private key file used for encryption.
        ///     Can be empty if no private key file is used.
        ///   * certificateFile contains the path to the certificate file (in PEM format).
        ///     If the private key and the certificate are stored in the same file, this
        ///     can be empty if privateKeyFile is given.
        ///   * caLocation contains the path to the file or directory containing the
        ///     CA/root certificates. Can be empty if the OpenSSL builtin CA certificates
        ///     are used (see loadDefaultCAs).
        ///   * verificationMode specifies whether and how peer certificates are validated.
        ///   * verificationDepth sets the upper limit for verification chain sizes. Verification
        ///     will fail if a certificate chain larger than this is encountered.
        ///   * loadDefaultCAs specifies whether the builtin CA certificates from OpenSSL are used.
        ///   * cipherList specifies the supported ciphers in OpenSSL notation.
        ///
        /// Note: If the private key is protected by a passphrase, a PrivateKeyPassphraseHandler
        /// must have been setup with the SSLManager, or the SSLManager's PrivateKeyPassphraseRequired
        /// event must be handled.

        Context(
            Usage usage,
            const std::string & caLocation,
            VerificationMode verificationMode = VERIFY_RELAXED,
            int verificationDepth = 9,
            bool loadDefaultCAs = false,
            const std::string & cipherList = "ALL:!ADH:!LOW:!EXP:!MD5:!3DES:@STRENGTH");
        /// Creates a Context.
        ///
        ///   * usage specifies whether the context is used by a client or server.
        ///   * caLocation contains the path to the file or directory containing the
        ///     CA/root certificates. Can be empty if the OpenSSL builtin CA certificates
        ///     are used (see loadDefaultCAs).
        ///   * verificationMode specifies whether and how peer certificates are validated.
        ///   * verificationDepth sets the upper limit for verification chain sizes. Verification
        ///     will fail if a certificate chain larger than this is encountered.
        ///   * loadDefaultCAs specifies whether the builtin CA certificates from OpenSSL are used.
        ///   * cipherList specifies the supported ciphers in OpenSSL notation.
        ///
        /// Note that a private key and/or certificate must be specified with
        /// usePrivateKey()/useCertificate() before the Context can be used.

        ~Context();
        /// Destroys the Context.

        void useCertificate(const Poco::Crypto::X509Certificate & certificate);
        /// Sets the certificate to be used by the Context.
        ///
        /// To set-up a complete certificate chain, it might be
        /// necessary to call addChainCertificate() to specify
        /// additional certificates.
        ///
        /// Note that useCertificate() must always be called before
        /// usePrivateKey().

        void addChainCertificate(const Poco::Crypto::X509Certificate & certificate);
        /// Adds a certificate for certificate chain validation.

        void addCertificateAuthority(const Poco::Crypto::X509Certificate & certificate);
        /// Add one trusted certification authority to be used by the Context.

        void usePrivateKey(const Poco::Crypto::RSAKey & key);
        /// Sets the private key to be used by the Context.
        ///
        /// Note that useCertificate() must always be called before
        /// usePrivateKey().
        ///
        /// Note: If the private key is protected by a passphrase, a PrivateKeyPassphraseHandler
        /// must have been setup with the SSLManager, or the SSLManager's PrivateKeyPassphraseRequired
        /// event must be handled.

        SSL_CTX * sslContext() const;
        /// Returns the underlying OpenSSL SSL Context object.

        SSL_CTX * takeSslContext();
        /// Takes ownership of the underlying OpenSSL SSL Context object.

        Usage usage() const;
        /// Returns whether the context is for use by a client or by a server
        /// and whether TLSv1 is required.

        bool isForServerUse() const;
        /// Returns true iff the context is for use by a server.

        Context::VerificationMode verificationMode() const;
        /// Returns the verification mode.

        void enableSessionCache(bool flag = true);
        /// Enable or disable SSL/TLS session caching.
        /// For session caching to work, it must be enabled
        /// on the server, as well as on the client side.
        ///
        /// The default is disabled session caching.
        ///
        /// To enable session caching on the server side, use the
        /// two-argument version of this method to specify
        /// a session ID context.

        void enableSessionCache(bool flag, const std::string & sessionIdContext);
        /// Enables or disables SSL/TLS session caching on the server.
        /// For session caching to work, it must be enabled
        /// on the server, as well as on the client side.
        ///
        /// SessionIdContext contains the application's unique
        /// session ID context, which becomes part of each
        /// session identifier generated by the server within this
        /// context. SessionIdContext can be an arbitrary sequence
        /// of bytes with a maximum length of SSL_MAX_SSL_SESSION_ID_LENGTH.
        ///
        /// A non-empty sessionIdContext should be specified even if
        /// session caching is disabled to avoid problems with clients
        /// requesting to reuse a session (e.g. Firefox 3.6).
        ///
        /// This method may only be called on SERVER_USE Context objects.

        bool sessionCacheEnabled() const;
        /// Returns true iff the session cache is enabled.

        void setSessionCacheSize(std::size_t size);
        /// Sets the maximum size of the server session cache, in number of
        /// sessions. The default size (according to OpenSSL documentation)
        /// is 1024*20, which may be too large for many applications,
        /// especially on embedded platforms with limited memory.
        ///
        /// Specifying a size of 0 will set an unlimited cache size.
        ///
        /// This method may only be called on SERVER_USE Context objects.

        std::size_t getSessionCacheSize() const;
        /// Returns the current maximum size of the server session cache.
        ///
        /// This method may only be called on SERVER_USE Context objects.

        void setSessionTimeout(long seconds);
        /// Sets the timeout (in seconds) of cached sessions on the server.
        /// A cached session will be removed from the cache if it has
        /// not been used for the given number of seconds.
        ///
        /// This method may only be called on SERVER_USE Context objects.

        long getSessionTimeout() const;
        /// Returns the timeout (in seconds) of cached sessions on the server.
        ///
        /// This method may only be called on SERVER_USE Context objects.

        void flushSessionCache();
        /// Flushes the SSL session cache on the server.
        ///
        /// This method may only be called on SERVER_USE Context objects.

        void enableExtendedCertificateVerification(bool flag = true);
        /// Enable or disable the automatic post-connection
        /// extended certificate verification.
        ///
        /// See X509Certificate::verify() for more information.

        bool extendedCertificateVerificationEnabled() const;
        /// Returns true iff automatic extended certificate
        /// verification is enabled.

        void disableStatelessSessionResumption();
        /// Newer versions of OpenSSL support RFC 4507 tickets for stateless
        /// session resumption.
        ///
        /// The feature can be disabled by calling this method.

        void disableProtocols(int protocols);
        /// Disables the given protocols.
        ///
        /// The protocols to be disabled are specified by OR-ing
        /// values from the Protocols enumeration, e.g.:
        ///
        ///   context.disableProtocols(PROTO_SSLV2 | PROTO_SSLV3);

        void preferServerCiphers();
        /// When choosing a cipher, use the server's preferences instead of the client
        /// preferences. When not called, the SSL server will always follow the clients
        /// preferences. When called, the SSL/TLS server will choose following its own
        /// preferences.

        const CAPaths & getCAPaths();

    private:
        void init(const Params & params);
        /// Initializes the Context with the given parameters.

        void initDH(const std::string & dhFile);
        /// Initializes the Context with Diffie-Hellman parameters.

        void initECDH(const std::string & curve);
        /// Initializes the Context with Elliptic-Curve Diffie-Hellman key
        /// exchange curve parameters.

        void createSSLContext();
        /// Create a SSL_CTX object according to Context configuration.

        Usage _usage;
        VerificationMode _mode;
        SSL_CTX * _pSSLContext;
        CAPaths _caPaths;
        bool _extendedCertificateVerification;
    };


    //
    // inlines
    //
    inline Context::Usage Context::usage() const
    {
        return _usage;
    }


    inline bool Context::isForServerUse() const
    {
        return _usage == SERVER_USE || _usage == TLSV1_SERVER_USE || _usage == TLSV1_1_SERVER_USE || _usage == TLSV1_2_SERVER_USE;
    }


    inline Context::VerificationMode Context::verificationMode() const
    {
        return _mode;
    }


    inline SSL_CTX * Context::sslContext() const
    {
        return _pSSLContext;
    }

    inline SSL_CTX * Context::takeSslContext()
    {
        auto * result = _pSSLContext;
        _pSSLContext = nullptr;
        return result;
    }


    inline bool Context::extendedCertificateVerificationEnabled() const
    {
        return _extendedCertificateVerification;
    }


}
} // namespace Poco::Net


#endif // NetSSL_Context_INCLUDED
