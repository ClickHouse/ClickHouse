//
// SSLManager.h
//
// $Id: //poco/1.4/NetSSL_OpenSSL/include/Poco/Net/SSLManager.h#4 $
//
// Library: NetSSL_OpenSSL
// Package: SSLCore
// Module:  SSLManager
//
// Definition of the SSLManager class.
//
// Copyright (c) 2006-2010, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#ifndef NetSSL_SSLManager_INCLUDED
#define NetSSL_SSLManager_INCLUDED


#include "Poco/Net/NetSSL.h"
#include "Poco/Net/VerificationErrorArgs.h"
#include "Poco/Net/Context.h"
#include "Poco/Net/PrivateKeyFactoryMgr.h"
#include "Poco/Net/CertificateHandlerFactoryMgr.h"
#include "Poco/Net/InvalidCertificateHandler.h"
#include "Poco/Util/AbstractConfiguration.h"
#include "Poco/BasicEvent.h"
#include "Poco/SharedPtr.h"
#include "Poco/Mutex.h"
#include <openssl/ssl.h>
#ifdef OPENSSL_FIPS
#include <openssl/fips.h>
#endif


namespace Poco {
namespace Net {


class Context;


class NetSSL_API SSLManager
	/// SSLManager is a singleton for holding the default server/client 
	/// Context and handling callbacks for certificate verification errors
	/// and private key passphrases.
	///
	/// Proper initialization of SSLManager is critical.
	///
	/// SSLManager can be initialized manually, by calling initializeServer()
	/// and/or initializeClient(), or intialization can be automatic. In the latter
	/// case, a Poco::Util::Application instance must be available and the required
	/// configuration properties must be set (see below).
	///
	/// Note that manual intialization must happen very early in the application,
	/// before defaultClientContext() or defaultServerContext() are called.
	///
	/// If defaultClientContext() and defaultServerContext() are never called
	/// in an application, initialization of SSLManager can be omitted.
	/// However, in this case, delegates for the ServerVerificationError,
	/// ClientVerificationError and PrivateKeyPassphraseRequired events
	/// must be registered.
	///
	/// An exemplary documentation which sets either the server or client default context and creates 
	/// a PrivateKeyPassphraseHandler that reads the password from the XML file looks like this:
	///
	///    <AppConfig>
	///       <openSSL>
	///          <server|client>
	///            <privateKeyFile>mycert.key</privateKeyFile>
	///            <certificateFile>mycert.crt</certificateFile>
	///            <caConfig>rootcert.pem</caConfig>
	///            <verificationMode>none|relaxed|strict|once</verificationMode>
	///            <verificationDepth>1..9</verificationDepth>
	///            <loadDefaultCAFile>true|false</loadDefaultCAFile>
	///            <cipherList>ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH</cipherList>
	///            <privateKeyPassphraseHandler>
	///                <name>KeyFileHandler</name>
	///                <options>
	///                    <password>test</password>
	///                </options>
	///            </privateKeyPassphraseHandler>
	///            <invalidCertificateHandler>
	///                 <name>ConsoleCertificateHandler</name>
	///            </invalidCertificateHandler>
	///            <cacheSessions>true|false</cacheSessions>
	///            <sessionIdContext>someString</sessionIdContext> <!-- server only -->
	///            <sessionCacheSize>0..n</sessionCacheSize>       <!-- server only -->
	///            <sessionTimeout>0..n</sessionTimeout>           <!-- server only -->
	///            <extendedVerification>true|false</extendedVerification>
	///            <requireTLSv1>true|false</requireTLSv1>
	///            <requireTLSv1_1>true|false</requireTLSv1_1>
	///            <requireTLSv1_2>true|false</requireTLSv1_2>
	///          </server|client>
	///          <fips>false</fips>
	///       </openSSL>
	///    </AppConfig>
	///
	/// Following is a list of supported configuration properties. Property names must always
	/// be prefixed with openSSL.server or openSSL.client. Some properties are only supported
	/// for servers.
	/// 
	///    - privateKeyFile (string): The path to the file containing the private key for the certificate
	///      in PEM format (or containing both the private key and the certificate).
	///    - certificateFile (string): The Path to the file containing the server's or client's certificate
	///      in PEM format. Can be omitted if the the file given in privateKeyFile contains the certificate as well.
	///    - caConfig (string): The path to the file or directory containing the trusted root certificates.
	///    - verificationMode (string): Specifies whether and how peer certificates are validated (see
	///      the Context class for details). Valid values are none, relaxed, strict, once.
	///    - verificationDepth (integer, 1-9): Sets the upper limit for verification chain sizes. Verification
	///      will fail if a certificate chain larger than this is encountered.
	///    - loadDefaultCAFile (boolean): Specifies wheter the builtin CA certificates from OpenSSL are used.
	///    - cipherList (string): Specifies the supported ciphers in OpenSSL notation
	///      (e.g. "ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH").
	///    - privateKeyPassphraseHandler.name (string): The name of the class (subclass of PrivateKeyPassphraseHandler)
	///      used for obtaining the passphrase for accessing the private key.
	///    - privateKeyPassphraseHandler.options.password (string): The password to be used by KeyFileHandler.
	///    - invalidCertificateHandler.name: The name of the class (subclass of CertificateHandler)
	///      used for confirming invalid certificates.
	///    - cacheSessions (boolean): Enables or disables session caching.
	///    - sessionIdContext (string): contains the application's unique session ID context, which becomes 
	///      part of each session identifier generated by the server. Can be an arbitrary sequence 
	///      of bytes with a maximum length of SSL_MAX_SSL_SESSION_ID_LENGTH. Should be specified
	///      for a server to enable session caching. Should be specified even if session caching
	///      is disabled to avoid problems with clients that request session caching (e.g. Firefox 3.6).
	///      If not specified, defaults to ${application.name}.
	///    - sessionCacheSize (integer): Sets the maximum size of the server session cache, in number of
	///      sessions. The default size (according to OpenSSL documentation) is 1024*20, which may be too 
	///      large for many applications, especially on embedded platforms with limited memory.
	///      Specifying a size of 0 will set an unlimited cache size.
	///    - sessionTimeout (integer):  Sets the timeout (in seconds) of cached sessions on the server.
	///    - extendedVerification (boolean): Enable or disable the automatic post-connection
	///      extended certificate verification.
	///    - requireTLSv1 (boolean): Require a TLSv1 connection.
	///    - requireTLSv1_1 (boolean): Require a TLSv1.1 connection.
	///    - requireTLSv1_2 (boolean): Require a TLSv1.2 connection.
	///    - fips: Enable or disable OpenSSL FIPS mode. Only supported if the OpenSSL version 
	///      that this library is built against supports FIPS mode.
{
public:
	typedef Poco::SharedPtr<PrivateKeyPassphraseHandler> PrivateKeyPassphraseHandlerPtr;
	typedef Poco::SharedPtr<InvalidCertificateHandler> InvalidCertificateHandlerPtr;

	Poco::BasicEvent<VerificationErrorArgs> ServerVerificationError;
		/// Fired whenever a certificate verification error is detected by the server during a handshake.

	Poco::BasicEvent<VerificationErrorArgs> ClientVerificationError;
		/// Fired whenever a certificate verification error is detected by the client during a handshake.

	Poco::BasicEvent<std::string> PrivateKeyPassphraseRequired;
		/// Fired when a encrypted certificate is loaded. Not setting the password
		/// in the event parameter will result in a failure to load the certificate.

	static SSLManager& instance();
		/// Returns the instance of the SSLManager singleton.

	void initializeServer(PrivateKeyPassphraseHandlerPtr ptrPassphraseHandler, InvalidCertificateHandlerPtr ptrCertificateHandler, Context::Ptr ptrContext);
		/// Initializes the server side of the SSLManager with a default passphrase handler, a default invalid certificate handler and a default context. If this method
		/// is never called the SSLmanager will try to initialize its members from an application configuration.
		///
		/// PtrPassphraseHandler and ptrCertificateHandler can be 0. However, in this case, event delegates
		/// must be registered with the ServerVerificationError and PrivateKeyPassphraseRequired events.
		///
		/// Note: Always create the handlers (or register the corresponding event delegates) before creating 
		/// the Context, as during creation of the Context the passphrase for the private key might be needed.
		///
		/// Valid initialization code would be:
		///     SharedPtr<PrivateKeyPassphraseHandler> pConsoleHandler = new KeyConsoleHandler;
		///     SharedPtr<InvalidCertificateHandler> pInvalidCertHandler = new ConsoleCertificateHandler;
		///     Context::Ptr pContext = new Context(Context::SERVER_USE, "any.pem", "any.pem", "rootcert.pem", Context::VERIFY_RELAXED, 9, false, "ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH");
		///     SSLManager::instance().initializeServer(pConsoleHandler, pInvalidCertHandler, pContext);

	void initializeClient(PrivateKeyPassphraseHandlerPtr ptrPassphraseHandler, InvalidCertificateHandlerPtr ptrHandler, Context::Ptr ptrContext);
		/// Initializes the client side of the SSLManager with a default passphrase handler, a default invalid certificate handler and a default context. If this method
		/// is never called the SSLmanager will try to initialize its members from an application configuration.
		///
		/// PtrPassphraseHandler and ptrCertificateHandler can be 0. However, in this case, event delegates
		/// must be registered with the ClientVerificationError and PrivateKeyPassphraseRequired events.
		///
		/// Note: Always create the handlers (or register the corresponding event delegates) before creating 
		/// the Context, as during creation of the Context the passphrase for the private key might be needed.
		///
		/// Valid initialization code would be:
		///     SharedPtr<PrivateKeyPassphraseHandler> pConsoleHandler = new KeyConsoleHandler;
		///     SharedPtr<InvalidCertificateHandler> pInvalidCertHandler = new ConsoleCertificateHandler;
		///     Context::Ptr pContext = new Context(Context::CLIENT_USE, "", "", "rootcert.pem", Context::VERIFY_RELAXED, 9, false, "ALL:!ADH:!LOW:!EXP:!MD5:@STRENGTH");
		///     SSLManager::instance().initializeClient(pConsoleHandler, pInvalidCertHandler, pContext);

	Context::Ptr defaultServerContext();
		/// Returns the default Context used by the server. 
		///
		/// Unless initializeServer() has been called, the first call to this method initializes the default Context
		/// from the application configuration.

	Context::Ptr defaultClientContext();
		/// Returns the default Context used by the client. 
		///
		/// Unless initializeClient() has been called, the first call to this method initializes the default Context
		/// from the application configuration.

	PrivateKeyPassphraseHandlerPtr serverPassphraseHandler();
		/// Returns the configured passphrase handler of the server. If none is set, the method will create a default one
		/// from an application configuration.

	InvalidCertificateHandlerPtr serverCertificateHandler();
		/// Returns an initialized certificate handler (used by the server to verify client cert) which determines how invalid certificates are treated.
		/// If none is set, it will try to auto-initialize one from an application configuration.

	PrivateKeyPassphraseHandlerPtr clientPassphraseHandler();
		/// Returns the configured passphrase handler of the client. If none is set, the method will create a default one
		/// from an application configuration.

	InvalidCertificateHandlerPtr clientCertificateHandler();
		/// Returns an initialized certificate handler (used by the client to verify server cert) which determines how invalid certificates are treated.
		/// If none is set, it will try to auto-initialize one from an application configuration.

	PrivateKeyFactoryMgr& privateKeyFactoryMgr();
		/// Returns the private key factory manager which stores the 
		/// factories for the different registered passphrase handlers for private keys.

	CertificateHandlerFactoryMgr& certificateHandlerFactoryMgr();
		/// Returns the CertificateHandlerFactoryMgr which stores the 
		/// factories for the different registered certificate handlers.

	static bool isFIPSEnabled();
		// Returns true if FIPS mode is enabled, false otherwise.
		
	void shutdown();
		/// Shuts down the SSLManager and releases the default Context
		/// objects. After a call to shutdown(), the SSLManager can no
		/// longer be used.
		///
		/// Normally, it's not necessary to call this method directly, as this
		/// will be called either by uninitializeSSL(), or when
		/// the SSLManager instance is destroyed.

	static const std::string CFG_SERVER_PREFIX;
	static const std::string CFG_CLIENT_PREFIX;

protected:
	static int verifyClientCallback(int ok, X509_STORE_CTX* pStore);
		/// The return value of this method defines how errors in
		/// verification are handled. Return 0 to terminate the handshake,
		/// or 1 to continue despite the error.

	static int verifyServerCallback(int ok, X509_STORE_CTX* pStore);
		/// The return value of this method defines how errors in
		/// verification are handled. Return 0 to terminate the handshake,
		/// or 1 to continue despite the error.

	static int privateKeyPassphraseCallback(char* pBuf, int size, int flag, void* userData);
		/// Method is invoked by OpenSSL to retrieve a passwd for an encrypted certificate.
		/// The request is delegated to the PrivatekeyPassword event. This method returns the
		/// length of the password.
		
	static Poco::Util::AbstractConfiguration& appConfig();
		/// Returns the application configuration.
		///
		/// Throws a InvalidStateException if not application instance
		/// is available.

private:
	SSLManager();
		/// Creates the SSLManager.

	~SSLManager();
		/// Destroys the SSLManager.

	void initDefaultContext(bool server);
		/// Inits the default context, the first time it is accessed.

	void initEvents(bool server);
		/// Registers delegates at the events according to the configuration.

	void initPassphraseHandler(bool server);
		/// Inits the passphrase handler.

	void initCertificateHandler(bool server);
		/// Inits the certificate handler.

	static int verifyCallback(bool server, int ok, X509_STORE_CTX* pStore);
		/// The return value of this method defines how errors in
		/// verification are handled. Return 0 to terminate the handshake,
		/// or 1 to continue despite the error.

	PrivateKeyFactoryMgr             _factoryMgr;
	CertificateHandlerFactoryMgr     _certHandlerFactoryMgr;
	Context::Ptr                     _ptrDefaultServerContext;
	PrivateKeyPassphraseHandlerPtr   _ptrServerPassphraseHandler;
	InvalidCertificateHandlerPtr     _ptrServerCertificateHandler;
	Context::Ptr                     _ptrDefaultClientContext;
	PrivateKeyPassphraseHandlerPtr   _ptrClientPassphraseHandler;
	InvalidCertificateHandlerPtr     _ptrClientCertificateHandler;
	Poco::FastMutex                  _mutex;

	static const std::string CFG_PRIV_KEY_FILE;
	static const std::string CFG_CERTIFICATE_FILE;
	static const std::string CFG_CA_LOCATION;
	static const std::string CFG_VER_MODE;
	static const Context::VerificationMode VAL_VER_MODE;
	static const std::string CFG_VER_DEPTH;
	static const int         VAL_VER_DEPTH;
	static const std::string CFG_ENABLE_DEFAULT_CA;
	static const bool        VAL_ENABLE_DEFAULT_CA;
	static const std::string CFG_CIPHER_LIST;
	static const std::string CFG_CYPHER_LIST; // for backwards compatibility
	static const std::string VAL_CIPHER_LIST;
	static const std::string CFG_DELEGATE_HANDLER;
	static const std::string VAL_DELEGATE_HANDLER;
	static const std::string CFG_CERTIFICATE_HANDLER;
	static const std::string VAL_CERTIFICATE_HANDLER;
	static const std::string CFG_CACHE_SESSIONS;
	static const std::string CFG_SESSION_ID_CONTEXT;
	static const std::string CFG_SESSION_CACHE_SIZE;
	static const std::string CFG_SESSION_TIMEOUT;
	static const std::string CFG_EXTENDED_VERIFICATION;
	static const std::string CFG_REQUIRE_TLSV1;
	static const std::string CFG_REQUIRE_TLSV1_1;
	static const std::string CFG_REQUIRE_TLSV1_2;

#ifdef OPENSSL_FIPS
	static const std::string CFG_FIPS_MODE;
	static const bool        VAL_FIPS_MODE;
#endif

	friend class Poco::SingletonHolder<SSLManager>;
	friend class Context;
};


//
// inlines
//
inline PrivateKeyFactoryMgr& SSLManager::privateKeyFactoryMgr()
{
	return _factoryMgr;
}


inline CertificateHandlerFactoryMgr& SSLManager::certificateHandlerFactoryMgr()
{
	return _certHandlerFactoryMgr;
}


inline bool SSLManager::isFIPSEnabled()
{
#ifdef OPENSSL_FIPS
	return FIPS_mode() ? true : false;
#else
	return false;
#endif
}


inline int SSLManager::verifyServerCallback(int ok, X509_STORE_CTX* pStore)
{
	return SSLManager::verifyCallback(true, ok, pStore);
}


inline int SSLManager::verifyClientCallback(int ok, X509_STORE_CTX* pStore)
{
	return SSLManager::verifyCallback(false, ok, pStore);
}


} } // namespace Poco::Net


#endif // NetSSL_SSLManager_INCLUDED
