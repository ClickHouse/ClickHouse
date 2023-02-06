//
// SSLManager.h
//
// Library: NetSSL_Win
// Package: SSLCore
// Module:  SSLManager
//
// Definition of the SSLManager class.
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
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
#include <wincrypt.h>
#include <schannel.h>
#ifndef SECURITY_WIN32
#define SECURITY_WIN32
#endif
#include <security.h>
#include <sspi.h>


namespace Poco {
namespace Net {


class Context;


class NetSSL_Win_API SSLManager
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
	/// Note that manual initialization must happen very early in the application,
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
	///       <schannel>
	///          <server|client>
	///            <certificateName>cert Id</certificateName>
	///            <certificateStore>MY</certificateStore>
	///            <verificationMode>none|relaxed|strict</verificationMode>
	///            <revocationCheck>true|false</revocationCheck>
	///            <trustRoots>true|false</trustRoots>
	///            <useMachineStore>true|false</useMachineStore>
	///            <useStrongCrypto>true|false</useStrongCrypto>
	///            <privateKeyPassphraseHandler>
	///                <name>KeyFileHandler</name>
	///                <options>
	///                    <password>s3cr3t</password>
	///                </options>
	///            </privateKeyPassphraseHandler>
	///            <invalidCertificateHandler>
	///                 <name>ConsoleCertificateHandler</name>
	///                 <options>
	///                 </options>
	///            </invalidCertificateHandler>
	///            <requireTLSv1>true|false</requireTLSv1>
	///            <requireTLSv1_1>true|false</requireTLSv1_1>
	///            <requireTLSv1_2>true|false</requireTLSv1_2>
	///          </server|client>
	///       </schannel>
	///    </AppConfig>
	///
	/// Following is a list of supported configuration properties. Property names must always
	/// be prefixed with openSSL.server or openSSL.client. Some properties are only supported
	/// for servers.
	/// 
	///    - certificateName (string): The subject name of the certificate to use. The certificate must
	///      be available in the Windows user or machine certificate store.  
	///    - certificatePath (string): The path of a certificate and private key file in PKCS #12 format.
	///    - certificateStore (string): The certificate store location to use. 
	///      Valid values are "MY", "Root", "Trust" or "CA". Defaults to "MY".
	///    - verificationMode (string): Specifies whether and how peer certificates are validated (see
	///      the Context class for details). Valid values are "none", "relaxed", "strict". Defaults to "relaxed".
	///    - revocationCheck (boolean): Enable or disable checking of certificates against revocation list. 
	///      Defaults to true. Not supported (ignored) on Windows Embedded Compact.
	///    - trustRoots (boolean): Trust root certificates from Windows root certificate store. Defaults to true.
	///    - useMachineStore (boolean): Use Windows machine certificate store instead of user store (server only).
	///      Special user privileges may be required. Defaults to false.
	///    - useStrongCrypto (boolean): Disable known weak cryptographic algorithms, cipher suites, and 
	///      SSL/TLS protocol versions that may be otherwise enabled for better interoperability. 
	///      Defaults to true.
	///    - privateKeyPassphraseHandler.name (string): The name of the class (subclass of PrivateKeyPassphraseHandler)
	///      used for obtaining the passphrase for accessing the private key.
	///    - privateKeyPassphraseHandler.options.password (string): The password to be used by KeyFileHandler.
	///    - invalidCertificateHandler.name: The name of the class (subclass of CertificateHandler)
	///      used for confirming invalid certificates.
	///    - requireTLSv1 (boolean): Require a TLSv1 connection. 
	///    - requireTLSv1_1 (boolean): Require a TLSv1.1 connection. Not supported on Windows Embedded Compact.
	///    - requireTLSv1_2 (boolean): Require a TLSv1.2 connection. Not supported on Windows Embedded Compact.
{
public:
	typedef Poco::SharedPtr<PrivateKeyPassphraseHandler> PrivateKeyPassphraseHandlerPtr;
	typedef Poco::SharedPtr<InvalidCertificateHandler> InvalidCertificateHandlerPtr;

	Poco::BasicEvent<VerificationErrorArgs>  ServerVerificationError;
		/// Fired whenever a certificate verification error is detected by the server during a handshake.

	Poco::BasicEvent<VerificationErrorArgs>  ClientVerificationError;
		/// Fired whenever a certificate verification error is detected by the client during a handshake.

	Poco::BasicEvent<std::string> PrivateKeyPassphraseRequired;
		/// Fired when a encrypted certificate is loaded. Not setting the password
		/// in the event parameter will result in a failure to load the certificate.

	static SSLManager& instance();
		/// Returns the instance of the SSLManager singleton.

	void initializeServer(PrivateKeyPassphraseHandlerPtr ptrPassphraseHandler, InvalidCertificateHandlerPtr pCertificateHandler, Context::Ptr pContext);
		/// Initializes the server side of the SSLManager with a default invalid certificate handler and a default context. If this method
		/// is never called the SSLmanager will try to initialize its members from an application configuration.
		///
		/// pCertificateHandler can be 0. However, in this case, event delegates
		/// must be registered with the ServerVerificationError event.
		///
		/// Note: Always create the handlers (or register the corresponding event delegates) before creating 
		/// the Context.
		///
		/// Valid initialization code would be:
		///     SharedPtr<InvalidCertificateHandler> pInvalidCertHandler = new ConsoleCertificateHandler;
		///     Context::Ptr pContext = new Context(Context::SERVER_USE, "mycert");
		///     SSLManager::instance().initializeServer(pInvalidCertHandler, pContext);

	void initializeClient(PrivateKeyPassphraseHandlerPtr ptrPassphraseHandler, InvalidCertificateHandlerPtr pCertificateHandler, Context::Ptr ptrContext);
		/// Initializes the client side of the SSLManager with  a default invalid certificate handler and a default context. If this method
		/// is never called the SSLmanager will try to initialize its members from an application configuration.
		///
		/// pCertificateHandler can be 0. However, in this case, event delegates
		/// must be registered with the ClientVerificationError event.
		///
		/// Note: Always create the handlers (or register the corresponding event delegates) before creating 
		/// the Context, as during creation of the Context the passphrase for the private key might be needed.
		///
		/// Valid initialization code would be:
		///     SharedPtr<InvalidCertificateHandler> pInvalidCertHandler = new ConsoleCertificateHandler;
		///     Context::Ptr pContext = new Context(Context::CLIENT_USE, "");
		///     SSLManager::instance().initializeClient(pInvalidCertHandler, pContext);

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
	SecurityFunctionTableW& securityFunctions();

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

	void loadSecurityLibrary();
		/// Loads the Windows security DLL.

	void unloadSecurityLibrary();
		/// Unloads the Windows security DLL.

	static Poco::Util::AbstractConfiguration& appConfig();
		/// Returns the application configuration.
		///
		/// Throws a InvalidStateException if not application instance
		/// is available.

	HMODULE _hSecurityModule;
	SecurityFunctionTableW _securityFunctions;

	PrivateKeyFactoryMgr           _factoryMgr;
	CertificateHandlerFactoryMgr   _certHandlerFactoryMgr;
	Context::Ptr                   _ptrDefaultServerContext;
	PrivateKeyPassphraseHandlerPtr _ptrServerPassphraseHandler;
	InvalidCertificateHandlerPtr   _ptrServerCertificateHandler;
	Context::Ptr                   _ptrDefaultClientContext;
	PrivateKeyPassphraseHandlerPtr _ptrClientPassphraseHandler;
	InvalidCertificateHandlerPtr   _ptrClientCertificateHandler;
	Poco::FastMutex _mutex;

	static const std::string CFG_CERT_NAME;
	static const std::string VAL_CERT_NAME;
	static const std::string CFG_CERT_PATH;
	static const std::string VAL_CERT_PATH;
	static const std::string CFG_CERT_STORE;
	static const std::string VAL_CERT_STORE;
	static const std::string CFG_VER_MODE;
	static const Context::VerificationMode VAL_VER_MODE;
	static const std::string CFG_REVOCATION_CHECK;
	static const bool VAL_REVOCATION_CHECK;
	static const std::string CFG_TRUST_ROOTS;
	static const bool VAL_TRUST_ROOTS;
	static const std::string CFG_USE_MACHINE_STORE;
	static const bool VAL_USE_MACHINE_STORE;
	static const std::string CFG_USE_STRONG_CRYPTO;
	static const bool VAL_USE_STRONG_CRYPTO;

	static const std::string CFG_DELEGATE_HANDLER;
	static const std::string VAL_DELEGATE_HANDLER;
	static const std::string CFG_CERTIFICATE_HANDLER;
	static const std::string VAL_CERTIFICATE_HANDLER;

	static const std::string CFG_REQUIRE_TLSV1;
	static const std::string CFG_REQUIRE_TLSV1_1;
	static const std::string CFG_REQUIRE_TLSV1_2;

	friend class Poco::SingletonHolder<SSLManager>;
	friend class SecureSocketImpl;
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


inline SecurityFunctionTableW& SSLManager::securityFunctions()
{
	return _securityFunctions;
}


} } // namespace Poco::Net


#endif // NetSSL_SSLManager_INCLUDED
