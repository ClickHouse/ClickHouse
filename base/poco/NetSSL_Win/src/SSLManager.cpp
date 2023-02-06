//
// SSLManager.cpp
//
// Library: NetSSL_Win
// Package: SSLCore
// Module:  SSLManager
//
// Copyright (c) 2006-2014, Applied Informatics Software Engineering GmbH.
// and Contributors.
//
// SPDX-License-Identifier:	BSL-1.0
//


#include "Poco/Net/SSLManager.h"
#include "Poco/Net/Context.h"
#include "Poco/Net/Utility.h"
#include "Poco/Net/PrivateKeyPassphraseHandler.h"
#include "Poco/Net/RejectCertificateHandler.h"
#include "Poco/SingletonHolder.h"
#include "Poco/Delegate.h"
#include "Poco/Util/Application.h"
#include "Poco/Util/OptionException.h"


namespace Poco {
namespace Net {


const std::string SSLManager::CFG_CERT_NAME("certificateName");
const std::string SSLManager::VAL_CERT_NAME("");
const std::string SSLManager::CFG_CERT_PATH("certificatePath");
const std::string SSLManager::VAL_CERT_PATH("");
const std::string SSLManager::CFG_CERT_STORE("certificateStore");
const std::string SSLManager::VAL_CERT_STORE("MY");
const std::string SSLManager::CFG_VER_MODE("verificationMode");
const Context::VerificationMode SSLManager::VAL_VER_MODE(Context::VERIFY_RELAXED);
const std::string SSLManager::CFG_REVOCATION_CHECK("revocationChecking");
const bool SSLManager::VAL_REVOCATION_CHECK(true);
const std::string SSLManager::CFG_TRUST_ROOTS("trustRoots");
const bool SSLManager::VAL_TRUST_ROOTS(true);
const std::string SSLManager::CFG_USE_MACHINE_STORE("useMachineStore");
const bool SSLManager::VAL_USE_MACHINE_STORE(false);
const std::string SSLManager::CFG_USE_STRONG_CRYPTO("useStrongCrypto");
const bool SSLManager::VAL_USE_STRONG_CRYPTO(true);
const std::string SSLManager::CFG_DELEGATE_HANDLER("privateKeyPassphraseHandler.name");
const std::string SSLManager::VAL_DELEGATE_HANDLER("KeyConsoleHandler");
const std::string SSLManager::CFG_CERTIFICATE_HANDLER("invalidCertificateHandler.name");
const std::string SSLManager::VAL_CERTIFICATE_HANDLER("ConsoleCertificateHandler");
const std::string SSLManager::CFG_SERVER_PREFIX("schannel.server.");
const std::string SSLManager::CFG_CLIENT_PREFIX("schannel.client.");
const std::string SSLManager::CFG_REQUIRE_TLSV1("requireTLSv1");
const std::string SSLManager::CFG_REQUIRE_TLSV1_1("requireTLSv1_1");
const std::string SSLManager::CFG_REQUIRE_TLSV1_2("requireTLSv1_2");


SSLManager::SSLManager():
	_hSecurityModule(0)
{
	loadSecurityLibrary();
}


SSLManager::~SSLManager()
{
	try
	{
		shutdown();

	}
	catch (...)
	{
		poco_unexpected();
	}
}


namespace
{
	static Poco::SingletonHolder<SSLManager> singleton;
}


SSLManager& SSLManager::instance()
{
	return *singleton.get();
}


void SSLManager::initializeServer(PrivateKeyPassphraseHandlerPtr pPassphraseHandler, InvalidCertificateHandlerPtr pCertHandler, Context::Ptr pContext)
{
	_ptrServerPassphraseHandler  = pPassphraseHandler;
	_ptrServerCertificateHandler = pCertHandler;
	_ptrDefaultServerContext     = pContext;
}


void SSLManager::initializeClient(PrivateKeyPassphraseHandlerPtr pPassphraseHandler, InvalidCertificateHandlerPtr pCertHandler, Context::Ptr pContext)
{
	_ptrClientPassphraseHandler  = pPassphraseHandler;
	_ptrClientCertificateHandler = pCertHandler;
	_ptrDefaultClientContext     = pContext;
}


Context::Ptr SSLManager::defaultServerContext()
{
	Poco::FastMutex::ScopedLock lock(_mutex);

	if (!_ptrDefaultServerContext)
		initDefaultContext(true);

	return _ptrDefaultServerContext;
}


Context::Ptr SSLManager::defaultClientContext()
{
	Poco::FastMutex::ScopedLock lock(_mutex);

	if (!_ptrDefaultClientContext)
	{
		try
		{
			initDefaultContext(false);
		} 
		catch (Poco::IllegalStateException&)
		{
			_ptrClientCertificateHandler = new RejectCertificateHandler(false);
			_ptrDefaultClientContext = new Context(Context::CLIENT_USE, "");
		}
	}

	return _ptrDefaultClientContext;
}


SSLManager::PrivateKeyPassphraseHandlerPtr SSLManager::serverPassphraseHandler()
{
	Poco::FastMutex::ScopedLock lock(_mutex);

	if (!_ptrServerPassphraseHandler)
		initPassphraseHandler(true);

	return _ptrServerPassphraseHandler;
}


SSLManager::PrivateKeyPassphraseHandlerPtr SSLManager::clientPassphraseHandler()
{
	Poco::FastMutex::ScopedLock lock(_mutex);

	if (!_ptrClientPassphraseHandler)
		initPassphraseHandler(false);

	return _ptrClientPassphraseHandler;
}


SSLManager::InvalidCertificateHandlerPtr SSLManager::serverCertificateHandler()
{
	Poco::FastMutex::ScopedLock lock(_mutex);

	if (!_ptrServerCertificateHandler)
		initCertificateHandler(true);

	return _ptrServerCertificateHandler;
}


SSLManager::InvalidCertificateHandlerPtr SSLManager::clientCertificateHandler()
{
	Poco::FastMutex::ScopedLock lock(_mutex);

	if (!_ptrClientCertificateHandler)
		initCertificateHandler(false);

	return _ptrClientCertificateHandler;
}


void SSLManager::initDefaultContext(bool server)
{
	if (server && _ptrDefaultServerContext) return;
	if (!server && _ptrDefaultClientContext) return;

	initEvents(server);

	const std::string prefix = server ? CFG_SERVER_PREFIX : CFG_CLIENT_PREFIX;
	Poco::Util::AbstractConfiguration& config = appConfig();
	std::string certName = config.getString(prefix + CFG_CERT_NAME, VAL_CERT_NAME);
	std::string certPath = config.getString(prefix + CFG_CERT_PATH, VAL_CERT_PATH);
	std::string certStore = config.getString(prefix + CFG_CERT_STORE, VAL_CERT_STORE);

	bool requireTLSv1 = config.getBool(prefix + CFG_REQUIRE_TLSV1, false);
	bool requireTLSv1_1 = config.getBool(prefix + CFG_REQUIRE_TLSV1_1, false);
	bool requireTLSv1_2 = config.getBool(prefix + CFG_REQUIRE_TLSV1_2, false);

	// optional options for which we have defaults defined
	Context::VerificationMode verMode = VAL_VER_MODE;
	if (config.hasProperty(prefix + CFG_VER_MODE))
	{
		// either: none, relaxed, strict, once
		std::string mode = config.getString(prefix + CFG_VER_MODE);
		verMode = Utility::convertVerificationMode(mode);
	}
	bool revocChecking = config.getBool(prefix + CFG_REVOCATION_CHECK, VAL_REVOCATION_CHECK);
	bool trustRoots = config.getBool(prefix + CFG_TRUST_ROOTS, VAL_TRUST_ROOTS);
	bool useMachineStore = config.getBool(prefix + CFG_USE_MACHINE_STORE, VAL_USE_MACHINE_STORE);
	bool useStrongCrypto = config.getBool(prefix + CFG_USE_STRONG_CRYPTO, VAL_USE_STRONG_CRYPTO);

	int options = 0;
	if (revocChecking) options |= Context::OPT_PERFORM_REVOCATION_CHECK;
	if (trustRoots) options |= Context::OPT_TRUST_ROOTS_WIN_CERT_STORE;
	if (useMachineStore) options |= Context::OPT_USE_MACHINE_STORE;
	if (useStrongCrypto) options |= Context::OPT_USE_STRONG_CRYPTO;
	if (!certPath.empty()) 
	{
		options |= Context::OPT_LOAD_CERT_FROM_FILE;
		certName = certPath;
	}

	Context::Usage usage;
	if (server)
	{
		if (requireTLSv1_2)
			usage = Context::TLSV1_2_SERVER_USE;
		else if (requireTLSv1_1)
			usage = Context::TLSV1_1_SERVER_USE;
		else if (requireTLSv1)
			usage = Context::TLSV1_SERVER_USE;
		else
			usage = Context::SERVER_USE;
		_ptrDefaultServerContext = new Context(usage, certName, verMode, options, certStore);
	}
	else
	{
		if (requireTLSv1_2)
			usage = Context::TLSV1_2_CLIENT_USE;
		else if (requireTLSv1_1)
			usage = Context::TLSV1_1_CLIENT_USE;
		else if (requireTLSv1)
			usage = Context::TLSV1_CLIENT_USE;
		else
			usage = Context::CLIENT_USE;
		_ptrDefaultClientContext = new Context(usage, certName, verMode, options, certStore);
	}
}


void SSLManager::initEvents(bool server)
{
	initPassphraseHandler(server);
	initCertificateHandler(server);
}


void SSLManager::initPassphraseHandler(bool server)
{
	if (server && _ptrServerPassphraseHandler) return;
	if (!server && _ptrClientPassphraseHandler) return;
	
	std::string prefix = server ? CFG_SERVER_PREFIX : CFG_CLIENT_PREFIX;
	Poco::Util::AbstractConfiguration& config = appConfig();

	std::string className(config.getString(prefix + CFG_DELEGATE_HANDLER, VAL_DELEGATE_HANDLER));

	const PrivateKeyFactory* pFactory = 0;
	if (privateKeyFactoryMgr().hasFactory(className))
	{
		pFactory = privateKeyFactoryMgr().getFactory(className);
	}

	if (pFactory)
	{
		if (server)
			_ptrServerPassphraseHandler = pFactory->create(server);
		else
			_ptrClientPassphraseHandler = pFactory->create(server);
	}
	else throw Poco::Util::UnknownOptionException(std::string("No passphrase handler known with the name ") + className);
}


void SSLManager::initCertificateHandler(bool server)
{
	if (server && _ptrServerCertificateHandler) return;
	if (!server && _ptrClientCertificateHandler) return;

	std::string prefix = server ? CFG_SERVER_PREFIX : CFG_CLIENT_PREFIX;
	Poco::Util::AbstractConfiguration& config = appConfig();

	std::string className(config.getString(prefix + CFG_CERTIFICATE_HANDLER, VAL_CERTIFICATE_HANDLER));

	const CertificateHandlerFactory* pFactory = 0;
	if (certificateHandlerFactoryMgr().hasFactory(className))
	{
		pFactory = certificateHandlerFactoryMgr().getFactory(className);
	}

	if (pFactory)
	{
		if (server)
			_ptrServerCertificateHandler = pFactory->create(true);
		else
			_ptrClientCertificateHandler = pFactory->create(false);
	}
	else throw Poco::Util::UnknownOptionException("No InvalidCertificate handler known with the name", className);
}


void SSLManager::shutdown()
{
	ClientVerificationError.clear();
	ServerVerificationError.clear();
	_ptrServerPassphraseHandler  = 0;
	_ptrServerCertificateHandler = 0;
	_ptrDefaultServerContext     = 0;
	_ptrClientPassphraseHandler  = 0;
	_ptrClientCertificateHandler = 0;
	_ptrDefaultClientContext     = 0;

	unloadSecurityLibrary();
}


void SSLManager::loadSecurityLibrary()
{
	if (_hSecurityModule) return;

	OSVERSIONINFO VerInfo;
	std::wstring dllPath;

	// Find out which security DLL to use, depending on
	// whether we are on Win2k, NT or Win9x

	VerInfo.dwOSVersionInfoSize = sizeof(OSVERSIONINFO);
	if (!GetVersionEx(&VerInfo))
		throw Poco::SystemException("Cannot determine OS version");

#if defined(_WIN32_WCE)
	dllPath = L"Secur32.dll";
#else
	if (VerInfo.dwPlatformId == VER_PLATFORM_WIN32_NT 
		&& VerInfo.dwMajorVersion == 4)
	{
		dllPath = L"Security.dll";
	}
	else if (VerInfo.dwPlatformId == VER_PLATFORM_WIN32_WINDOWS ||
		VerInfo.dwPlatformId == VER_PLATFORM_WIN32_NT )
	{
		dllPath = L"Secur32.dll";
	}
	else
	{
		throw Poco::SystemException("Cannot determine which security DLL to use");
	}
#endif

	//
	//  Load Security DLL
	//

	_hSecurityModule = LoadLibraryW(dllPath.c_str());
	if(_hSecurityModule == 0)
	{
		throw Poco::SystemException("Failed to load security DLL");
	}

#if defined(_WIN32_WCE)
	INIT_SECURITY_INTERFACE pInitSecurityInterface = (INIT_SECURITY_INTERFACE)GetProcAddressW( _hSecurityModule, L"InitSecurityInterfaceW");
#else
	INIT_SECURITY_INTERFACE pInitSecurityInterface = (INIT_SECURITY_INTERFACE)GetProcAddress( _hSecurityModule, "InitSecurityInterfaceW");
#endif

	if (!pInitSecurityInterface)
	{
		FreeLibrary(_hSecurityModule);
		_hSecurityModule = 0;
		throw Poco::SystemException("Failed to initialize security DLL (no init function)");
	}

	PSecurityFunctionTable pSecurityFunc = pInitSecurityInterface();
	if (!pSecurityFunc)
	{
		FreeLibrary(_hSecurityModule);
		_hSecurityModule = 0;
		throw Poco::SystemException("Failed to initialize security DLL (no function table)");
	}

	CopyMemory(&_securityFunctions, pSecurityFunc, sizeof(_securityFunctions));
}


void SSLManager::unloadSecurityLibrary()
{
	if (_hSecurityModule)
	{
		FreeLibrary(_hSecurityModule);
		_hSecurityModule = 0;
	}
}


Poco::Util::AbstractConfiguration& SSLManager::appConfig()
{
	try
	{
		return Poco::Util::Application::instance().config();
	}
	catch (Poco::NullPointerException&)
	{
		throw Poco::IllegalStateException(
			"An application configuration is required to initialize the Poco::Net::SSLManager, "
			"but no Poco::Util::Application instance is available."
		);
	}
}


void initializeSSL()
{
}
	

void uninitializeSSL()
{
	SSLManager::instance().shutdown();
}


} } // namespace Poco::Net
