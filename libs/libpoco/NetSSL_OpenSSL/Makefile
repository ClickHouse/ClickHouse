#
# Makefile
#
# $Id: //poco/Main/template/library.make#2 $
#
# Makefile for Poco NetSSL_OpenSSL
#

include $(POCO_BASE)/build/rules/global

SYSLIBS += -lssl -lcrypto

objects = AcceptCertificateHandler RejectCertificateHandler ConsoleCertificateHandler \
	CertificateHandlerFactory CertificateHandlerFactoryMgr \
	Context HTTPSClientSession HTTPSStreamFactory HTTPSSessionInstantiator \
	InvalidCertificateHandler KeyConsoleHandler \
	KeyFileHandler PrivateKeyFactory PrivateKeyFactoryMgr \
	PrivateKeyPassphraseHandler SecureServerSocket SecureServerSocketImpl \
	SecureSocketImpl SecureStreamSocket SecureStreamSocketImpl \
	SSLException SSLManager Utility VerificationErrorArgs \
	X509Certificate Session SecureSMTPClientSession

target         = PocoNetSSL
target_version = $(LIBVERSION)
target_libs    = PocoNet PocoCrypto PocoUtil PocoFoundation 

include $(POCO_BASE)/build/rules/lib
