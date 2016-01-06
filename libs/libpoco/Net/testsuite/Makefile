#
# Makefile
#
# $Id: //poco/1.4/Net/testsuite/Makefile#2 $
#
# Makefile for Poco Net testsuite
#

include $(POCO_BASE)/build/rules/global

objects = \
	DNSTest HTTPServerTestSuite MulticastSocketTest SocketStreamTest \
	DatagramSocketTest HTTPStreamFactoryTest MultipartReaderTest SocketTest \
	Driver HTTPTestServer MultipartWriterTest SocketsTestSuite \
	EchoServer HTTPTestSuite NameValueCollectionTest TCPServerTest \
	HTTPClientSessionTest IPAddressTest NetCoreTestSuite TCPServerTestSuite \
	HTTPRequestTest MessageHeaderTest NetTestSuite UDPEchoServer \
	HTTPResponseTest MessagesTestSuite NetworkInterfaceTest \
	HTTPServerTest MulticastEchoServer SocketAddressTest \
	HTTPCookieTest HTTPCredentialsTest HTMLFormTest HTMLTestSuite \
	MediaTypeTest QuotedPrintableTest DialogSocketTest \
	HTTPClientTestSuite FTPClientTestSuite FTPClientSessionTest \
	FTPStreamFactoryTest DialogServer \
	SocketReactorTest ReactorTestSuite \
	MailTestSuite MailMessageTest MailStreamTest \
	SMTPClientSessionTest POP3ClientSessionTest \
	RawSocketTest ICMPClientTest ICMPSocketTest ICMPClientTestSuite \
	NTPClientTest NTPClientTestSuite \
	WebSocketTest WebSocketTestSuite \
	SyslogTest \
	OAuth10CredentialsTest OAuth20CredentialsTest OAuthTestSuite

target         = testrunner
target_version = 1
target_libs    = PocoNet PocoFoundation CppUnit

include $(POCO_BASE)/build/rules/exec

ifdef POCO_UNBUNDLED
        SYSLIBS += -lz
endif
