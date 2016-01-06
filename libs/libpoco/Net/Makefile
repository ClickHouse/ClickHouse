#
# Makefile
#
# $Id: //poco/1.4/Net/Makefile#3 $
#
# Makefile for Poco Net
#

include $(POCO_BASE)/build/rules/global

SHAREDOPT_CXX += -DNet_EXPORTS

objects = \
	Net DNS HTTPResponse HostEntry Socket \
	DatagramSocket HTTPServer IPAddress IPAddressImpl SocketAddress SocketAddressImpl \
	HTTPBasicCredentials HTTPCookie HTMLForm MediaType DialogSocket \
	DatagramSocketImpl FilePartSource HTTPServerConnection MessageHeader \
	HTTPChunkedStream HTTPServerConnectionFactory MulticastSocket SocketStream \
	HTTPClientSession HTTPServerParams MultipartReader StreamSocket SocketImpl \
	HTTPFixedLengthStream HTTPServerRequest HTTPServerRequestImpl MultipartWriter StreamSocketImpl \
	HTTPHeaderStream HTTPServerResponse HTTPServerResponseImpl NameValueCollection TCPServer \
	HTTPMessage HTTPServerSession NetException TCPServerConnection HTTPBufferAllocator \
	HTTPAuthenticationParams HTTPCredentials HTTPDigestCredentials \
	HTTPRequest HTTPSession HTTPSessionInstantiator HTTPSessionFactory NetworkInterface  \
	HTTPRequestHandler HTTPStream HTTPIOStream ServerSocket TCPServerDispatcher TCPServerConnectionFactory \
	HTTPRequestHandlerFactory HTTPStreamFactory ServerSocketImpl TCPServerParams \
	QuotedPrintableEncoder QuotedPrintableDecoder StringPartSource \
	FTPClientSession FTPStreamFactory PartHandler PartSource PartStore NullPartHandler \
	SocketReactor SocketNotifier SocketNotification AbstractHTTPRequestHandler \
	MailRecipient MailMessage MailStream SMTPClientSession POP3ClientSession \
	RawSocket RawSocketImpl ICMPClient ICMPEventArgs ICMPPacket ICMPPacketImpl \
	ICMPSocket ICMPSocketImpl ICMPv4PacketImpl \
	NTPClient NTPEventArgs NTPPacket \
	RemoteSyslogChannel RemoteSyslogListener SMTPChannel \
	WebSocket WebSocketImpl \
	OAuth10Credentials OAuth20Credentials

target         = PocoNet
target_version = $(LIBVERSION)
target_libs    = PocoFoundation

include $(POCO_BASE)/build/rules/lib
