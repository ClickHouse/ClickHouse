#include <DB/IO/HTTPCommon.h>

#include <Poco/Net/AcceptCertificateHandler.h>
#include <Poco/Net/Context.h>
#include <Poco/Net/HTTPServerResponse.h>
#include <Poco/Net/InvalidCertificateHandler.h>
#include <Poco/Net/PrivateKeyPassphraseHandler.h>
#include <Poco/Net/RejectCertificateHandler.h>
#include <Poco/Net/SSLManager.h>
#include <Poco/Util/Application.h>
#include <Poco/Util/Application.h>
#include <Poco/Version.h>


namespace DB
{
void setResponseDefaultHeaders(Poco::Net::HTTPServerResponse & response)
{
	if (!response.getKeepAlive())
		return;

	Poco::Timespan keep_alive_timeout(Poco::Util::Application::instance().config().getInt("keep_alive_timeout", 10), 0);
	if (keep_alive_timeout.totalSeconds())
		response.set("Keep-Alive", "timeout=" + std::to_string(keep_alive_timeout.totalSeconds()));
}

std::once_flag client_ssl_init_once;

void clientSSLInit()
{
	// http://stackoverflow.com/questions/18315472/https-request-in-c-using-poco
	Poco::Net::initializeSSL();
	bool insecure = Poco::Util::Application::instance().config().getBool("https_client_insecure", false);
	Poco::SharedPtr<Poco::Net::InvalidCertificateHandler> ptr_handler(insecure
			? dynamic_cast<Poco::Net::InvalidCertificateHandler *>(new Poco::Net::AcceptCertificateHandler(true))
			: dynamic_cast<Poco::Net::InvalidCertificateHandler *>(new Poco::Net::RejectCertificateHandler(true)));
	Poco::Net::Context::Params ssl_params;
	ssl_params.verificationMode = insecure ? Poco::Net::Context::VERIFY_NONE : Poco::Net::Context::VERIFY_RELAXED;
	Poco::Net::Context::Ptr ptr_context(new Poco::Net::Context(Poco::Net::Context::CLIENT_USE, ssl_params));
	ptr_context->enableSessionCache(true);
#if POCO_VERSION >= 0x01070000
	ptr_context->disableProtocols(Poco::Net::Context::PROTO_SSLV2 | Poco::Net::Context::PROTO_SSLV3);
	ptr_context->preferServerCiphers();
#endif
	Poco::Net::SSLManager::instance().initializeClient(nullptr, ptr_handler, ptr_context);
}
}
