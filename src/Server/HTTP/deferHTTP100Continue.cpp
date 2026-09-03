#include <Server/HTTP/deferHTTP100Continue.h>


namespace DB
{

bool shouldDeferHTTP100Continue(const HTTPRequest & request)
{
    const std::string& header_value = request.get("X-ClickHouse-100-Continue", Poco::Net::HTTPMessage::EMPTY);
    return !header_value.empty() && Poco::icompare(header_value, "defer") == 0;
}

}
