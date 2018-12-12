#include <Misc/ExternalTable.h>
#include <Poco/Net/PartHandler.h>

namespace DB
{

class Context;

/// Parsing of external table used when sending tables via http
/// The `handlePart` function will be called for each table passed,
/// so it's also necessary to call `clean` at the end of the `handlePart`.
class ExternalTablesHandler : public Poco::Net::PartHandler, BaseExternalTable
{
public:
    ExternalTablesHandler(Context & context_, const Poco::Net::NameValueCollection & params_) : context(context_), params(params_) {}

    void handlePart(const Poco::Net::MessageHeader & header, std::istream & stream);

private:
    Context & context;
    const Poco::Net::NameValueCollection & params;
    std::unique_ptr<ReadBuffer> read_buffer_impl;
};

}
