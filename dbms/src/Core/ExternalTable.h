#pragma once

#include <string>
#include <vector>
#include <memory>
#include <iosfwd>

#include <Poco/Net/PartHandler.h>

#include <Core/Block.h>
#include <Client/Connection.h>
#include <IO/ReadBuffer.h>


namespace Poco
{
    namespace Net
    {
        class NameValueCollection;
        class MessageHeader;
    }
}

namespace boost
{
    namespace program_options
    {
        class variables_map;
    }
}


namespace DB
{

class Context;


/// The base class containing the basic information about external table and
/// basic functions for extracting this information from text fields.
class BaseExternalTable
{
public:
    std::string file;       /// File with data or '-' if stdin
    std::string name;       /// The name of the table
    std::string format;     /// Name of the data storage format

    /// Description of the table structure: (column name, data type name)
    std::vector<std::pair<std::string, std::string>> structure;

    std::unique_ptr<ReadBuffer> read_buffer;
    Block sample_block;

    virtual ~BaseExternalTable() {}

    /// Initialize read_buffer, depending on the data source. By default, does nothing.
    virtual void initReadBuffer() {}

    /// Get the table data - a pair (a stream with the contents of the table, the name of the table)
    ExternalTableData getData(const Context & context);

protected:
    /// Clear all accumulated information
    void clean();

    /// Function for debugging information output
    void write();

    static std::vector<std::string> split(const std::string & s, const std::string & d);

    /// Construct the `structure` vector from the text field `structure`
    virtual void parseStructureFromStructureField(const std::string & argument);

    /// Construct the `structure` vector from the text field `types`
    virtual void parseStructureFromTypesField(const std::string & argument);

private:
    /// Initialize sample_block according to the structure of the table stored in the `structure`
    void initSampleBlock();
};


/// Parsing of external table used in the tcp client.
class ExternalTable : public BaseExternalTable
{
public:
    void initReadBuffer() override;

    /// Extract parameters from variables_map, which is built on the client command line
    ExternalTable(const boost::program_options::variables_map & external_options);
};


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
