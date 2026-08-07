#pragma once

#include <Client/Connection.h>
#include <Core/Block.h>
#include <Interpreters/Context_fwd.h>
#include <IO/ReadBuffer.h>
#include <Server/HTTP/HTMLForm.h>

#include <iosfwd>
#include <memory>
#include <string>
#include <vector>


namespace Poco::Net
{
class NameValueCollection;
class MessageHeader;
}

namespace boost::program_options
{
class variables_map;
}


namespace DB
{

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

    virtual ~BaseExternalTable() = default;

    /// Initialize read_buffer, depending on the data source. By default, does nothing.
    virtual void initReadBuffer() {}

    /// Get the table data - a pair (a stream with the contents of the table, the name of the table)
    ExternalTableDataPtr getData(ContextPtr context);

protected:
    /// Clear all accumulated information
    void clear();

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
    explicit ExternalTable(const boost::program_options::variables_map & external_options);
};


/// Parsing of external table used when sending tables via http
/// The `handlePart` function will be called for each table passed,
/// so it's also necessary to call `clean` at the end of the `handlePart`.
class ExternalTablesHandler : public HTMLForm::PartHandler, BaseExternalTable, WithMutableContext
{
public:
    ExternalTablesHandler(ContextMutablePtr context_, const Poco::Net::NameValueCollection & params_) : WithMutableContext(context_), params(params_) {}

    void handlePart(const Poco::Net::MessageHeader & header, ReadBuffer & stream) override;

private:
    const Poco::Net::NameValueCollection & params;
};


}
