#include "ExternalTablesHandler.h"

#include <Poco/Net/MessageHeader.h>
#include <Interpreters/Context.h>
#include <IO/ReadBufferFromIStream.h> 
#include <IO/LimitReadBuffer.h>
#include <Storages/StorageMemory.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
}

void ExternalTablesHandler::handlePart(const Poco::Net::MessageHeader & header, std::istream & stream)
{
    const Settings & settings = context.getSettingsRef();

    /// The buffer is initialized here, not in the virtual function initReadBuffer
    read_buffer_impl = std::make_unique<ReadBufferFromIStream>(stream);

    if (settings.http_max_multipart_form_data_size)
        read_buffer = std::make_unique<LimitReadBuffer>(
            *read_buffer_impl, settings.http_max_multipart_form_data_size,
            true, "the maximum size of multipart/form-data. This limit can be tuned by 'http_max_multipart_form_data_size' setting");
    else
        read_buffer = std::move(read_buffer_impl);

    /// Retrieve a collection of parameters from MessageHeader
    Poco::Net::NameValueCollection content;
    std::string label;
    Poco::Net::MessageHeader::splitParameters(header.get("Content-Disposition"), label, content);

    /// Get parameters
    name = content.get("name", "_data");
    format = params.get(name + "_format", "TabSeparated");

    if (params.has(name + "_structure"))
        parseStructureFromStructureField(params.get(name + "_structure"));
    else if (params.has(name + "_types"))
        parseStructureFromTypesField(params.get(name + "_types"));
    else
        throw Exception("Neither structure nor types have not been provided for external table " + name + ". Use fields " + name + "_structure or " + name + "_types to do so.", ErrorCodes::BAD_ARGUMENTS);

    ExternalTableData data = getData(context);

    /// Create table
    NamesAndTypesList columns = sample_block.getNamesAndTypesList();
    StoragePtr storage = StorageMemory::create(data.second, ColumnsDescription{columns});
    storage->startup();
    context.addExternalTable(data.second, storage);
    BlockOutputStreamPtr output = storage->write(ASTPtr(), settings);

    /// Write data
    data.first->readPrefix();
    output->writePrefix();
    while (Block block = data.first->read())
        output->write(block);
    data.first->readSuffix();
    output->writeSuffix();

    /// We are ready to receive the next file, for this we clear all the information received
    clean();
}

}
