#pragma once

#include <Core/Block.h>
#include <Core/MySQL/PacketEndpoint.h>
#include <Interpreters/Context_fwd.h>
#include <Processors/Formats/IOutputFormat.h>
#include <Processors/Formats/IRowOutputFormat.h>

namespace DB
{

class IColumn;
class IDataType;
class WriteBuffer;
struct FormatSettings;

/** A stream for outputting data in a binary line-by-line format.
  */
class MySQLOutputFormat final : public IOutputFormat, WithContext
{
public:
    MySQLOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & settings_);

    String getName() const override { return "MySQLOutputFormat"; }

    void setContext(ContextPtr context_);

    void flush() override;

private:
    void consume(Chunk) override;
    void finalizeImpl() override;
    void writePrefix() override;

    uint32_t client_capabilities = 0;
    uint8_t * sequence_id = nullptr;
    uint8_t dummy_sequence_id = 0;
    MySQLProtocol::PacketEndpointPtr packet_endpoint;
    DataTypes data_types;
    Serializations serializations;
    bool use_binary_result_set = false;
};

}
