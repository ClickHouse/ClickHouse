#pragma once

#include <Processors/Formats/IRowOutputFormat.h>
#include <Core/Block.h>

#include <Core/MySQL/Authentication.h>
#include <Core/MySQL/PacketsGeneric.h>
#include <Core/MySQL/PacketsConnection.h>
#include <Core/MySQL/PacketsProtocolText.h>
#include <Formats/FormatSettings.h>

namespace DB
{

class IColumn;
class IDataType;
class WriteBuffer;

/** A stream for outputting data in a binary line-by-line format.
  */
class MySQLOutputFormat final : public IOutputFormat, WithContext
{
public:
    MySQLOutputFormat(WriteBuffer & out_, const Block & header_, const FormatSettings & settings_);

    String getName() const override { return "MySQLOutputFormat"; }

    void setContext(ContextPtr context_);

    void consume(Chunk) override;
    void finalize() override;
    void flush() override;
    void doWritePrefix() override { initialize(); }

    void initialize();

private:
    bool initialized = false;

    std::optional<MySQLWireContext> own_mysql_context;
    MySQLWireContext * mysql_context = nullptr;
    MySQLProtocol::PacketEndpointPtr packet_endpoint;
    FormatSettings format_settings;
    DataTypes data_types;
    Serializations serializations;
};

}
