#pragma once

#include "config.h"
#if USE_CAPNP

#include <Core/Block.h>
#include <Formats/CapnProtoSchema.h>
#include <Formats/CapnProtoSerializer.h>
#include <Processors/Formats/IRowInputFormat.h>
#include <Processors/Formats/ISchemaReader.h>

namespace DB
{

class FormatSchemaInfo;
class ReadBuffer;

/** A stream for reading messages in Cap'n Proto format in given schema.
  * Like Protocol Buffers and Thrift (but unlike JSON or MessagePack),
  * Cap'n Proto messages are strongly-typed and not self-describing.
  * The schema in this case cannot be compiled in, so it uses a runtime schema parser.
  * See https://capnproto.org/cxx.html
  */
class CapnProtoRowInputFormat final : public IRowInputFormat
{
public:
    CapnProtoRowInputFormat(ReadBuffer & in_, Block header, Params params_, const CapnProtoSchemaInfo & info, const FormatSettings & format_settings);

    String getName() const override { return "CapnProtoRowInputFormat"; }

private:
    bool readRow(MutableColumns & columns, RowReadExtension &) override;

    bool supportsCountRows() const override { return true; }
    size_t countRows(size_t max_block_size) override;

    std::pair<kj::Array<capnp::word>, size_t> readMessagePrefix();
    kj::Array<capnp::word> readMessage();
    void skipMessage();

    std::shared_ptr<CapnProtoSchemaParser> parser;
    capnp::StructSchema schema;
    std::unique_ptr<CapnProtoSerializer> serializer;
};

class CapnProtoSchemaReader : public IExternalSchemaReader
{
public:
    explicit CapnProtoSchemaReader(const FormatSettings & format_settings_);

    NamesAndTypesList readSchema() override;

private:
    const FormatSettings format_settings;
};

}

#endif // USE_CAPNP
