#pragma once

#include "config_formats.h"
#if USE_CAPNP

#include <Core/Block.h>
#include <Formats/CapnProtoUtils.h>
#include <Processors/Formats/IRowInputFormat.h>

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
class CapnProtoRowInputFormat : public IRowInputFormat
{
public:
    CapnProtoRowInputFormat(ReadBuffer & in_, Block header, Params params_, const FormatSchemaInfo & info, const FormatSettings & format_settings_);

    String getName() const override { return "CapnProtoRowInputFormat"; }

    bool readRow(MutableColumns & columns, RowReadExtension &) override;

private:
    kj::Array<capnp::word> readMessage();

    std::shared_ptr<CapnProtoSchemaParser> parser;
    capnp::StructSchema root;
    const FormatSettings format_settings;
    DataTypes column_types;
    Names column_names;
};

}

#endif // USE_CAPNP
