#pragma once

#if USE_CAPNP

#include <Core/Block.h>
#include <capnp/dynamic.h>
#include <Formats/FormatSettings.h>

namespace DB
{

class CapnProtoSerializer
{
public:
    CapnProtoSerializer(const DataTypes & data_types, const Names & names, const capnp::StructSchema & schema, const FormatSettings::CapnProto & settings);

    void writeRow(const Columns & columns, capnp::DynamicStruct::Builder builder, size_t row_num);

    void readRow(MutableColumns & columns, capnp::DynamicStruct::Reader & reader);

    ~CapnProtoSerializer();

private:
    class Impl;
    std::unique_ptr<Impl> serializer_impl;
};

}

#endif
