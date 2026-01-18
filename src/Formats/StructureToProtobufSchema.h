#pragma once

#include <IO/WriteBuffer.h>
#include <Core/NamesAndTypes.h>

namespace DB
{

struct StructureToProtobufSchema
{
    static constexpr auto name = "structureToProtobufSchema";

    static void
    writeSchema(WriteBuffer & buf, const String & message_name, const NamesAndTypesList & names_and_types_, bool with_envelope = false);
};

}
