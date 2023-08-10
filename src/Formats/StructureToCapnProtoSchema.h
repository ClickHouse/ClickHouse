#pragma once

#include <IO/WriteBuffer.h>
#include <Core/NamesAndTypes.h>

namespace DB
{

struct StructureToCapnProtoSchema
{
    static constexpr auto name = "structureToCapnProtoSchema";

    static void writeSchema(WriteBuffer & buf, const String & message_name, const NamesAndTypesList & names_and_types_);
};

}
