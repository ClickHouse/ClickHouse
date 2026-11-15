#pragma once

#include <base/types.h>

namespace DB
{

class NamesAndTypesList;
class WriteBuffer;

struct StructureToCapnProtoSchema
{
    static constexpr auto name = "structureToCapnProtoSchema";

    static void
    writeSchema(WriteBuffer & buf, const String & message_name, const NamesAndTypesList & names_and_types_, bool with_envelope = false);
};

}
