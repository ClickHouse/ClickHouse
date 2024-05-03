#pragma once

#include <Core/NamesAndTypes.h>
#include <DataTypes/NestedUtils.h>
#include <DataTypes/DataTypeTuple.h>

namespace DB
{

namespace StructureToFormatSchemaUtils
{
    void writeIndent(WriteBuffer & buf, size_t indent);

    void startNested(WriteBuffer & buf, const String & nested_name, const String & nested_type, size_t indent);

    void endNested(WriteBuffer & buf, size_t indent);

    String getSchemaFieldName(const String & column_name);

    String getSchemaMessageName(const String & column_name);

    NamesAndTypesList collectNested(const NamesAndTypesList & names_and_types);

    NamesAndTypesList getCollectedTupleElements(const DataTypeTuple & tuple_type);
}

}
