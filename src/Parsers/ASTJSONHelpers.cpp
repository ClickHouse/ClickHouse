#include <Parsers/ASTJSONHelpers.h>
#include <Parsers/ASTWithAlias.h>
#include <Parsers/ASTQueryParameter.h>
#include <Core/Field.h>
#include <Common/FieldVisitorDump.h>

namespace DB
{

void JSONObjectWriter::writeAlias(const ASTWithAlias & node)
{
    if (!node.alias.empty())
        writeString("alias", node.alias);
    if (node.preferAliasToColumnName())
        writeBool("prefer_alias_to_column_name", true);
    if (node.parametrised_alias)
        writeChild("parametrised_alias", node.parametrised_alias.get());
}


static void writeFieldJSON(WriteBuffer & out, const FormatSettings & fs, const Field & field)
{
    out << "{\"field_type\":";
    writeJSONString(field.getTypeName(), out, fs);

    switch (field.getType())
    {
        case Field::Types::Null:
        {
            if (field == NEGATIVE_INFINITY)
            {
                out << ",\"value\":";
                writeJSONString("-Inf", out, fs);
            }
            else if (field == POSITIVE_INFINITY)
            {
                out << ",\"value\":";
                writeJSONString("+Inf", out, fs);
            }
            else
            {
                out << ",\"value\":null";
            }
            break;
        }
        case Field::Types::UInt64:
            out << ",\"value\":" << field.safeGet<UInt64>();
            break;
        case Field::Types::Int64:
            out << ",\"value\":" << field.safeGet<Int64>();
            break;
        case Field::Types::Float64:
            out << ",\"value\":";
            writeFloatText(field.safeGet<Float64>(), out);
            break;
        case Field::Types::Bool:
            out << ",\"value\":" << (field.safeGet<UInt64>() != 0 ? "true" : "false");
            break;
        case Field::Types::String:
            out << ",\"value\":";
            writeJSONString(field.safeGet<String>(), out, fs);
            break;
        case Field::Types::Array:
        {
            out << ",\"value\":[";
            const auto & arr = field.safeGet<Array>();
            for (size_t i = 0; i < arr.size(); ++i)
            {
                if (i > 0) out << ',';
                writeFieldJSON(out, fs, arr[i]);
            }
            out << ']';
            break;
        }
        case Field::Types::Tuple:
        {
            out << ",\"value\":[";
            const auto & tup = field.safeGet<Tuple>();
            for (size_t i = 0; i < tup.size(); ++i)
            {
                if (i > 0) out << ',';
                writeFieldJSON(out, fs, tup[i]);
            }
            out << ']';
            break;
        }
        case Field::Types::Map:
        {
            out << ",\"value\":[";
            const auto & map = field.safeGet<Map>();
            for (size_t i = 0; i < map.size(); ++i)
            {
                if (i > 0) out << ',';
                writeFieldJSON(out, fs, map[i]);
            }
            out << ']';
            break;
        }
        default:
        {
            /// For complex types (Decimal, UUID, IPv4, IPv6, Int128/256, etc.),
            /// use dump format which is round-trippable via Field::restoreFromDump.
            out << ",\"value\":";
            writeJSONString(applyVisitor(FieldVisitorDump(), field), out, fs);
            break;
        }
    }
    out << '}';
}


void JSONObjectWriter::writeFieldValue(const char * k, const Field & field)
{
    writeKey(k);
    writeFieldJSON(out, fs, field);
}

}
