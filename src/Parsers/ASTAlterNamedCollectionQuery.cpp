#include <IO/Operators.h>
#include <Parsers/ASTAlterNamedCollectionQuery.h>
#include <Parsers/JSONObjectBuilder.h>
#include <Parsers/formatSettingName.h>
#include <base/types.h>
#include <Common/FieldVisitorToString.h>
#include <Common/quoteString.h>

namespace DB
{

ASTPtr ASTAlterNamedCollectionQuery::clone() const
{
    return std::make_shared<ASTAlterNamedCollectionQuery>(*this);
}

void ASTAlterNamedCollectionQuery::formatImpl(WriteBuffer & ostr, const IAST::FormatSettings & settings, IAST::FormatState &, IAST::FormatStateStacked) const
{
    ostr << "ALTER NAMED COLLECTION ";
    if (if_exists)
        ostr << "IF EXISTS ";
    ostr << backQuoteIfNeed(collection_name);
    formatOnCluster(ostr, settings);
    if (!changes.empty())
    {
        ostr << " SET ";
        bool first = true;
        for (const auto & change : changes)
        {
            if (!first)
                ostr << ", ";
            else
                first = false;

            formatSettingName(change.name, ostr);
            if (settings.show_secrets)
                ostr << " = " << applyVisitor(FieldVisitorToString(), change.value);
            else
                ostr << " = '[HIDDEN]'";
            auto override_value = overridability.find(change.name);
            if (override_value != overridability.end())
                ostr << " " << (override_value->second ? "" : "NOT ") << "OVERRIDABLE";
        }
    }
    if (!delete_keys.empty())
    {
        ostr << " DELETE ";
        bool first = true;
        for (const auto & key : delete_keys)
        {
            if (!first)
                ostr << ", ";
            else
                first = false;

            formatSettingName(key, ostr);
        }
    }
}

void ASTAlterNamedCollectionQuery::writeJSON(WriteBuffer & buf, size_t indent) const
{
    JSONObjectBuilder builder(buf, indent);
    builder.startObject(getID(' '));

    builder.writeField("collection_name", collection_name);
    builder.writeField("if_exists", if_exists);
    builder.writeArray("DELETE", delete_keys, delete_keys.size() > 0);

    if (!changes.empty())
    {
        builder.writeField(
            "changes",
            [&]()
            {
                WriteBufferFromOwnString obj_buf;

                size_t outer_indent = builder.getIndent();
                buf.write(String(outer_indent * 2, ' ').c_str(), outer_indent * 2);
                writeChar('{', buf);
                writeChar('\n', buf);
                String inner_indent = String((outer_indent + 1) * 2, ' ');

                for (size_t i = 0; i < changes.size(); ++i)
                {
                    obj_buf.write(inner_indent.c_str(), inner_indent.size());
                    obj_buf.write('"');
                    obj_buf.write(changes[i].name.c_str(), changes[i].name.size());
                    obj_buf.write("\": ", 2);

                    obj_buf.write('"');
                    String val_as_str = changes[i].value.dump();
                    obj_buf.write(val_as_str.c_str(), val_as_str.size());
                    obj_buf.write('"');

                    if (i < changes.size() - 1)
                        obj_buf.write(",\n", 1);
                    else
                        obj_buf.write('\n');
                }

                // Close object with proper indent
                obj_buf.write(String((builder.getIndent()) * 2, ' ').c_str(), builder.getIndent() * 2);
                obj_buf.write('}');

                buf.write(obj_buf.str().c_str(), obj_buf.str().size());
            });
    }

    if (!overridability.empty())
    {
        builder.writeField(
            "overridability",
            [&]()
            {
                WriteBufferFromOwnString obj_buf;

                size_t outer_indent = builder.getIndent();
                buf.write(String(outer_indent * 2, ' ').c_str(), outer_indent * 2);
                writeChar('{', buf);
                writeChar('\n', buf);
                String inner_indent = String((outer_indent + 1) * 2, ' ');

                size_t count = 0;
                for (const auto & [key, value] : overridability)
                {
                    obj_buf.write(inner_indent.c_str(), inner_indent.size());
                    obj_buf.write('"');
                    obj_buf.write(key.c_str(), key.size());
                    obj_buf.write("\": ", 2);
                    String val_as_str = value ? "true" : "false";
                    obj_buf.write(val_as_str.c_str(), val_as_str.size());

                    if (count < overridability.size() - 1)
                        obj_buf.write(",\n", 1);
                    else
                        obj_buf.write('\n');
                    ++count;
                }

                // Close object with proper indent
                obj_buf.write(String((builder.getIndent()) * 2, ' ').c_str(), builder.getIndent() * 2);
                obj_buf.write('}');

                buf.write(obj_buf.str().c_str(), obj_buf.str().size());
            });
    }

    builder.endObject();
}
}
