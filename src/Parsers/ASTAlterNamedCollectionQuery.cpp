#include <algorithm>
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
    return make_intrusive<ASTAlterNamedCollectionQuery>(*this);
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
                writeChar('{', buf);
                for (size_t i = 0; i < changes.size(); ++i)
                {
                    if (i != 0)
                        writeChar(',', buf);
                    builder.writeStringValue(changes[i].name);
                    writeCString(": ", buf);
                    builder.writeStringValue(changes[i].value.dump());
                }
                writeChar('}', buf);
            });
    }

    if (!overridability.empty())
    {
        builder.writeField(
            "overridability",
            [&]()
            {
                /// `overridability` is an unordered_map; sort by key so the output is deterministic.
                std::vector<std::string> keys;
                keys.reserve(overridability.size());
                for (const auto & [key, value] : overridability)
                    keys.push_back(key);
                std::sort(keys.begin(), keys.end());

                writeChar('{', buf);
                for (size_t i = 0; i < keys.size(); ++i)
                {
                    if (i != 0)
                        writeChar(',', buf);
                    builder.writeStringValue(keys[i]);
                    writeCString(": ", buf);
                    writeCString(overridability.at(keys[i]) ? "true" : "false", buf);
                }
                writeChar('}', buf);
            });
    }

    builder.endObject();
}
}
