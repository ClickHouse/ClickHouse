#pragma once

#include <optional>
#include <base/types.h>

#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <Common/SipHash.h>
#include <IO/Operators.h>

namespace DB
{
    struct TemporaryReplaceTableName
    {
        String name_hash;
        String random_suffix;

        String toString() const
        {
            WriteBufferFromOwnString out;
            writeText(out);
            return out.str();
        }

        static std::optional<TemporaryReplaceTableName> fromString(const String & str)
        {
            TemporaryReplaceTableName result;
            ReadBufferFromString in(str);
            try
            {
                result.readText(in);
            }
            catch (...)
            {
                return std::nullopt;
            }
            return result;
        }

    private:
        void writeText(WriteBuffer & out) const
        {
            out << "_tmp_replace_" << name_hash << "_" << random_suffix;
        }

        void readText(ReadBuffer & in)
        {
            in >> "_tmp_replace_" >> name_hash >> "_" >> random_suffix;
        }
    };
}
