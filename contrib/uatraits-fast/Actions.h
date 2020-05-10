#pragma once

#include <metrika/core/libs/uatraits-fast/uatraits-fast.h>
#include <re2/re2.h>
#include <contrib/libs/clickhouse/libs/libcommon/include/common/StringRef.h>


struct SimpleMatchingActionSetStringRef : public UATraits::IAction
{
    SimpleMatchingActionSetStringRef(UATraits::Result::StringRefFields field_, StringRef value_)
        : field(field_)
        , value(value_)
        , has_substitution(value_ == "$1")
    {
    }

    void execute(StringRef /*user_agent*/, UATraits::Result & result, size_t subpatterns_size, re2::StringPiece * subpatterns) const
    {
        StringRef result_value;
        if (has_substitution)
        {
            if (subpatterns_size >= 2)
            {
                result_value.data = subpatterns[1].data();
                result_value.size = subpatterns[1].end() - result_value.data;
            }
        }
        else
        {
            result_value = value;
        }
        result.string_ref_fields[field] = result_value;
    }

    UATraits::Result::StringRefFields field;
    StringRef value;
    bool has_substitution;
};


struct ActionSetStringRef : public UATraits::IAction
{
    ActionSetStringRef(UATraits::Result::StringRefFields field_, StringRef value_) : field(field_), value(value_) {}

    void execute(StringRef /*user_agent*/, UATraits::Result & result, size_t /*subpatterns_size*/, re2::StringPiece * /*subpatterns*/) const
    {
        result.string_ref_fields[field] = value;
    }

    UATraits::Result::StringRefFields field;
    StringRef value;
};


struct ActionSetBool : public UATraits::IAction
{
    ActionSetBool(UATraits::Result::BoolFields field_, bool value_) : field(field_), value(value_) {}

    void execute(StringRef /*user_agent*/, UATraits::Result & result, size_t /*subpatterns_size*/, re2::StringPiece * /*subpatterns*/) const
    {
        result.bool_fields[field] = value;
    }

    UATraits::Result::BoolFields field;
    bool value;
};


struct ActionSetUInt : public UATraits::IAction
{
    ActionSetUInt(UATraits::Result::UIntFields field_, unsigned value_) : field(field_), value(value_) {}

    void execute(StringRef /*user_agent*/, UATraits::Result & result, size_t /*subpatterns_size*/, re2::StringPiece * /*subpatterns*/) const
    {
        result.uint_fields[field] = value;
    }

    UATraits::Result::UIntFields field;
    unsigned value;
};


struct ActionSetVersion : public UATraits::IAction
{
    ActionSetVersion(UATraits::Result::VersionFields field_, StringRef value_) : field(field_), value(value_)
    {
        /// Не учитывается экранирование символа $. Не учитывается возможность подстановок другой формы.
        has_substitutions = nullptr != memchr(value.data, '$', value.size);
    }

    static void parseVersion(const StringRef src, UATraits::Version & version)
    {
        /// Парсится вся версия целиком, без учёта того, какие подстановки присутствуют, и в каком порядке.
        const char * pos = src.data;
        const char * end = src.data + src.size;
        const char * old_pos = pos;

        pos = DB::tryReadIntText(version.v1, pos, end);

        if (pos > old_pos)
        {
            if (pos < end && !(*pos >= '0' && *pos <= '9'))
                ++pos;

            old_pos = pos;
            pos = DB::tryReadIntText(version.v2, pos, end);

            if (pos > old_pos)
            {
                if (pos < end && !(*pos >= '0' && *pos <= '9'))
                    ++pos;

                old_pos = pos;
                pos = DB::tryReadIntText(version.v3, pos, end);

                if (pos > old_pos)
                {
                    if (pos < end && !(*pos >= '0' && *pos <= '9'))
                        ++pos;

                    old_pos = pos;
                    pos = DB::tryReadIntText(version.v4, pos, end);
                }
            }
        }
    }

    void execute(StringRef /*user_agent*/, UATraits::Result & result, size_t subpatterns_size, re2::StringPiece * subpatterns) const
    {
        StringRef version;

        if (!has_substitutions)
            version = value;
        else
        {
            if (subpatterns_size >= 2)
                version.data = subpatterns[1].data();

            if (subpatterns_size >= 2 && subpatterns[1].end() > version.data)
                version.size = subpatterns[1].end() - version.data;
            if (subpatterns_size >= 3 && subpatterns[2].end() > version.data)
                version.size = subpatterns[2].end() - version.data;
            if (subpatterns_size >= 4 && subpatterns[3].end() > version.data)
                version.size = subpatterns[3].end() - version.data;
            if (subpatterns_size >= 5 && subpatterns[4].end() > version.data)
                version.size = subpatterns[4].end() - version.data;
        }
        parseVersion(version, result.version_fields[field]);
    }

    UATraits::Result::VersionFields field;
    StringRef value;
    bool has_substitutions;
};
