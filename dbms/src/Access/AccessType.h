#pragma once

#include <Core/Types.h>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <array>


namespace DB
{
/// Represents an access type which can be granted on databases, tables, columns, etc.
enum class AccessType
{
    NONE,  /// no access
    ALL,   /// full access

    SHOW,  /// allows to execute SHOW TABLES, SHOW CREATE TABLE, SHOW DATABASES and so on
           /// (granted implicitly with any other grant)

    EXISTS,  /// allows to execute EXISTS, USE, i.e. to check existence
             /// (granted implicitly on the database level with any other grant on the database and lower levels,
             ///  e.g. "GRANT SELECT(x) ON db.table" also grants EXISTS on db.*)

    SELECT,
    INSERT,
    UPDATE,  /// allows to execute ALTER UPDATE
    DELETE,  /// allows to execute ALTER DELETE
};

constexpr size_t MAX_ACCESS_TYPE = static_cast<size_t>(AccessType::DELETE) + 1;

std::string_view toString(AccessType type);


namespace impl
{
    template <typename = void>
    class AccessTypeToKeywordConverter
    {
    public:
        static const AccessTypeToKeywordConverter & instance()
        {
            static const AccessTypeToKeywordConverter res;
            return res;
        }

        std::string_view convert(AccessType type) const
        {
            return access_type_to_keyword_mapping[static_cast<size_t>(type)];
        }

    private:
        void addToMapping(AccessType type, const std::string_view & str)
        {
            String str2{str};
            boost::replace_all(str2, "_", " ");
            if (islower(str2[0]))
                str2 += "()";
            access_type_to_keyword_mapping[static_cast<size_t>(type)] = str2;
        }

        AccessTypeToKeywordConverter()
        {
#define ACCESS_TYPE_TO_KEYWORD_CASE(type) \
            addToMapping(AccessType::type, #type)

            ACCESS_TYPE_TO_KEYWORD_CASE(NONE);
            ACCESS_TYPE_TO_KEYWORD_CASE(ALL);
            ACCESS_TYPE_TO_KEYWORD_CASE(SHOW);
            ACCESS_TYPE_TO_KEYWORD_CASE(EXISTS);

            ACCESS_TYPE_TO_KEYWORD_CASE(SELECT);
            ACCESS_TYPE_TO_KEYWORD_CASE(INSERT);
            ACCESS_TYPE_TO_KEYWORD_CASE(UPDATE);
            ACCESS_TYPE_TO_KEYWORD_CASE(DELETE);

#undef ACCESS_TYPE_TO_KEYWORD_CASE
        }

        std::array<String, MAX_ACCESS_TYPE> access_type_to_keyword_mapping;
    };
}

inline std::string_view toKeyword(AccessType type) { return impl::AccessTypeToKeywordConverter<>::instance().convert(type); }

}
