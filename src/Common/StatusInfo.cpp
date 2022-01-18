#include <Common/StatusInfo.h>
#include <Interpreters/ExternalLoader.h>

/// Available status. Add something here as you wish.
#define APPLY_FOR_STATUS(M) \
    M(DictionaryStatus, "Dictionary Status.", DB::getStatusEnumAllPossibleValues()) \


namespace CurrentStatusInfo
{
    #define M(NAME, DOCUMENTATION, ENUM) extern const Status NAME = __COUNTER__;
        APPLY_FOR_STATUS(M)
    #undef M
    constexpr Status END = __COUNTER__;

    std::mutex locks[END] {};
    std::unordered_map<String, Int8> values[END] {};

    const char * getName(Status event)
    {
        static const char * strings[] =
        {
        #define M(NAME, DOCUMENTATION, ENUM) #NAME,
            APPLY_FOR_STATUS(M)
        #undef M
        };

        return strings[event];
    }

    const char * getDocumentation(Status event)
    {
        static const char * strings[] =
        {
        #define M(NAME, DOCUMENTATION, ENUM) #DOCUMENTATION,
            APPLY_FOR_STATUS(M)
        #undef M
        };

        return strings[event];
    }

    const std::vector<std::pair<String, Int8>> & getAllPossibleValues(Status event)
    {
        static const std::vector<std::pair<String, Int8>> enum_values [] =
        {
        #define M(NAME, DOCUMENTATION, ENUM) ENUM,
            APPLY_FOR_STATUS(M)
        #undef M
        };
        return enum_values[event];
    }

    Status end() { return END; }
}

#undef APPLY_FOR_STATUS
