#include <IO/ReadSettings.h>
#include <Common/Exception.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int UNKNOWN_READ_METHOD;
}

const char * toString(ReadMethod read_method)
{
    switch (read_method)
    {
#define CASE_READ_METHOD(NAME) case ReadMethod::NAME: return #NAME;
    FOR_EACH_READ_METHOD(CASE_READ_METHOD)
#undef CASE_READ_METHOD
    }
    __builtin_unreachable();
}

ReadMethod parseReadMethod(const std::string & name)
{
#define CASE_READ_METHOD(NAME) if (name == #NAME) return ReadMethod::NAME;
    FOR_EACH_READ_METHOD(CASE_READ_METHOD)
#undef CASE_READ_METHOD
    throw Exception(ErrorCodes::UNKNOWN_READ_METHOD, "Unknown read method '{}'", name);
}

}
