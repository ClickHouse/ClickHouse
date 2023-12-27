#pragma once

#include <Parsers/IAST_fwd.h>
#include <Poco/AutoPtr.h>
#include <Poco/Util/AbstractConfiguration.h>


namespace DB
{

struct DictionaryAttachInfo
{
    ASTPtr create_query;
    Poco::AutoPtr<Poco::Util::AbstractConfiguration> config;
    time_t modification_time;
};

}
