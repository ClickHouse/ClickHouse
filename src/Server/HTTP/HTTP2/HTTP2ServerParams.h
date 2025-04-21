#pragma once

#include <Poco/AutoPtr.h>
#include <Poco/RefCountedObject.h>
#include <Poco/Util/AbstractConfiguration.h>

namespace DB
{

class HTTP2ServerParams : public Poco::RefCountedObject
{
public:
    using Ptr = Poco::AutoPtr<HTTP2ServerParams>;

    static Ptr fromConfig(const Poco::Util::AbstractConfiguration & config);
};

}
