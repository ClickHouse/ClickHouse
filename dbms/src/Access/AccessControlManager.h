#pragma once

#include <Access/MultipleAccessStorage.h>
#include <Poco/AutoPtr.h>


namespace Poco
{
    namespace Util
    {
        class AbstractConfiguration;
    }
}

namespace DB
{
/// Manages access control entities.
class AccessControlManager : public MultipleAccessStorage
{
public:
    AccessControlManager();
    ~AccessControlManager();

    void loadFromConfig(const Poco::Util::AbstractConfiguration & users_config);
};

}
