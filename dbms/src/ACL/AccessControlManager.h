#pragma once

#include <ACL/MultipleAttributesStorage.h>

namespace DB
{
/// Manages access control entities.
class AccessControlManager : public MultipleAttributesStorage
{
public:
    AccessControlManager();
    ~AccessControlManager();
};

}
