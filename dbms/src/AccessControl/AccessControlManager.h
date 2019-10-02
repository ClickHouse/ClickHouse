#pragma once

#include <AccessControl/MultipleAttributesStorage.h>

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
