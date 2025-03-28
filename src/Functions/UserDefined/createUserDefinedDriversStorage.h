#pragma once

#include <Functions/UserDefined/IUserDefinedDriversStorage.h>

namespace DB
{

class IUserDefinedDriversStorage;

std::unique_ptr<IUserDefinedDriversStorage> createUserDefinedDriversStorage();

}
