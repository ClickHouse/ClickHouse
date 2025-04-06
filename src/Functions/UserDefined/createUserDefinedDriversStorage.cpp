#include <Functions/UserDefined/createUserDefinedDriversStorage.h>
#include <Functions/UserDefined/UserDefinedDriversStorage.h>


namespace DB
{

std::unique_ptr<IUserDefinedDriversStorage> createUserDefinedDriversStorage()
{
    return std::make_unique<UserDefinedDriversStorage>();
}

}
