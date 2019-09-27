#include <ACL/AccessControlManager.h>
#include <ACL/MultipleAttributesStorage.h>
//#include <ACL/MemoryAttributesStorage.h>


namespace DB
{
namespace AccessControlNames
{
    extern const size_t ROLE_NAMESPACE_IDX = 0;
}


AccessControlManager::AccessControlManager()
    : MultipleAttributesStorage(std::vector<std::unique_ptr<Storage>>{})
{
}


AccessControlManager::~AccessControlManager()
{
}

}
