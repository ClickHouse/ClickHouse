#pragma once

#include <Storages/IStorage.h>

#include <base/shared_ptr_helper.h>

namespace DB
{

class StorageDirectory : public shared_ptr_helper<StorageDirectory>, public IStorage
{
    friend struct shared_ptr_helper<StorageFile>;
};

}
