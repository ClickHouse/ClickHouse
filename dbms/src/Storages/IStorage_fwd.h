#pragma once

#include <map>
#include <string>

#include <boost/smart_ptr/intrusive_ptr.hpp>


namespace DB
{

class IStorage;

void intrusive_ptr_add_ref(IStorage *);
void intrusive_ptr_release(IStorage *);

using StoragePtr = boost::intrusive_ptr<IStorage>;
using Tables = std::map<std::string, StoragePtr>;

}
