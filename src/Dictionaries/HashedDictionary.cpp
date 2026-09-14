#include <Dictionaries/HashedDictionary.h>

namespace DB
{

template class HashedDictionary<DictionaryKeyType::Simple, /* sparse= */ false>;
template class HashedDictionary<DictionaryKeyType::Complex, /* sparse= */ false>;

}
