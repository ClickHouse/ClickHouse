#include <Dictionaries/HashedDictionary.h>

namespace DB
{

template class HashedDictionary<DictionaryKeyType::Simple, /* sparse= */ true>;
template class HashedDictionary<DictionaryKeyType::Complex, /* sparse= */ true>;

}
