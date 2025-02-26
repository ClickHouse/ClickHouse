

#include <map>
#include <base/types.h>
namespace Iceberg
{

template <typename T>
class IteratorWrapper
{
private:
    using StorageType = std::map<String, T>;
    using StorageConstIterator = StorageType::const_iterator;
    using StorageIterator = StorageType::iterator;

public:
    explicit IteratorWrapper(StorageConstIterator iterator_) : iterator(iterator_) { }
    explicit IteratorWrapper(StorageIterator iterator_) : iterator(iterator_) { }

    String getName() const { return iterator->first; }

    const T * operator->() const { return &iterator->second; }
    const T & operator*() const { return iterator->second; }

private:
    StorageIterator iterator;
};

}
