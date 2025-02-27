#pragma once

#include <map>
#include <base/types.h>
namespace Iceberg
{

template <typename T>
class IteratorWrapper
{
public:
    using StorageConstIterator = StorageType::const_iterator;

private:
    using StorageType = std::map<String, T>;
    using StorageIterator = StorageType::iterator;
    using CreationFunction = std::function<void(IteratorWrapper &)>;

public:
    explicit IteratorWrapper(StorageConstIterator iterator_) : iterator(iterator_) { }
    explicit IteratorWrapper(StorageIterator iterator_) : iterator(iterator_) { }
    explicit IteratorWrapper(CreationFunction creation_function_) : creation_function(creation_function_) { }
    const String & getName() const { return iterator->first; }

    const T * operator->() const { return &iterator->second; }
    const T & operator*() const { return iterator->second; }

private:
    void setIterator(StorageIterator iterator_) { iterator = iterator_; }
private:
    std::optional<StorageIterator> iterator;
    CreationFunction creation_function;

    friend class CreationFunction;
};

}


// #pragma once

// #include <map>
// #include <base/types.h>
// namespace Iceberg
// {

// template <typename T, typename CreationFunction>
// class IteratorWrapper
// {
// private:
//     using StorageType = std::map<String, T>;
//     using StorageConstIterator = StorageType::const_iterator;
//     using StorageIterator = StorageType::iterator;

// public:
//     explicit IteratorWrapper(StorageConstIterator iterator_) : iterator(iterator_) { }
//     explicit IteratorWrapper(StorageIterator iterator_) : iterator(iterator_) { }
//     explicit IteratorWrapper(CreationFunction creation_function_)
//         : creation_function(creation_function_)
//     {
//     }

//     const String & getName() const
//     {
//         initialize();
//         return iterator->first;
//     }

//     const T * operator->() const
//     {
//         initialize();
//         return &iterator->second;
//     }
//     const T & operator*() const
//     {
//         initialize();
//         return iterator->second;
//     }

// private:
//     void initialize() const
//     {
//         if (!iterator.has_value())
//         {
//             iterator = creation_function();
//         }
//     }

//     mutable std::optional<StorageConstIterator> iterator;
//     CreationFunction creation_function;
// };

// }
