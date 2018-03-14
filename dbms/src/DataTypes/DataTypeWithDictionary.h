#include <Columns/IColumn.h>
#include <memory>
#include <Common/HashTable/HashMap.h>

namespace DB
{



template <typename IndexType>
class CountingRecursiveDictionary
{
public:
    using DictionaryType = HashMap<StringRef, IndexType, StringRefHash>;

    void insertData(const char * pos, size_t length) { column->insertData(pos, length); }

    StringRef getDataAt(size_t n) const
    {
        if (n < prev_dictionary_size)
            return prev_dictionary->getDataAt(n);
        else
            return column->getDataAt(n - prev_dictionary_size);
    }

private:
    ColumnPtr column;
    DictionaryType dictionary;

    std::shared_ptr<CountingRecursiveDictionary> prev_dictionary;
    size_t prev_dictionary_size = 0;
};

}
