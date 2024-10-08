#include <cstdlib>
#include <base/defines.h>
#include <benchmark/benchmark.h>

class OldSortedStringDictionary
{
public:
    struct DictEntry
    {
        DictEntry(const char * str, size_t len) : data(str), length(len) { }
        const char * data;
        size_t length;
    };

    OldSortedStringDictionary() : totalLength(0) { }

    // insert a new string into dictionary, return its insertion order
    size_t insert(const char * str, size_t len);

    // reorder input index buffer from insertion order to dictionary order
    void reorder(std::vector<int64_t> & idxBuffer) const;

    // get dict entries in insertion order
    void getEntriesInInsertionOrder(std::vector<const DictEntry *> &) const;

    size_t size() const;

    // return total length of strings in the dictionary
    uint64_t length() const;

    void clear();

    // store indexes of insertion order in the dictionary for not-null rows
    std::vector<int64_t> idxInDictBuffer;

private:
    struct LessThan
    {
        bool operator()(const DictEntry & left, const DictEntry & right) const
        {
            int ret = memcmp(left.data, right.data, std::min(left.length, right.length));
            if (ret != 0)
            {
                return ret < 0;
            }
            return left.length < right.length;
        }
    };

    std::map<DictEntry, size_t, LessThan> dict;
    std::vector<std::vector<char>> data;
    uint64_t totalLength;
};

// insert a new string into dictionary, return its insertion order
size_t OldSortedStringDictionary::insert(const char * str, size_t len)
{
    auto ret = dict.insert({DictEntry(str, len), dict.size()});
    if (ret.second)
    {
        // make a copy to internal storage
        data.push_back(std::vector<char>(len));
        memcpy(data.back().data(), str, len);
        // update dictionary entry to link pointer to internal storage
        DictEntry * entry = const_cast<DictEntry *>(&(ret.first->first));
        entry->data = data.back().data();
        totalLength += len;
    }
    return ret.first->second;
}

/**
   * Reorder input index buffer from insertion order to dictionary order
   *
   * We require this function because string values are buffered by indexes
   * in their insertion order. Until the entire dictionary is complete can
   * we get their sorted indexes in the dictionary in that ORC specification
   * demands dictionary should be ordered. Therefore this function transforms
   * the indexes from insertion order to dictionary value order for final
   * output.
   */
void OldSortedStringDictionary::reorder(std::vector<int64_t> & idxBuffer) const
{
    // iterate the dictionary to get mapping from insertion order to value order
    std::vector<size_t> mapping(dict.size());
    size_t dictIdx = 0;
    for (auto it = dict.cbegin(); it != dict.cend(); ++it)
    {
        mapping[it->second] = dictIdx++;
    }

    // do the transformation
    for (size_t i = 0; i != idxBuffer.size(); ++i)
    {
        idxBuffer[i] = static_cast<int64_t>(mapping[static_cast<size_t>(idxBuffer[i])]);
    }
}

// get dict entries in insertion order
void OldSortedStringDictionary::getEntriesInInsertionOrder(std::vector<const DictEntry *> & entries) const
{
    entries.resize(dict.size());
    for (auto it = dict.cbegin(); it != dict.cend(); ++it)
    {
        entries[it->second] = &(it->first);
    }
}

// return count of entries
size_t OldSortedStringDictionary::size() const
{
    return dict.size();
}

// return total length of strings in the dictionary
uint64_t OldSortedStringDictionary::length() const
{
    return totalLength;
}

void OldSortedStringDictionary::clear()
{
    totalLength = 0;
    data.clear();
    dict.clear();
}


/**
   * Implementation of increasing sorted string dictionary
   */
class NewSortedStringDictionary
{
public:
    struct DictEntry
    {
        DictEntry(const char * str, size_t len) : data(str), length(len) { }
        const char * data;
        size_t length;
    };

    struct DictEntryWithIndex
    {
        DictEntryWithIndex(const char * str, size_t len, size_t index_) : entry(str, len), index(index_) { }
        DictEntry entry;
        size_t index;
    };

    NewSortedStringDictionary() : totalLength_(0) { }

    // insert a new string into dictionary, return its insertion order
    size_t insert(const char * str, size_t len);

    // reorder input index buffer from insertion order to dictionary order
    void reorder(std::vector<int64_t> & idxBuffer) const;

    // get dict entries in insertion order
    void getEntriesInInsertionOrder(std::vector<const DictEntry *> &) const;

    // return count of entries
    size_t size() const;

    // return total length of strings in the dictionary
    uint64_t length() const;

    void clear();

    // store indexes of insertion order in the dictionary for not-null rows
    std::vector<int64_t> idxInDictBuffer;

private:
    struct LessThan
    {
        bool operator()(const DictEntryWithIndex & l, const DictEntryWithIndex & r)
        {
            const auto & left = l.entry;
            const auto & right = r.entry;
            int ret = memcmp(left.data, right.data, std::min(left.length, right.length));
            if (ret != 0)
            {
                return ret < 0;
            }
            return left.length < right.length;
        }
    };

    mutable std::vector<DictEntryWithIndex> flatDict_;
    std::unordered_map<std::string, size_t> keyToIndex;
    uint64_t totalLength_;
};

// insert a new string into dictionary, return its insertion order
size_t NewSortedStringDictionary::insert(const char * str, size_t len)
{
    size_t index = flatDict_.size();
    auto ret = keyToIndex.emplace(std::string(str, len), index);
    if (ret.second)
    {
        flatDict_.emplace_back(ret.first->first.data(), ret.first->first.size(), index);
        totalLength_ += len;
    }
    return ret.first->second;
}

/**
   * Reorder input index buffer from insertion order to dictionary order
   *
   * We require this function because string values are buffered by indexes
   * in their insertion order. Until the entire dictionary is complete can
   * we get their sorted indexes in the dictionary in that ORC specification
   * demands dictionary should be ordered. Therefore this function transforms
   * the indexes from insertion order to dictionary value order for final
   * output.
   */
void NewSortedStringDictionary::reorder(std::vector<int64_t> & idxBuffer) const
{
    // iterate the dictionary to get mapping from insertion order to value order
    std::vector<size_t> mapping(flatDict_.size());
    for (size_t i = 0; i < flatDict_.size(); ++i)
    {
        mapping[flatDict_[i].index] = i;
    }

    // do the transformation
    for (size_t i = 0; i != idxBuffer.size(); ++i)
    {
        idxBuffer[i] = static_cast<int64_t>(mapping[static_cast<size_t>(idxBuffer[i])]);
    }
}

// get dict entries in insertion order
void NewSortedStringDictionary::getEntriesInInsertionOrder(std::vector<const DictEntry *> & entries) const
{
    std::sort(
        flatDict_.begin(),
        flatDict_.end(),
        [](const DictEntryWithIndex & left, const DictEntryWithIndex & right) { return left.index < right.index; });

    entries.resize(flatDict_.size());
    for (size_t i = 0; i < flatDict_.size(); ++i)
    {
        entries[i] = &(flatDict_[i].entry);
    }
}

// return count of entries
size_t NewSortedStringDictionary::size() const
{
    return flatDict_.size();
}

// return total length of strings in the dictionary
uint64_t NewSortedStringDictionary::length() const
{
    return totalLength_;
}

void NewSortedStringDictionary::clear()
{
    totalLength_ = 0;
    keyToIndex.clear();
    flatDict_.clear();
}

template <size_t cardinality>
static std::vector<std::string> mockStrings()
{
    std::vector<std::string> res(1000000);
    for (auto & s : res)
    {
        s = "test string dictionary " + std::to_string(rand() % cardinality);
    }
    return res;
}

template <typename DictionaryImpl>
static NO_INLINE std::unique_ptr<DictionaryImpl> createAndWriteStringDictionary(const std::vector<std::string> & strs)
{
    auto dict = std::make_unique<DictionaryImpl>();
    for (const auto & str : strs)
    {
        auto index = dict->insert(str.data(), str.size());
        dict->idxInDictBuffer.push_back(index);
    }
    dict->reorder(dict->idxInDictBuffer);

    return dict;
}

template <typename DictionaryImpl, size_t cardinality>
static void BM_writeStringDictionary(benchmark::State & state)
{
    auto strs = mockStrings<cardinality>();
    for (auto _ : state)
    {
        auto dict = createAndWriteStringDictionary<DictionaryImpl>(strs);
        benchmark::DoNotOptimize(dict);
    }
}

BENCHMARK_TEMPLATE(BM_writeStringDictionary, OldSortedStringDictionary, 10);
BENCHMARK_TEMPLATE(BM_writeStringDictionary, NewSortedStringDictionary, 10);
BENCHMARK_TEMPLATE(BM_writeStringDictionary, OldSortedStringDictionary, 100);
BENCHMARK_TEMPLATE(BM_writeStringDictionary, NewSortedStringDictionary, 100);
BENCHMARK_TEMPLATE(BM_writeStringDictionary, OldSortedStringDictionary, 1000);
BENCHMARK_TEMPLATE(BM_writeStringDictionary, NewSortedStringDictionary, 1000);
BENCHMARK_TEMPLATE(BM_writeStringDictionary, OldSortedStringDictionary, 10000);
BENCHMARK_TEMPLATE(BM_writeStringDictionary, NewSortedStringDictionary, 10000);
BENCHMARK_TEMPLATE(BM_writeStringDictionary, OldSortedStringDictionary, 100000);
BENCHMARK_TEMPLATE(BM_writeStringDictionary, NewSortedStringDictionary, 100000);

