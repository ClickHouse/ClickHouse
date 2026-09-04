#include <Core/CaseAwareBlockNameMap.h>

#include <cctype>
#include <memory>
#include <string_view>
#include <unordered_set>
#include <Core/Block.h>
#include <Formats/FormatSettings.h>
#include <base/StringViewHash.h>
#include <base/defines.h>
#include <sparsehash/dense_hash_map>
#include <Poco/String.h>
#include <Common/Exception.h>
#include <Common/SipHash.h>

namespace DB
{

namespace ErrorCodes
{
extern const int INCORRECT_DATA;
}

/// Interface for BlockNameMaps
class IBlockNameMap
{
public:
    virtual ~IBlockNameMap() = default;

    /// Adds a new element to the map
    virtual void add(std::string_view key, size_t idx) = 0;
    /// Sets the size of the map
    virtual void setSize(size_t size) = 0;
    /// Gets the size
    virtual size_t size() const = 0;
    /// Gets the value for the given key, returns NOT_FOUND if key is not present
    virtual size_t get(std::string_view key) const = 0;
    /// Method used by the map to compare strings
    virtual bool stringCompare(std::string_view left, std::string_view right) const = 0;
};

struct CaseInsensitiveHash
{
    size_t operator()(const std::string_view key) const
    {
        SipHash hash;
        for (char c : key)
            hash.update(static_cast<unsigned char>(Poco::Ascii::toLower(static_cast<unsigned char>(c))));
        return hash.get64();
    }
};

struct CaseInsensitiveEquality
{
    bool operator()(const std::string_view left, const std::string_view right) const { return CaseInsensitiveEquality::equal(left, right); }

    static bool equal(const std::string_view left, const std::string_view right)
    {
        if (left.size() != right.size())
            return false;

        for (size_t i = 0; i < left.size(); i++)
        {
            if (Poco::Ascii::toLower(left[i]) != Poco::Ascii::toLower(right[i]))
                return false;
        }
        return true;
    }
};

/// Case aware map
class CaseSensitiveBlockNameMap : public IBlockNameMap
{
public:
    CaseSensitiveBlockNameMap() { map.set_empty_key(std::string_view{}); }

    void setSize(size_t size) override
    {
        expected_size = size;
        map.resize(size);
    }

    void add(const std::string_view key, size_t idx) override { map[key] = idx; }

    size_t size() const override { return expected_size; }

    size_t get(const std::string_view key) const override
    {
        auto it = map.find(key);
        if (it == map.end())
        {
            return CaseAwareBlockNameMap::NOT_FOUND;
        }
        return it->second;
    }

    bool stringCompare(std::string_view left, std::string_view right) const override { return left == right; }

private:
    ::google::dense_hash_map<std::string_view, size_t, StringViewHash> map;
    size_t expected_size{0};
};

/// Case independent map
class CaseInsensitiveBlockNameMap : public IBlockNameMap
{
public:
    CaseInsensitiveBlockNameMap() { map.set_empty_key(std::string_view{}); }

    void setSize(size_t size) override
    {
        expected_size = size;
        map.resize(size);
    }

    void add(const std::string_view key, size_t idx) override
    {
        auto it = map.find(key);
        // If the key exists and points to the same idx, then it is not ambiguous
        if (it != map.end() && it->second != idx)
        {
            ambiguous_keys.insert(key);
        }
        map[key] = idx;
    }

    size_t size() const override { return expected_size; }

    /// Retrieves the position of a given key
    /// Can throw in the case where `key` is ambiguous
    /// For example: Name and namE will both map to the same key (name)
    size_t get(const std::string_view key) const override
    {
        auto it = map.find(key);
        if (it == map.end())
        {
            return CaseAwareBlockNameMap::NOT_FOUND;
        }
        if (ambiguous_keys.contains(key))
        {
            throw Exception(ErrorCodes::INCORRECT_DATA, "Ambiguous field (`{}`) when processing data.", key);
        }
        return it->second;
    }

    bool stringCompare(std::string_view left, std::string_view right) const override { return CaseInsensitiveEquality::equal(left, right); }

protected:
    ::google::dense_hash_map<std::string_view, size_t, CaseInsensitiveHash, CaseInsensitiveEquality> map;
    std::unordered_set<std::string_view, CaseInsensitiveHash, CaseInsensitiveEquality> ambiguous_keys;
    size_t expected_size{0};
};

/// Auto case map
/// First tries a case aware search, if it fails then it tries case independent
class AutoCaseBlockNameMap : public IBlockNameMap
{
public:
    void setSize(size_t size) override
    {
        expected_size = size;
        map.setSize(size);
        i_map.setSize(size);
    }

    void add(const std::string_view key, size_t idx) override
    {
        map.add(key, idx);
        i_map.add(key, idx);
    }

    size_t size() const override { return expected_size; }

    /** Retrieves the position of a given key
      * Can throw in the case where `key` is ambiguous
      * For example:
      *  map: {Name: 0, namE: 1}
      *
      *  map[Name] -> 0
      *  map[namE] -> 1
      *  map[name] -> error, ambiguous
      */
    size_t get(const std::string_view key) const override
    {
        // First check if the key has an exact match
        auto idx = map.get(key);
        if (idx != CaseAwareBlockNameMap::NOT_FOUND)
        {
            return idx;
        }

        // Check if the key has a match ignoring case
        return i_map.get(key);
    }

    bool stringCompare(std::string_view left, std::string_view right) const override { return map.stringCompare(left, right); }

private:
    CaseSensitiveBlockNameMap map;
    CaseInsensitiveBlockNameMap i_map;
    size_t expected_size{0};
};

CaseAwareBlockNameMap::CaseAwareBlockNameMap(FormatSettings::InputFormatColumnMatchingCaseSensitivity input_mode)
{
    switch (input_mode)
    {
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::MATCH_CASE: {
            map = std::make_unique<CaseSensitiveBlockNameMap>();
            break;
        }
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::IGNORE_CASE: {
            map = std::make_unique<CaseInsensitiveBlockNameMap>();
            break;
        }
        case FormatSettings::InputFormatColumnMatchingCaseSensitivity::AUTO: {
            map = std::make_unique<AutoCaseBlockNameMap>();
            break;
        }
    }
}

CaseAwareBlockNameMap::~CaseAwareBlockNameMap() = default;

void CaseAwareBlockNameMap::initFromBlock(const Block & block)
{
    setSize(block.getIndexByName().size());
    const auto & index_by_name = block.getIndexByName();
    for (const auto & [name, index] : index_by_name)
    {
        add(name, index);
    }
}

void CaseAwareBlockNameMap::add(std::string_view column_name, size_t idx)
{
    map->add(column_name, idx);
}

size_t CaseAwareBlockNameMap::get(std::string_view column_name) const
{
    return map->get(column_name);
}


bool CaseAwareBlockNameMap::equal(std::string_view left, std::string_view right) const
{
    return map->stringCompare(left, right);
}

size_t CaseAwareBlockNameMap::size() const
{
    return map->size();
}

void CaseAwareBlockNameMap::setSize(size_t size)
{
    map->setSize(size);
}
}
