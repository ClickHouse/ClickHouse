#pragma once

#include <memory>
#include <string_view>
#include <Formats/FormatSettings.h>

namespace DB
{

class Block;
class IBlockNameMap;

/// Case aware map between column name and position in a Block
class CaseAwareBlockNameMap
{
public:
    enum SearchResult : size_t
    {
        NOT_FOUND = size_t(-1), /// Return value of `get` method whenever the key is not found
    };

    explicit CaseAwareBlockNameMap(FormatSettings::InputFormatColumnMatchingCaseSensitivity input_mode);
    ~CaseAwareBlockNameMap();
    /// Adds a new pair column_name and position
    void add(std::string_view column_name, size_t idx);
    /// Fetches the position of the given column_name
    /// Returns NOT_FOUND in case the column_name is not in the map
    size_t get(std::string_view column_name) const;

    size_t size() const;

    /// Sets the expected size of the map
    void setSize(size_t size);

    /// Constructs the map from the given block
    void initFromBlock(const Block & block);

    /// Compares two strings, using the same method as the one used internally
    bool equal(std::string_view left, std::string_view right) const;
private:
    std::unique_ptr<IBlockNameMap> map;
};
}
