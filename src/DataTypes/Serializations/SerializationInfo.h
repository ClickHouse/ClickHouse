#pragma once

#include <Core/Block.h>

namespace DB
{

class SerializationInfo
{
public:
    static constexpr auto version = 1;

    using NameToNumber = std::unordered_map<String, size_t>;

    void add(const Block & block);
    void add(const SerializationInfo & other);

    size_t getNumberOfNonDefaultValues(const String & column_name) const;
    size_t getNumberOfRows() const { return number_of_rows; }

    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;

private:
    void fromJSON(const String & json_str);
    String toJSON() const;

    size_t number_of_rows = 0;
    NameToNumber non_default_values;
};

}
