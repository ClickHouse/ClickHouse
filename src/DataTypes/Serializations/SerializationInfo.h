#pragma once

#include <Core/Block.h>

namespace DB
{

class SerializationInfo
{
public:
    static constexpr auto version = 1;

    using NameToNumber = std::unordered_map<String, size_t>;

    SerializationInfo(
        double ratio_for_sparse_serialization_,
        size_t default_rows_search_step_ = IColumn::DEFAULT_ROWS_SEARCH_STEP);

    void add(const Block & block);
    void update(const SerializationInfo & other);
    void add(const SerializationInfo & other);

    size_t getNumberOfDefaultRows(const String & column_name) const;
    size_t getNumberOfRows() const { return number_of_rows; }
    double getRatioForSparseSerialization() const { return ratio_for_sparse_serialization; }

    void read(ReadBuffer & in);
    void write(WriteBuffer & out) const;

private:
    void fromJSON(const String & json_str);
    String toJSON() const;

    double ratio_for_sparse_serialization;
    size_t default_rows_search_step;

    size_t number_of_rows = 0;
    NameToNumber default_rows;
};

}
