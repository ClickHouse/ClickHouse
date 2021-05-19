#pragma once

#include <Core/Block.h>
#include <DataTypes/Serializations/ISerialization.h>

namespace DB
{

class SerializationInfo
{
public:
    using NameToKind = std::unordered_map<String, ISerialization::Kind>;

    SerializationInfo() = default;
    SerializationInfo(size_t number_of_rows_, const NameToKind & kinds);

    static constexpr auto version = 1;
    size_t getNumberOfDefaultRows(const String & column_name) const;
    ISerialization::Kind getKind(const String & column_name) const;

    size_t getNumberOfRows() const { return number_of_rows; }

    void readText(ReadBuffer & in);
    void writeText(WriteBuffer & out) const;

    static NameToKind getKinds(const Block & block);
    static NameToKind readKindsBinary(ReadBuffer & in);
    static void writeKindsBinary(const NameToKind & kinds, WriteBuffer & out);

private:
    void fromJSON(const String & json_str);
    String toJSON() const;

    struct Column
    {
        ISerialization::Kind kind = ISerialization::Kind::DEFAULT;
        size_t num_defaults = 0;
    };

    using NameToColumn = std::unordered_map<String, Column>;

    size_t number_of_rows = 0;
    NameToColumn columns;

    friend class SerializationInfoBuilder;
};

using SerializationInfoPtr = std::shared_ptr<SerializationInfo>;

class SerializationInfoBuilder
{
public:
    SerializationInfoBuilder();
    SerializationInfoBuilder(
        double ratio_for_sparse_serialization_,
        size_t default_rows_search_step_ = IColumn::DEFAULT_ROWS_SEARCH_STEP);

    void add(const Block & block);
    void add(const SerializationInfo & other);

    SerializationInfoPtr build();
    SerializationInfoPtr buildFrom(const SerializationInfo & other);
    static SerializationInfoPtr buildFromBlock(const Block & block);

    double getRatioForSparseSerialization() const { return ratio_for_sparse_serialization; }

private:
    double ratio_for_sparse_serialization;
    size_t default_rows_search_step;

    SerializationInfoPtr info;
};

using SerializationInfoBuilderPtr = std::shared_ptr<SerializationInfoBuilder>;

}
