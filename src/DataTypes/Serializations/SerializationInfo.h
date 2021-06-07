#pragma once

#include <Core/Block.h>
#include <DataTypes/Serializations/ISerialization.h>
#include <Columns/ColumnSparse.h>

namespace DB
{

/** Contains information about kinds of serialization of columns.
 *  Also contains information about content of columns,
 *  that helps to choose kind of serialization of column.
 *
 *  Currently has only information about number of default rows,
 *  that helps to choose sparse serialization.
 *
 *  Should be extended, when new kinds of serialization will be implemented.
 */
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

    /// Information about one column.
    /// Can be extended, when new kinds of serialization will be implemented.
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

/// Builder, that helps to create SerializationInfo.
class SerializationInfoBuilder
{
public:
    SerializationInfoBuilder();
    SerializationInfoBuilder(
        double ratio_for_sparse_serialization_,
        double default_rows_search_sample_ratio_ = ColumnSparse::DEFAULT_ROWS_SEARCH_SAMPLE_RATIO);

    /// Add information about column from block.
    void add(const Block & block);

    /// Add information about column from other SerializationInfo.
    void add(const SerializationInfo & other);

    /// Choose kind of serialization for every column
    /// according its content and return finalized SerializationInfo.
    SerializationInfoPtr build();

    /// Create SerializationInfo from other.
    /// Respects kinds of serialization for columns, that exist in other SerializationInfo,
    /// but keeps information about content of column from current SerializationInfo.
    SerializationInfoPtr buildFrom(const SerializationInfo & other);

    double getRatioForSparseSerialization() const { return ratio_for_sparse_serialization; }

private:
    double ratio_for_sparse_serialization;
    double default_rows_search_sample_ratio;

    SerializationInfoPtr info;
};

using SerializationInfoBuilderPtr = std::shared_ptr<SerializationInfoBuilder>;

}
