#pragma once

#include <unordered_map>

#include <Core/Types.h>
#include <Core/Field.h>
#include <Core/ColumnsWithTypeAndName.h>
#include <Common/typeid_cast.h>
#include <Common/HashTable/HashMap.h>
#include <Columns/IColumn.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <Columns/IColumnUnique.h>

namespace DB
{

/** Column for JSON values.
  */
class ColumnJSONB final : public COWHelper<IColumn, ColumnJSONB>
{
public:
    const char * getFamilyName() const override { return "JSONB"; }

    size_t size() const override { return getRelationsBinary().size(); }

    Field operator[](size_t row_num) const override;

    StringRef getDataAt(size_t n) const override;

    void get(size_t rows, Field & res) const override;

    void insert(const Field & field) override;

    void insertFrom(const IColumn & src, size_t row_num) override;

    void insertRangeFrom(const IColumn & src, size_t offset, size_t limit) override;

    void insertData(const char *pos, size_t length) override;

    size_t byteSize() const override;

    size_t allocatedBytes() const override;

    void popBack(size_t n) override;

    void insertDefault() override;

    StringRef serializeValueIntoArena(size_t n, Arena & arena, char const *& begin) const override;

    const char * deserializeAndInsertFromArena(const char * pos) override;

    void updateHashWithValue(size_t n, SipHash & hash) const override;

    ColumnPtr filter(const Filter & filter, ssize_t result_size_hint) const override;

    ColumnPtr permute(const Permutation & perm, size_t limit) const override;

    ColumnPtr index(const IColumn & indexes, size_t limit) const override;

    int compareAt(size_t n, size_t m, const IColumn & rhs, int nan_direction_hint) const override;

    void getPermutation(bool reverse, size_t limit, int nan_direction_hint, Permutation & res) const override;

    ColumnPtr replicate(const Offsets & offsets) const override;

    std::vector<MutableColumnPtr> scatter(ColumnIndex num_columns, const Selector & selector) const override;

    void gather(ColumnGathererStream & gatherer_stream) override;

    void getExtremes(Field & min, Field & max) const override;

    MutableColumnPtr cloneEmpty() const override;

private:
    class StructGraph   /// Store JSON data structure information for some rows
    {
    public:
        StructGraph(const StructGraph & other) = default;

        explicit StructGraph(ColumnPtr keys_dictionary_, ColumnPtr relations_dictionary_)
            : keys_dictionary(keys_dictionary_), relations_dictionary(relations_dictionary_)
        {}

        explicit StructGraph(MutableColumnPtr & keys_dictionary_, MutableColumnPtr & relations_dictionary_)
            : keys_dictionary(std::move(keys_dictionary_)), relations_dictionary(std::move(relations_dictionary_))
        {}

        WrappedPtr & getKeysAsUniqueColumnPtr() { return keys_dictionary; }
        const ColumnPtr & getKeysAsUniqueColumnPtr() const { return keys_dictionary; }

        WrappedPtr & getRelationsAsUniqueColumnPtr() { return relations_dictionary; }
        const ColumnPtr & getRelationsAsUniqueColumnPtr() const { return relations_dictionary; }

        IColumnUnique & getKeysAsUniqueColumn() { return static_cast<IColumnUnique &>(*keys_dictionary); }
        const IColumnUnique & getKeysAsUniqueColumn() const { return static_cast<const IColumnUnique &>(*keys_dictionary); }

        IColumnUnique & getRelationsAsUniqueColumn() { return static_cast<IColumnUnique &>(*relations_dictionary); }
        const IColumnUnique & getRelationsAsUniqueColumn() const { return static_cast<const IColumnUnique &>(*relations_dictionary); }

        ColumnPtr insertKeysDictionaryFrom(const IColumnUnique & src, const ColumnArray & positions);

        ColumnPtr insertRelationsFrom(
            const IColumnUnique & keys_src, const IColumnUnique & relations_src, const ColumnArray & positions, size_t offset, size_t limit);

    private:
        WrappedPtr keys_dictionary;
        WrappedPtr relations_dictionary;
    };

    class BinaryJSONData  /// Store JSON data for some rows
    {
    public:
        BinaryJSONData(
            MutableColumns && data_columns_, bool multiple_columns_ = false, bool is_nullable_ = false, bool is_low_cardinality_ = false);

        bool isMultipleColumns() { return multiple_columns; }
        bool isMultipleColumns() const { return multiple_columns; }
        void setMultipleColumns(bool multiple_columns_) { multiple_columns = multiple_columns_; }

        IColumn & getBinaryColumn();    /// when split_data_to_columns = false, we use this store binary data
        const IColumn & getBinaryColumn() const;    /// when split_data_to_columns = false, we use this store binary data

        ColumnPtr & getBinaryColumnPtr();
        const ColumnPtr & getBinaryColumnPtr() const;

        IColumn & getRelationsColumn()  { return *data_columns[0]; }
        const IColumn & getRelationsColumn() const { return *data_columns[0]; }

        ColumnPtr & getRelationsColumnPtr()  { return data_columns[0]; }
        const ColumnPtr & getRelationsColumnPtr() const { return data_columns[0]; }

        std::vector<WrappedPtr> & getAllDataColumns() { return data_columns; }
        const std::vector<WrappedPtr> & getAllDataColumns() const { return data_columns; }

    private:
        bool is_nullable = false;
        bool is_low_cardinality = false;
        bool multiple_columns = false;
        std::vector<WrappedPtr> data_columns;
    };

    StructGraph struct_graph;
    BinaryJSONData binary_json_data;

    ColumnJSONB(const ColumnJSONB &) = default;
    explicit ColumnJSONB(MutableColumnPtr && keys_dictionary_, MutableColumnPtr && relations_dictionary_, MutableColumns && data_columns_,
        bool multiple_columns = false, bool is_nullable_ = false, bool is_lowCardinality_ = false);

    friend class COWHelper<IColumn, ColumnJSONB>;

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumn, ColumnJSONB>;
    static MutablePtr create(
        const ColumnPtr & keys_dictionary_, const ColumnPtr & relations_dictionary_, const Columns & data_columns_,
        bool multiple_columns_ = false, bool is_nullable_ = false, bool is_lowCardinality_ = false);

    bool isMultipleColumn() { return binary_json_data.isMultipleColumns(); }
    bool isMultipleColumn() const { return binary_json_data.isMultipleColumns(); }
    void setMultipleColumn(bool is_binary) { binary_json_data.setMultipleColumns(!is_binary); }

    IColumn & getDataBinary() { return binary_json_data.getBinaryColumn(); }
    const IColumn & getDataBinary() const { return binary_json_data.getBinaryColumn(); }

    ColumnPtr & getDataBinaryPtr() { return binary_json_data.getBinaryColumnPtr(); }
    const ColumnPtr & getDataBinaryPtr() const { return binary_json_data.getBinaryColumnPtr(); }

    IColumn & getRelationsBinary() { return binary_json_data.getRelationsColumn(); }
    const IColumn & getRelationsBinary() const { return binary_json_data.getRelationsColumn(); }

    ColumnPtr & getRelationsBinaryPtr() { return binary_json_data.getRelationsColumnPtr(); }
    const ColumnPtr & getRelationsBinaryPtr() const { return binary_json_data.getRelationsColumnPtr(); }

    IColumnUnique & getKeysDictionary() { return struct_graph.getKeysAsUniqueColumn(); }
    const IColumnUnique & getKeysDictionary() const { return struct_graph.getKeysAsUniqueColumn(); }

    ColumnPtr & getKeysDictionaryPtr() { return struct_graph.getKeysAsUniqueColumnPtr(); }
    const ColumnPtr & getKeysDictionaryPtr() const { return struct_graph.getKeysAsUniqueColumnPtr(); }

    IColumnUnique & getRelationsDictionary() { return struct_graph.getRelationsAsUniqueColumn(); }
    const IColumnUnique & getRelationsDictionary() const { return struct_graph.getRelationsAsUniqueColumn(); }

    ColumnPtr & getRelationsDictionaryPtr() { return struct_graph.getRelationsAsUniqueColumnPtr(); }
    const ColumnPtr & getRelationsDictionaryPtr() const { return struct_graph.getRelationsAsUniqueColumnPtr(); }

    Ptr convertToMultipleIfNeed(size_t offset, size_t limit) const;
};

}
