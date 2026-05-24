#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/// A named, ordered collection of typed fields stored as a single physical
/// stream (one binary record per row), unlike Tuple which stores one stream
/// per element. In memory it is a ColumnTuple of the field columns.
class DataTypeRow final : public IDataType
{
private:
    DataTypes elems;
    Strings names;

public:
    static constexpr bool is_parametric = true;

    DataTypeRow(const DataTypes & elems_, const Strings & names_);

    TypeIndex getTypeId() const override { return TypeIndex::Row; }
    /// In-memory representation reuses ColumnTuple, so the column type is Tuple.
    TypeIndex getColumnType() const override { return TypeIndex::Tuple; }
    std::string doGetName() const override;
    const char * getFamilyName() const override { return "Row"; }

    bool canBeInsideNullable() const override { return false; }
    bool supportsSparseSerialization() const override { return false; }
    bool canBeInsideSparseColumns() const override { return false; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;
    void insertDefaultInto(IColumn & column) const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return !elems.empty(); }
    bool isComparable() const override { return false; }
    bool textCanContainOnlyValidUTF8() const override;
    bool haveMaximumSizeOfValue() const override;
    size_t getMaximumSizeOfValueInMemory() const override;
    size_t getSizeOfValueInMemory() const override;

    SerializationPtr doGetSerialization(const SerializationInfoSettings & settings) const override;

    DataTypePtr getNormalizedType() const override;
    const DataTypes & getElements() const { return elems; }
    const Strings & getElementNames() const { return names; }

    void updateHashImpl(SipHash & hash) const override;

    void forEachChild(const ChildCallback & callback) const override;
};

}
