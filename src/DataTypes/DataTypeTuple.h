#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/** Tuple data type.
  * Used as an intermediate result when evaluating expressions.
  * Also can be used as a column - the result of the query execution.
  *
  * Tuple elements can have names.
  * If an element is unnamed, it will have automatically assigned name like '1', '2', '3' corresponding to its position.
  * Manually assigned names must not begin with digit. Names must be unique.
  *
  * All tuples with same size and types of elements are equivalent for expressions, regardless to names of elements.
  */
class DataTypeTuple final : public IDataType
{
private:
    DataTypes elems;
    Strings names;
    bool have_explicit_names;
    bool serialize_names = true;
public:
    static constexpr bool is_parametric = true;

    DataTypeTuple(const DataTypes & elems);
    DataTypeTuple(const DataTypes & elems, const Strings & names, bool serialize_names_ = true);

    static bool canBeCreatedWithNames(const Strings & names);

    TypeIndex getTypeId() const override { return TypeIndex::Tuple; }
    std::string doGetName() const override;
    const char * getFamilyName() const override { return "Tuple"; }

    bool canBeInsideNullable() const override { return false; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;
    void insertDefaultInto(IColumn & column) const override;

    bool equals(const IDataType & rhs) const override;

    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return !elems.empty(); }
    bool isComparable() const override;
    bool textCanContainOnlyValidUTF8() const override;
    bool haveMaximumSizeOfValue() const override;
    size_t getMaximumSizeOfValueInMemory() const override;
    size_t getSizeOfValueInMemory() const override;

    DataTypePtr tryGetSubcolumnType(const String & subcolumn_name) const override;
    ColumnPtr getSubcolumn(const String & subcolumn_name, const IColumn & column) const override;

    SerializationPtr getSerialization(const String & column_name, const StreamExistenceCallback & callback) const override;

    SerializationPtr getSubcolumnSerialization(
        const String & subcolumn_name, const BaseSerializationGetter & base_serialization_getter) const override;

    SerializationPtr doGetDefaultSerialization() const override;

    const DataTypes & getElements() const { return elems; }
    const Strings & getElementNames() const { return names; }

    size_t getPositionByName(const String & name) const;

    bool haveExplicitNames() const { return have_explicit_names; }
    bool serializeNames() const { return serialize_names; }

private:
    template <typename OnSuccess, typename OnContinue>
    auto getSubcolumnEntity(const String & subcolumn_name,
        const OnSuccess & on_success, const OnContinue & on_continue) const;
};

}

