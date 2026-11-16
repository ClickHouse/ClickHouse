#pragma once

#include <DataTypes/IDataType.h>


namespace DB
{

/** Map data type.
  * Map is implemented as two arrays of keys and values.
  * Serialization of type 'Map(K, V)' is similar to serialization.
  * of 'Array(Tuple(keys K, values V))' or in other words of 'Nested(keys K, valuev V)'.
  */
class DataTypeMap final : public IDataType
{
private:
    DataTypePtr key_type;
    DataTypePtr value_type;

    /// 'nested' is an Array(Tuple(key_type, value_type))
    DataTypePtr nested;

public:
    static constexpr bool is_parametric = true;

    /// Prefix used for dynamic subcolumn names that represent a single map key, e.g. `m.key_foo`.
    static constexpr std::string_view KEY_SUBCOLUMN_PREFIX = "key_";

    explicit DataTypeMap(const DataTypePtr & nested_);
    explicit DataTypeMap(const DataTypes & elems);
    DataTypeMap(const DataTypePtr & key_type_, const DataTypePtr & value_type_);

    TypeIndex getTypeId() const override { return TypeIndex::Map; }
    std::string doGetName() const override;
    std::string doGetPrettyName(size_t indent) const override;
    const char * getFamilyName() const override { return "Map"; }

    bool canBeInsideNullable() const override { return false; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    bool equals(const IDataType & rhs) const override;
    bool isComparable() const override { return key_type->isComparable() && value_type->isComparable(); }
    bool isParametric() const override { return true; }
    bool haveSubtypes() const override { return true; }
    DataTypePtr getNormalizedType() const override
    {
        return std::make_shared<DataTypeMap>(key_type->getNormalizedType(), value_type->getNormalizedType());
    }
    const DataTypePtr & getKeyType() const { return key_type; }
    const DataTypePtr & getValueType() const { return value_type; }

    void updateHashImpl(SipHash & hash) const override;
    DataTypes getKeyValueTypes() const { return {key_type, value_type}; }
    const DataTypePtr & getNestedType() const { return nested; }
    DataTypePtr getNestedTypeWithUnnamedTuple() const;
    DataTypePtr getNestedDataType() const;

    SerializationPtr doGetSerialization(const SerializationInfoSettings & settings) const override;

    static bool isValidKeyType(DataTypePtr key_type);

    void forEachChild(const ChildCallback & callback) const override;

    bool hasDynamicSubcolumnsData() const override { return true; }
    bool hasDynamicStructure() const override { return key_type->hasDynamicStructure() || value_type->hasDynamicStructure(); }
    std::unique_ptr<SubstreamData> getDynamicSubcolumnData(std::string_view subcolumn_name, const SubstreamData & data, size_t initial_array_level, bool throw_if_null) const override;
private:
    void assertKeyType() const;
};

}

