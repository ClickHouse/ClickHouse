#pragma once

#include <DataTypes/IDataType.h>

namespace DB
{

/// Dynamic type allows to store values of any type inside it and to read
/// subcolumns with any type without knowing all of them in advance.
class DataTypeDynamic final : public IDataType
{
public:
    static constexpr bool is_parametric = true;

    /// Don't change this constant, it can break backward compatibility.
    static constexpr size_t DEFAULT_MAX_DYNAMIC_TYPES = 32;

    explicit DataTypeDynamic(size_t max_dynamic_types_ = DEFAULT_MAX_DYNAMIC_TYPES);

    TypeIndex getTypeId() const override { return TypeIndex::Dynamic; }
    const char * getFamilyName() const override { return "Dynamic"; }

    bool isParametric() const override { return true; }
    bool canBeInsideNullable() const override { return false; }
    bool supportsSparseSerialization() const override { return false; }
    bool canBeInsideSparseColumns() const override { return false; }
    bool isComparable() const override { return true; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override;

    /// 2 Dynamic types with different max_dynamic_types parameters are considered as different.
    bool equals(const IDataType & rhs) const override
    {
        if (const auto * rhs_dynamic_type = typeid_cast<const DataTypeDynamic *>(&rhs))
            return max_dynamic_types == rhs_dynamic_type->max_dynamic_types;
        return false;
    }

    bool haveSubtypes() const override { return false; }

    bool hasDynamicSubcolumnsData() const override { return true; }
    std::unique_ptr<SubstreamData> getDynamicSubcolumnData(std::string_view subcolumn_name, const SubstreamData & data, bool throw_if_null) const override;

    size_t getMaxDynamicTypes() const { return max_dynamic_types; }

private:
    SerializationPtr doGetDefaultSerialization() const override;
    String doGetName() const override;

    size_t max_dynamic_types;
};

}

