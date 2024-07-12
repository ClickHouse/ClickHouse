#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeDynamic.h>
#include <Core/Field.h>
#include <Columns/ColumnObjectDeprecated.h>
#include <Common/re2.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NOT_IMPLEMENTED;
}

class DataTypeObject : public IDataType
{
public:
    enum class SchemaFormat
    {
        JSON = 0,
    };

    /// Don't change these constants, it can break backward compatibility.
    static constexpr size_t DEFAULT_MAX_SEPARATELY_STORED_PATHS = 1000;
    static constexpr size_t NESTED_OBJECT_MAX_DYNAMIC_PATHS_REDUCE_FACTOR = 4;
    static constexpr size_t NESTED_OBJECT_MAX_DYNAMIC_TYPES_REDUCE_FACTOR = 2;

    DataTypeObject(
        const SchemaFormat & schema_format_,
        const std::unordered_map<String, DataTypePtr> & typed_paths_ = {},
        const std::unordered_set<String> & paths_to_skip_ = {},
        const std::vector<String> & path_prefixes_to_skip_ = {},
        const std::vector<String> & path_regexps_to_skip_ = {},
        size_t max_dynamic_paths_ = DEFAULT_MAX_SEPARATELY_STORED_PATHS,
        size_t max_dynamic_types_ = DataTypeDynamic::DEFAULT_MAX_DYNAMIC_TYPES);

    DataTypeObject(const SchemaFormat & schema_format_, size_t max_dynamic_paths_, size_t max_dynamic_types_);

    const char * getFamilyName() const override { return "Object"; }
    String doGetName() const override;
    TypeIndex getTypeId() const override { return TypeIndex::Object; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override
    {
        throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method getDefault() is not implemented for data type {}", getName());
    }

    bool isParametric() const override { return true; }
    bool canBeInsideNullable() const override { return false; }
    bool supportsSparseSerialization() const override { return false; }
    bool canBeInsideSparseColumns() const override { return false; }
    bool isComparable() const override { return false; }
    bool haveSubtypes() const override { return false; }

    bool equals(const IDataType & rhs) const override;

    bool hasDynamicSubcolumnsData() const override { return true; }
    std::unique_ptr<SubstreamData> getDynamicSubcolumnData(std::string_view subcolumn_name, const SubstreamData & data, bool throw_if_null) const override;

    SerializationPtr doGetDefaultSerialization() const override;

    const SchemaFormat & getSchemaFormat() const { return schema_format; }
    const std::unordered_map<String, DataTypePtr> & getTypedPaths() const { return typed_paths; }
    const std::unordered_set<String> & getPathsToSkip() const { return paths_to_skip; }
    const std::vector<String> & getPathPrefixesToSkip() const { return path_prefixes_to_skip; }
    const std::vector<String> & getPathRegexpsToSkip() const { return path_regexps_to_skip; }

    size_t getMaxDynamicTypes() const { return max_dynamic_types; }
    size_t getMaxDynamicPaths() const { return max_dynamic_paths; }

private:
    SchemaFormat schema_format;
    std::unordered_map<String, DataTypePtr> typed_paths;
    std::unordered_set<String> paths_to_skip;
    std::vector<String> path_prefixes_to_skip;
    std::vector<String> path_regexps_to_skip;
    size_t max_dynamic_paths;
    size_t max_dynamic_types;
};

}
