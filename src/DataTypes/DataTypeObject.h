#pragma once

#include <DataTypes/IDataType.h>
#include <DataTypes/DataTypeDynamic.h>
#include <Core/Field.h>
#include <Columns/ColumnObjectDeprecated.h>
#include <Common/re2.h>


namespace DB
{

class DataTypeObject : public IDataType
{
public:
    enum class SchemaFormat
    {
        JSON = 0,
    };

    /// Don't change this constant, it can break backward compatibility.
    static constexpr size_t DEFAULT_MAX_SEPARATELY_STORED_PATHS = 1024;

    explicit DataTypeObject(
        const SchemaFormat & schema_format_,
        std::unordered_map<String, DataTypePtr> typed_paths_ = {},
        std::unordered_set<String> paths_to_skip_ = {},
        std::vector<String> path_regexps_to_skip_ = {},
        size_t max_dynamic_paths_ = DEFAULT_MAX_SEPARATELY_STORED_PATHS,
        size_t max_dynamic_types_ = DataTypeDynamic::DEFAULT_MAX_DYNAMIC_TYPES);

    DataTypeObject(const SchemaFormat & schema_format_, size_t max_dynamic_paths_, size_t max_dynamic_types_);

    const char * getFamilyName() const override { return "Object"; }
    String doGetName() const override;
    TypeIndex getTypeId() const override { return TypeIndex::Object; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override { return Object(); }

    bool isParametric() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool supportsSparseSerialization() const override { return false; }
    bool canBeInsideSparseColumns() const override { return false; }
    bool isComparable() const override { return true; }
    bool isComparableForEquality() const override { return true; }
    bool haveSubtypes() const override { return false; }

    bool equals(const IDataType & rhs) const override;

    void updateHashImpl(SipHash & hash) const override;

    void forEachChild(const ChildCallback &) const override;

    bool hasDynamicSubcolumnsData() const override { return true; }
    std::unique_ptr<SubstreamData> getDynamicSubcolumnData(std::string_view subcolumn_name, const SubstreamData & data, bool throw_if_null) const override;

    SerializationPtr doGetDefaultSerialization() const override;

    const SchemaFormat & getSchemaFormat() const { return schema_format; }
    const std::unordered_map<String, DataTypePtr> & getTypedPaths() const { return typed_paths; }
    const std::unordered_set<String> & getPathsToSkip() const { return paths_to_skip; }
    const std::vector<String> & getPathRegexpsToSkip() const { return path_regexps_to_skip; }

    size_t getMaxDynamicTypes() const { return max_dynamic_types; }
    size_t getMaxDynamicPaths() const { return max_dynamic_paths; }

    DataTypePtr getTypeOfNestedObjects() const;

    /// Shared data has type Array(Tuple(String, String)).
    static const DataTypePtr & getTypeOfSharedData();

private:
    /// Don't change these constants, it can break backward compatibility.
    static constexpr size_t NESTED_OBJECT_MAX_DYNAMIC_PATHS_REDUCE_FACTOR = 4;
    static constexpr size_t NESTED_OBJECT_MAX_DYNAMIC_TYPES_REDUCE_FACTOR = 2;

    SchemaFormat schema_format;
    /// Set of paths with types that were specified in type declaration.
    std::unordered_map<String, DataTypePtr> typed_paths;
    /// Set of paths that should be skipped during data parsing.
    std::unordered_set<String> paths_to_skip;
    /// List of regular expressions that should be used to skip paths during data parsing.
    std::vector<String> path_regexps_to_skip;
    /// Limit on the number of paths that can be stored as subcolumn.
    size_t max_dynamic_paths;
    /// Limit of dynamic types that should be used for Dynamic columns.
    size_t max_dynamic_types;
};

}
