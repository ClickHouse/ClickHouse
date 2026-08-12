#pragma once

#include <Core/Field.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/IDataType.h>
#include <DataTypes/JSONPathRegexpMatcher.h>
#include <Common/UnorderedMapWithMemoryTracking.h>


namespace DB
{

class DataTypeObject final : public IDataType
{
public:
    enum class SchemaFormat
    {
        JSON = 0,
    };

    static constexpr size_t MAX_TYPED_PATHS = 1000;
    static constexpr size_t MAX_DYNAMIC_PATHS_LIMIT = 10000;
    /// Don't change this constant, it can break backward compatibility.
    static constexpr size_t DEFAULT_MAX_DYNAMIC_PATHS = 1024;
    static constexpr const char * SPECIAL_SUBCOLUMN_NAME_FOR_DISTINCT_PATHS_CALCULATION = "__special_subcolumn_name_for_distinct_paths_calculation";

    /// Prefix character for sub-object subcolumns, e.g. "^`some`.path.path".
    static constexpr char SUB_OBJECT_SUBCOLUMN_PREFIX = '^';
    /// Prefix character for combined literal+sub-object subcolumns, e.g. "@`some`.path.path".
    static constexpr char COMBINED_SUBCOLUMN_PREFIX = '@';

    explicit DataTypeObject(
        const SchemaFormat & schema_format_,
        std::unordered_map<String, DataTypePtr> typed_paths_ = {},
        std::unordered_set<String> paths_to_skip_ = {},
        std::vector<String> path_regexps_to_skip_ = {},
        size_t max_dynamic_paths_ = DEFAULT_MAX_DYNAMIC_PATHS,
        size_t max_dynamic_types_ = DataTypeDynamic::DEFAULT_MAX_DYNAMIC_TYPES,
        std::vector<JSONPathRegexpRule> shared_data_path_rules_ = {},
        String shared_data_path_prefix_ = {});

    DataTypeObject(const SchemaFormat & schema_format_, size_t max_dynamic_paths_, size_t max_dynamic_types_);

    const char * getFamilyName() const override { return "Object"; }
    String doGetName() const override;
    TypeIndex getTypeId() const override { return TypeIndex::Object; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override { return Object(); }

    void insertDefaultInto(IColumn & column) const override;

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
    bool hasDynamicStructure() const override { return true; }
    std::unique_ptr<SubstreamData> getDynamicSubcolumnData(std::string_view subcolumn_name, const SubstreamData & data, size_t initial_array_level, bool throw_if_null) const override;

    SerializationPtr doGetSerialization(const SerializationInfoSettings & settings) const override;

    const SchemaFormat & getSchemaFormat() const { return schema_format; }
    String getSchemaFormatString() const;
    const std::unordered_map<String, DataTypePtr> & getTypedPaths() const { return typed_paths; }

    /// Returns a map from typed-path name to its default serialization, resolved once.
    /// Used by mergedJSONPatch to serialize/deserialize typed-path values without a type tag.
    UnorderedMapWithMemoryTracking<String, SerializationPtr> getTypedPathSerializations() const;
    const std::unordered_set<String> & getPathsToSkip() const { return paths_to_skip; }
    const std::vector<String> & getPathRegexpsToSkip() const { return path_regexps_to_skip; }
    /// Paths matching one of these rules are always stored in shared data and are never promoted
    /// to a dedicated dynamic-path subcolumn, regardless of the `max_dynamic_paths` budget.
    const std::vector<JSONPathRegexpRule> & getSharedDataPathRules() const { return shared_data_path_rules; }
    const JSONPathRegexpMatcherPtr & getSharedDataPathMatcher() const { return shared_data_path_matcher; }
    const String & getSharedDataPathPrefix() const { return shared_data_path_prefix; }

    size_t getMaxDynamicTypes() const { return max_dynamic_types; }
    size_t getMaxDynamicPaths() const { return max_dynamic_paths; }

    DataTypePtr getTypeOfNestedObjects() const;
    DataTypePtr getDynamicType() const;

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
    /// Canonical persisted rules and their immutable compiled matcher.
    std::vector<JSONPathRegexpRule> shared_data_path_rules;
    JSONPathRegexpMatcherPtr shared_data_path_matcher;
    /// Root-relative prefix used by derived sub-object columns. It is empty for stored columns.
    String shared_data_path_prefix;
    /// Limit on the number of paths that can be stored as subcolumn.
    size_t max_dynamic_paths;
    /// Limit of dynamic types that should be used for Dynamic columns.
    size_t max_dynamic_types;
};

/// Returns true when two types differ only in `SHARED REGEXP` policy. This difference never changes
/// value representation and can therefore be handled without a value conversion.
bool isJSONSharedDataPathPolicyOnlyChange(const IDataType * from, const IDataType * to);

/// Returns a copy of `type` with `SHARED REGEXP` rules from corresponding `JSON` nodes in
/// `source_type` added as placement provenance. `Array`, `Nullable`, `Tuple`, `Map`, and typed `JSON`
/// paths are traversed recursively; non-JSON structure is taken from `type`. If an exact union cannot
/// fit the matcher limits (or uses incompatible root prefixes), internal provenance conservatively
/// saturates to "all untyped paths stay shared" instead of failing a merge.
DataTypePtr mergeJSONSharedDataPathRules(const DataTypePtr & type, const DataTypePtr & source_type);

/// Returns a copy of `type` whose corresponding `JSON` nodes use the `SHARED REGEXP` policy from
/// `policy_source_type`. The same nested containers as mergeJSONSharedDataPathRules are traversed.
DataTypePtr replaceJSONSharedDataPathPolicy(const DataTypePtr & type, const DataTypePtr & policy_source_type);

}
