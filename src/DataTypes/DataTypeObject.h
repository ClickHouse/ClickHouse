#pragma once

#include <Core/Field.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/IDataType.h>
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

    /// Build the combined subcolumn name for a given key, e.g. "mykey" -> "@`mykey`".
    /// The key is back-quoted to handle special characters (dots, backticks, etc.).
    static String getCombinedSubcolumnName(const String & key);

    explicit DataTypeObject(
        const SchemaFormat & schema_format_,
        std::unordered_map<String, DataTypePtr> typed_paths_ = {},
        std::unordered_set<String> paths_to_skip_ = {},
        std::vector<String> path_regexps_to_skip_ = {},
        size_t max_dynamic_paths_ = DEFAULT_MAX_DYNAMIC_PATHS,
        size_t max_dynamic_types_ = DataTypeDynamic::DEFAULT_MAX_DYNAMIC_TYPES,
        DataTypePtr default_path_type_ = nullptr);

    DataTypeObject(const SchemaFormat & schema_format_, size_t max_dynamic_paths_, size_t max_dynamic_types_, DataTypePtr default_path_type_ = nullptr);

    const char * getFamilyName() const override { return "Object"; }
    String doGetName() const override;
    TypeIndex getTypeId() const override { return TypeIndex::Object; }

    MutableColumnPtr createColumn() const override;

    Field getDefault() const override { return Object(); }

    void insertDefaultInto(IColumn & column) const override;
    bool isDefaultInsertTrivial() const override;

    bool isParametric() const override { return true; }
    bool canBeInsideNullable() const override { return true; }
    bool supportsSparseSerialization() const override { return false; }
    bool canBeInsideSparseColumns() const override { return false; }
    bool isComparable() const override { return true; }
    bool isComparableForEquality() const override { return true; }
    bool haveSubtypes() const override { return false; }

    bool equals(const IDataType & rhs) const override;

    void updateHashImpl(SipHash & hash) const override;

    /// Children are the typed paths followed by the default path type (when set), so that the
    /// default path type is visible to type-tree walks (validators such as
    /// `enable_time_time64_type` / `allow_suspicious_low_cardinality_types`).
    size_t getNumberOfChildren() const override { return sorted_typed_paths.size() + (default_path_type ? 1 : 0); }
    const DataTypePtr & getChild(size_t index) const override
    {
        chassert(index < getNumberOfChildren());
        if (index < sorted_typed_paths.size())
            return sorted_typed_paths[index].second;
        return default_path_type;
    }

    bool hasDynamicSubcolumnsData() const override { return true; }
    bool hasDynamicStructure() const override { return true; }
    std::unique_ptr<SubcolumnInfo> getDynamicSubcolumnInfo(std::string_view subcolumn_name, const SubstreamData & data, size_t initial_array_level, bool throw_if_null) const override;

    SerializationPtr doGetSerialization(const SerializationInfoSettings & settings) const override;

    const SchemaFormat & getSchemaFormat() const { return schema_format; }
    String getSchemaFormatString() const;
    const std::unordered_map<String, DataTypePtr> & getTypedPaths() const { return typed_paths; }

    /// Returns a map from typed-path name to its default serialization, resolved once.
    /// Used by mergedJSONPatch to serialize/deserialize typed-path values without a type tag.
    UnorderedMapWithMemoryTracking<String, SerializationPtr> getTypedPathSerializations() const;
    const std::unordered_set<String> & getPathsToSkip() const { return paths_to_skip; }
    const std::vector<String> & getPathRegexpsToSkip() const { return path_regexps_to_skip; }

    size_t getMaxDynamicTypes() const { return max_dynamic_types; }
    size_t getMaxDynamicPaths() const { return max_dynamic_paths; }

    /// Default data type of all non-typed paths (JSON(DEFAULT PATH TYPE T)). nullptr if not specified.
    const DataTypePtr & getDefaultPathType() const { return default_path_type; }
    bool hasDefaultPathType() const { return default_path_type != nullptr; }

    DataTypePtr getTypeOfNestedObjects() const;
    DataTypePtr getDynamicType() const;
    /// Type of runtime (dynamic) path columns: `Dynamic` for ordinary JSON and `Variant(T)` for
    /// JSON(DEFAULT PATH TYPE T), where the NULL discriminator marks a path missing in the row.
    static DataTypePtr getTypeOfRuntimePaths(const DataTypePtr & default_path_type);

    /// Extracts a combined literal+sub-object subcolumn for the given path as Dynamic.
    /// When skip_null_typed_paths is true, typed paths with NULL values are not considered present:
    /// a parent whose typed descendants are all NULL is absent, while a NULL typed literal still
    /// surfaces a non-empty sub-object. Typed literals are cast to Dynamic so the result type is
    /// always Dynamic (including the empty-sub-object early return).
    ColumnPtr extractCombinedSubcolumn(const String & path, const ColumnPtr & column, bool skip_null_typed_paths) const;

    /// Shared data has type Array(Tuple(String, String)).
    static const DataTypePtr & getTypeOfSharedData();
    /// Array(Tuple(String, T)) for JSON(DEFAULT PATH TYPE T), Array(Tuple(String, String)) if `default_path_type` is nullptr.
    /// Creates a new type on each call, callers on hot paths should cache it.
    static DataTypePtr getTypeOfSharedData(const DataTypePtr & default_path_type);

private:
    DataTypePtr doCloneWithChildren(const DataTypes & new_children) const override;

    /// Don't change these constants, it can break backward compatibility.
    static constexpr size_t NESTED_OBJECT_MAX_DYNAMIC_PATHS_REDUCE_FACTOR = 4;
    static constexpr size_t NESTED_OBJECT_MAX_DYNAMIC_TYPES_REDUCE_FACTOR = 2;

    SchemaFormat schema_format;
    /// Set of paths with types that were specified in type declaration.
    std::unordered_map<String, DataTypePtr> typed_paths;
    /// The same typed paths in the canonical order `getChild` and `doCloneWithChildren` agree on.
    /// `typed_paths` is a hash map, so an order has to be imposed rather than read off it, and it is
    /// imposed once here so that enumerating the children does not sort or allocate.
    std::vector<std::pair<String, DataTypePtr>> sorted_typed_paths;
    /// Set of paths that should be skipped during data parsing.
    std::unordered_set<String> paths_to_skip;
    /// List of regular expressions that should be used to skip paths during data parsing.
    std::vector<String> path_regexps_to_skip;
    /// Limit on the number of paths that can be stored as subcolumn.
    size_t max_dynamic_paths;
    /// Limit of dynamic types that should be used for Dynamic columns.
    size_t max_dynamic_types;
    /// Default data type of all non-typed paths (JSON(DEFAULT PATH TYPE T)).
    DataTypePtr default_path_type;
};


}
