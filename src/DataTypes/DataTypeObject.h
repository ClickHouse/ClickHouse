#pragma once

#include <Core/Field.h>
#include <DataTypes/DataTypeDynamic.h>
#include <DataTypes/IDataType.h>
#include <Common/StringHashForHeterogeneousLookup.h>
#include <Common/UnorderedMapWithMemoryTracking.h>

namespace re2
{
class RE2;
}

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

    /// The typed paths by name; the hasher takes a string_view, so a prefix of a name is looked up without a copy.
    using TypedPathsMap = std::unordered_map<String, DataTypePtr, StringHashForHeterogeneousLookup, std::equal_to<>>;

    explicit DataTypeObject(
        const SchemaFormat & schema_format_,
        TypedPathsMap typed_paths_ = {},
        std::unordered_set<String> paths_to_skip_ = {},
        std::vector<String> path_regexps_to_skip_ = {},
        size_t max_dynamic_paths_ = DEFAULT_MAX_DYNAMIC_PATHS,
        size_t max_dynamic_types_ = DataTypeDynamic::DEFAULT_MAX_DYNAMIC_TYPES,
        std::unordered_set<String> shared_typed_paths_ = {},
        std::vector<String> shared_data_path_regexps_ = {});

    DataTypeObject(const SchemaFormat & schema_format_, size_t max_dynamic_paths_, size_t max_dynamic_types_);

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
    /// Equal except for the set of SHARED typed paths. Such types have identical columns.
    bool equalsIgnoringSharedTypedPaths(const DataTypeObject & rhs) const;
    /// Equal except for the SHARED REGEXP rules, which change only where new dynamic paths are placed.
    bool equalsExceptSharedDataPathRegexps(const IDataType & rhs) const;

    void updateHashImpl(SipHash & hash) const override;

    size_t getNumberOfChildren() const override { return sorted_typed_paths.size(); }
    const DataTypePtr & getChild(size_t index) const override
    {
        chassert(index < sorted_typed_paths.size());
        return sorted_typed_paths[index].second;
    }

    bool hasDynamicSubcolumnsData() const override { return true; }
    bool hasDynamicStructure() const override { return true; }
    std::unique_ptr<SubcolumnInfo> getDynamicSubcolumnInfo(std::string_view subcolumn_name, const SubstreamData & data, size_t initial_array_level, bool throw_if_null) const override;
    void forEachKnownDynamicSubcolumnName(const SubcolumnNameCallback & callback, size_t array_level) const override;

    SerializationPtr doGetSerialization(const SerializationInfoSettings & settings) const override;

    const SchemaFormat & getSchemaFormat() const { return schema_format; }
    String getSchemaFormatString() const;
    const TypedPathsMap & getTypedPaths() const { return typed_paths; }
    /// Typed paths declared SHARED: stored in the typed shared data of a part instead of separate streams.
    const std::unordered_set<String> & getSharedTypedPaths() const { return shared_typed_paths; }
    bool hasSharedTypedPaths() const { return !shared_typed_paths.empty(); }
    bool isSharedTypedPath(const String & path) const { return shared_typed_paths.contains(path); }

    /// Returns a map from typed-path name to its default serialization, resolved once.
    /// Used by mergedJSONPatch to serialize/deserialize typed-path values without a type tag.
    UnorderedMapWithMemoryTracking<String, SerializationPtr> getTypedPathSerializations() const;
    const std::unordered_set<String> & getPathsToSkip() const { return paths_to_skip; }
    const std::vector<String> & getPathRegexpsToSkip() const { return path_regexps_to_skip; }
    const std::vector<String> & getSharedDataPathRegexps() const { return shared_data_path_regexps; }
    const std::shared_ptr<const re2::RE2> & getSharedDataPathMatcher() const { return shared_data_path_matcher; }

    size_t getMaxDynamicTypes() const { return max_dynamic_types; }
    size_t getMaxDynamicPaths() const { return max_dynamic_paths; }

    DataTypePtr getTypeOfNestedObjects() const;
    DataTypePtr getDynamicType() const;

    /// Extracts a combined literal+sub-object subcolumn for the given path as Dynamic.
    /// When skip_null_typed_paths is true, typed paths with NULL values are not considered present:
    /// a parent whose typed descendants are all NULL is absent, while a NULL typed literal still
    /// surfaces a non-empty sub-object. Typed literals are cast to Dynamic so the result type is
    /// always Dynamic (including the empty-sub-object early return).
    ColumnPtr extractCombinedSubcolumn(const String & path, const ColumnPtr & column, bool skip_null_typed_paths) const;

    /// Shared data has type Array(Tuple(String, String)).
    static const DataTypePtr & getTypeOfSharedData();

private:
    DataTypePtr doCloneWithChildren(const DataTypes & new_children) const override;

    /// Equal except for both path placement rules: the SHARED marks of typed paths and the SHARED REGEXP rules.
    bool equalsExceptPathPlacement(const DataTypeObject & rhs) const;

    /// Don't change these constants, it can break backward compatibility.
    static constexpr size_t NESTED_OBJECT_MAX_DYNAMIC_PATHS_REDUCE_FACTOR = 4;
    static constexpr size_t NESTED_OBJECT_MAX_DYNAMIC_TYPES_REDUCE_FACTOR = 2;

    SchemaFormat schema_format;
    /// Set of paths with types that were specified in type declaration.
    TypedPathsMap typed_paths;
    /// The same typed paths in the canonical order `getChild` and `doCloneWithChildren` agree on.
    /// `typed_paths` is a hash map, so an order has to be imposed rather than read off it, and it is
    /// imposed once here so that enumerating the children does not sort or allocate.
    std::vector<std::pair<String, DataTypePtr>> sorted_typed_paths;
    /// The typed paths declared `SHARED`.
    std::unordered_set<String> shared_typed_paths;
    /// Set of paths that should be skipped during data parsing.
    std::unordered_set<String> paths_to_skip;
    /// List of regular expressions that should be used to skip paths during data parsing.
    std::vector<String> path_regexps_to_skip;
    /// List of regular expressions for paths that are always stored in shared data.
    std::vector<String> shared_data_path_regexps;
    /// All shared data path regexps combined into one, nullptr if there are none.
    std::shared_ptr<const re2::RE2> shared_data_path_matcher;
    /// Limit on the number of paths that can be stored as subcolumn.
    size_t max_dynamic_paths;
    /// Limit of dynamic types that should be used for Dynamic columns.
    size_t max_dynamic_types;
};


/// Returns true if JSON types (possibly inside Array or Nullable) differ only in SHARED REGEXP.
bool isJSONSharedDataPathRegexpsOnlyChange(const IDataType & from, const IDataType & to);

}
