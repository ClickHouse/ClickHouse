#include <Formats/SchemaInferenceUtils.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypeVariant.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeNothing.h>
#include <DataTypes/transformTypesRecursively.h>
#include <DataTypes/DataTypeObjectDeprecated.h>
#include <DataTypes/DataTypeFactory.h>
#include <IO/ReadBufferFromString.h>
#include <IO/ReadHelpers.h>
#include <IO/parseDateTimeBestEffort.h>
#include <IO/PeekableReadBuffer.h>
#include <IO/readFloatText.h>

#include <Core/Block.h>
#include <Common/assert_cast.h>
#include <Common/SipHash.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int TOO_DEEP_RECURSION;
    extern const int NOT_IMPLEMENTED;
    extern const int INCORRECT_DATA;
    extern const int ONLY_NULLS_WHILE_READING_SCHEMA;
}

namespace
{
    /// Special data type that represents JSON object as a set of paths and their types.
    /// It supports merging two JSON objects and creating Named Tuple from itself.
    /// It's used only for schema inference of Named Tuples from JSON objects.
    /// Example:
    /// JSON objects:
    /// "obj1" : {"a" : {"b" : 1, "c" : {"d" : 'Hello'}}, "e" : "World"}
    /// "obj2" : {"a" : {"b" : 2, "f" : [1,2,3]}, "g" : {"h" : 42}}
    /// JSONPaths type for each object:
    /// obj1 : {'a.b' : Int64, 'a.c.d' : String, 'e' : String}
    /// obj2 : {'a.b' : Int64, 'a.f' : Array(Int64), 'g.h' : Int64}
    /// Merged JSONPaths type for obj1 and obj2:
    /// obj1 ⋃ obj2 : {'a.b' : Int64, 'a.c.d' : String, 'a.f' : Array(Int64), 'e' : String, 'g.h' : Int64}
    /// Result Named Tuple:
    /// Tuple(a Tuple(b Int64, c Tuple(d String), f Array(Int64)), e String, g Tuple(h Int64))
    class DataTypeJSONPaths : public IDataTypeDummy
    {
    public:
        /// We create DataTypeJSONPaths on each row in input data, to
        /// compare and merge such types faster, we use hash map to
        /// store mapping path -> data_type. Path is a vector
        /// of path components, to use hash map we need a hash
        /// for std::vector<String>. We cannot just concatenate
        /// components with '.' and store it as a string,
        /// because components can also contain '.'
        struct PathHash
        {
            size_t operator()(const std::vector<String> & path) const
            {
                SipHash hash;
                hash.update(path.size());
                for (const auto & part : path)
                    hash.update(part);
                return hash.get64();
            }
        };

        using Paths = std::unordered_map<std::vector<String>, DataTypePtr, PathHash>;

        explicit DataTypeJSONPaths(Paths paths_) : paths(std::move(paths_))
        {
        }

        DataTypeJSONPaths() = default;

        const char * getFamilyName() const override { return "JSONPaths"; }
        String doGetName() const override { return finalize()->getName(); }
        TypeIndex getTypeId() const override { return TypeIndex::JSONPaths; }

        bool isParametric() const override
        {
            throw Exception(ErrorCodes::NOT_IMPLEMENTED, "Method isParametric is not implemented for JSONObjectForInference type");
        }

        bool equals(const IDataType & rhs) const override
        {
            if (this == &rhs)
                return true;

            if (rhs.getTypeId() != getTypeId())
                return false;

            const auto & rhs_paths = assert_cast<const DataTypeJSONPaths &>(rhs).paths;
            if (paths.size() != rhs_paths.size())
                return false;

            for (const auto & [path, type] : paths)
            {
                auto it = rhs_paths.find(path);
                if (it == rhs_paths.end() || !it->second->equals(*type))
                    return false;
            }

            return true;
        }

        bool merge(const DataTypeJSONPaths & rhs, std::function<void(DataTypePtr & type1, DataTypePtr & type2)> transform_types)
        {
            for (const auto & [rhs_path, rhs_type] : rhs.paths)
            {
                auto [it, inserted] = paths.insert({rhs_path, rhs_type});
                if (!inserted)
                {
                    auto & type = it->second;
                    /// If types are different, try to apply provided transform function.
                    if (!type->equals(*rhs_type))
                    {
                        auto rhs_type_copy = rhs_type;
                        transform_types(type, rhs_type_copy);
                        /// If types for the same path are different even after transform, we cannot merge these objects.
                        if (!type->equals(*rhs_type_copy))
                            return false;
                    }
                }
            }

            return true;
        }

        bool empty() const { return paths.empty(); }

        DataTypePtr finalize(bool use_string_type_for_ambiguous_paths = false) const
        {
            if (paths.empty())
                throw Exception(ErrorCodes::ONLY_NULLS_WHILE_READING_SCHEMA, "Cannot infer named Tuple from JSON object because object is empty");

            /// Construct a path tree from list of paths and their types and convert it to named Tuple.
            /// Example:
            /// Paths : {'a.b' : Int64, 'a.c.d' : String, 'e' : String, 'f.g' : Array(Int64), 'f.h' : String}
            /// Tree:
            ///              ┌─ 'c' ─ 'd' (String)
            ///       ┌─ 'a' ┴─ 'b' (Int64)
            /// root ─┼─ 'e' (String)
            ///       └─ 'f' ┬─ 'g' (Array(Int64))
            ///              └─ 'h' (String)
            /// Result Named Tuple:
            /// Tuple('a' Tuple('b' Int64, 'c' Tuple('d' String)), 'e' String, 'f' Tuple('g' Array(Int64), 'h' String))
            PathNode root_node;
            for (const auto & [path, type] : paths)
            {
                PathNode * current_node = &root_node;
                String current_path;
                for (const auto & name : path)
                {
                    current_path += (current_path.empty() ? "" : ".") + name;
                    current_node = &current_node->nodes[name];
                    current_node->path = current_path;
                }

                current_node->leaf_type = type;
            }

            return root_node.getType(use_string_type_for_ambiguous_paths);
        }

    private:
        struct PathNode
        {
            /// Use just map to have result tuple with names in lexicographic order.
            /// No strong reason for it, made for consistency.
            std::map<String, PathNode> nodes;
            DataTypePtr leaf_type;
            /// Store path to this node for better exception message in case of ambiguous paths.
            String path;

            DataTypePtr getType(bool use_string_type_for_ambiguous_paths) const
            {
                if (nodes.empty())
                    return leaf_type;

                Names node_names;
                node_names.reserve(nodes.size());
                DataTypes node_types;
                node_types.reserve(nodes.size());
                for (const auto & [name, node] : nodes)
                {
                    node_names.push_back(name);
                    node_types.push_back(node.getType(use_string_type_for_ambiguous_paths));
                }

                auto tuple_type = std::make_shared<DataTypeTuple>(std::move(node_types), std::move(node_names));

                /// Check if we have ambiguous paths.
                /// For example:
                /// 'a.b.c' : Int32 and 'a.b' : String
                /// Also check if leaf type is Nothing, because the next situation is possible:
                /// {"a" : {"b" : null}} -> 'a.b' : Nullable(Nothing)
                /// {"a" : {"b" : {"c" : 42}}} -> 'a.b.c' : Int32
                /// And after merge we will have ambiguous paths 'a.b.c' : Int32 and 'a.b' : Nullable(Nothing),
                /// but it's a valid case and we should ignore path 'a.b'.
                if (leaf_type && !isNothing(removeNullable(leaf_type)) && !nodes.empty())
                {
                    if (use_string_type_for_ambiguous_paths)
                        return std::make_shared<DataTypeString>();

                    throw Exception(
                        ErrorCodes::INCORRECT_DATA,
                        "JSON objects have ambiguous data: in some objects path '{}' has type '{}' and in some - '{}'. You can enable setting "
                        "input_format_json_use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects to use String type "
                        "for path '{}'",
                        path, leaf_type->getName(), tuple_type->getName(), path);
                }

                return tuple_type;
            }
        };

        Paths paths;
    };

    void updateTypeIndexes(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        type_indexes.clear();
        for (const auto & type : data_types)
            type_indexes.insert(type->getTypeId());
    }

    /// If we have both Nothing and non Nothing types, convert all Nothing types to the first non Nothing.
    /// For example if we have types [Nothing, String, Nothing] we change it to [String, String, String]
    void transformNothingSimpleTypes(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        /// Check if we have both Nothing and non Nothing types.
        if (!type_indexes.contains(TypeIndex::Nothing) || type_indexes.size() <= 1)
            return;

        DataTypePtr not_nothing_type = nullptr;
        for (const auto & type : data_types)
        {
            if (!isNothing(type))
            {
                not_nothing_type = type;
                break;
            }
        }

        for (auto & type : data_types)
        {
            if (isNothing(type))
                type = not_nothing_type;
        }

        type_indexes.erase(TypeIndex::Nothing);
    }

    /// If we have both Int64 and UInt64, convert all not-negative Int64 to UInt64,
    /// because UInt64 is inferred only in case of Int64 overflow.
    void transformIntegers(DataTypes & data_types, TypeIndexesSet & type_indexes, JSONInferenceInfo * json_info)
    {
        if (!type_indexes.contains(TypeIndex::Int64) || !type_indexes.contains(TypeIndex::UInt64))
            return;

        bool have_negative_integers = false;
        for (auto & type : data_types)
        {
            if (WhichDataType(type).isInt64())
            {
                bool is_negative = json_info && json_info->negative_integers.contains(type.get());
                have_negative_integers |= is_negative;
                if (!is_negative)
                    type = std::make_shared<DataTypeUInt64>();
            }
        }

        if (!have_negative_integers)
            type_indexes.erase(TypeIndex::Int64);
    }

    /// If we have both Int64 and Float64 types, convert all Int64 to Float64.
    void transformIntegersAndFloatsToFloats(DataTypes & data_types, TypeIndexesSet & type_indexes, JSONInferenceInfo * json_info)
    {
        bool have_floats = type_indexes.contains(TypeIndex::Float64);
        bool have_integers = type_indexes.contains(TypeIndex::Int64) || type_indexes.contains(TypeIndex::UInt64);
        if (!have_integers || !have_floats)
            return;

        for (auto & type : data_types)
        {
            WhichDataType which(type);
            if (which.isInt64() || which.isUInt64())
            {
                auto new_type = std::make_shared<DataTypeFloat64>();
                if (json_info && json_info->numbers_parsed_from_json_strings.erase(type.get()))
                    json_info->numbers_parsed_from_json_strings.insert(new_type.get());
                type = new_type;
            }
        }

        type_indexes.erase(TypeIndex::Int64);
        type_indexes.erase(TypeIndex::UInt64);
    }

    /// if setting 'try_infer_variant' is true then we convert to type variant.
    void transformVariant(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        if (checkIfTypesAreEqual(data_types))
            return;

        DataTypes variant_types;
        for (const auto & type : data_types)
        {
            if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(type.get()))
            {
                const auto & current_variants = variant_type->getVariants();
                variant_types.insert(variant_types.end(), current_variants.begin(), current_variants.end());
            }
            else
            {
                variant_types.push_back(type);
            }
        }

        auto variant_type = std::make_shared<DataTypeVariant>(variant_types);

        for (auto & type : data_types)
            type = variant_type;
        type_indexes = {TypeIndex::Variant};
    }

    /// If we have only date/datetimes types (Date/DateTime/DateTime64), convert all of them to the common type,
    /// otherwise, convert all Date, DateTime and DateTime64 to String.
    void transformDatesAndDateTimes(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        bool have_dates = type_indexes.contains(TypeIndex::Date);
        bool have_datetimes = type_indexes.contains(TypeIndex::DateTime);
        bool have_datetimes64 = type_indexes.contains(TypeIndex::DateTime64);
        bool all_dates_or_datetimes = (type_indexes.size() == (static_cast<size_t>(have_dates) + static_cast<size_t>(have_datetimes) + static_cast<size_t>(have_datetimes64)));

        if (!all_dates_or_datetimes && (have_dates || have_datetimes || have_datetimes64))
        {
            for (auto & type : data_types)
            {
                if (isDate(type) || isDateTime(type) || isDateTime64(type))
                    type = std::make_shared<DataTypeString>();
            }

            type_indexes.erase(TypeIndex::Date);
            type_indexes.erase(TypeIndex::DateTime);
            type_indexes.erase(TypeIndex::DateTime64);
            type_indexes.insert(TypeIndex::String);
            return;
        }

        for (auto & type : data_types)
        {
            if (isDate(type) && (have_datetimes || have_datetimes64))
            {
                if (have_datetimes64)
                    type = std::make_shared<DataTypeDateTime64>(9);
                else
                    type = std::make_shared<DataTypeDateTime>();
                type_indexes.erase(TypeIndex::Date);
            }
            else if (isDateTime(type) && have_datetimes64)
            {
                type = std::make_shared<DataTypeDateTime64>(9);
                type_indexes.erase(TypeIndex::DateTime);
            }
        }
    }

    /// If we have numbers (Int64/UInt64/Float64) and String types and numbers were parsed from String,
    /// convert all numbers to String.
    void transformJSONNumbersBackToString(
        DataTypes & data_types, const FormatSettings & settings, TypeIndexesSet & type_indexes, JSONInferenceInfo * json_info)
    {
        bool have_strings = type_indexes.contains(TypeIndex::String);
        bool have_numbers = type_indexes.contains(TypeIndex::Int64) || type_indexes.contains(TypeIndex::UInt64) || type_indexes.contains(TypeIndex::Float64);
        if (!have_strings || !have_numbers)
            return;

        for (auto & type : data_types)
        {
            if (isNumber(type)
                && (settings.json.read_numbers_as_strings || !json_info
                    || json_info->numbers_parsed_from_json_strings.contains(type.get())))
                type = std::make_shared<DataTypeString>();
        }

        updateTypeIndexes(data_types, type_indexes);
    }

    /// If we have both Bool and number (Int64/UInt64/Float64) types,
    /// convert all Bool to Int64/UInt64/Float64.
    void transformBoolsAndNumbersToNumbers(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        bool have_floats = type_indexes.contains(TypeIndex::Float64);
        bool have_signed_integers = type_indexes.contains(TypeIndex::Int64);
        bool have_unsigned_integers = type_indexes.contains(TypeIndex::UInt64);
        bool have_bools = type_indexes.contains(TypeIndex::UInt8);
        /// Check if we have both Bool and Integer/Float.
        if (!have_bools || (!have_signed_integers && !have_unsigned_integers && !have_floats))
            return;

        for (auto & type : data_types)
        {
            if (isBool(type))
            {
                if (have_signed_integers)
                    type = std::make_shared<DataTypeInt64>();
                else if (have_unsigned_integers)
                    type = std::make_shared<DataTypeUInt64>();
                else
                    type = std::make_shared<DataTypeFloat64>();
            }
        }

        type_indexes.erase(TypeIndex::UInt8);
    }

    /// If we have Bool and String types convert all numbers to String.
    /// It's applied only when setting input_format_json_read_bools_as_strings is enabled.
    void transformJSONBoolsAndStringsToString(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        if (!type_indexes.contains(TypeIndex::String) || !type_indexes.contains(TypeIndex::UInt8))
            return;

        for (auto & type : data_types)
        {
            if (isBool(type))
                type = std::make_shared<DataTypeString>();
        }

        type_indexes.erase(TypeIndex::UInt8);
    }

    /// If we have type Nothing/Nullable(Nothing) and some other non Nothing types,
    /// convert all Nothing/Nullable(Nothing) types to the first non Nothing.
    /// For example, when we have [Nothing, Array(Int64)] it will convert it to [Array(Int64), Array(Int64)]
    /// (it can happen when transforming complex nested types like [Array(Nothing), Array(Array(Int64))])
    void transformNothingComplexTypes(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        bool have_nothing = false;
        DataTypePtr not_nothing_type = nullptr;
        for (const auto & type : data_types)
        {
            if (isNothing(removeNullable(type)))
                have_nothing = true;
            else
                not_nothing_type = type;
        }

        if (!have_nothing || !not_nothing_type)
            return;

        for (auto & type : data_types)
        {
            if (isNothing(removeNullable(type)))
                type = not_nothing_type;
        }

        updateTypeIndexes(data_types, type_indexes);
    }

    /// If we have both Nullable and non Nullable types, make all types Nullable
    void transformNullableTypes(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        if (!type_indexes.contains(TypeIndex::Nullable))
            return;

        for (auto & type : data_types)
        {
            if (type->canBeInsideNullable())
                type = makeNullable(type);
        }

        updateTypeIndexes(data_types, type_indexes);
    }

    /// If we have unnamed Tuple with the same nested types like Tuple(Int64, Int64),
    /// convert it to Array(Int64). It's used for JSON values.
    /// For example when we had type Tuple(Int64, Nullable(Nothing)) and we
    /// transformed it to Tuple(Nullable(Int64), Nullable(Int64)) we will
    /// also transform it to Array(Nullable(Int64))
    void transformTuplesWithEqualNestedTypesToArrays(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        if (!type_indexes.contains(TypeIndex::Tuple))
            return;

        bool remove_tuple_index = true;
        for (auto & type : data_types)
        {
            if (isTuple(type))
            {
                const auto * tuple_type = assert_cast<const DataTypeTuple *>(type.get());
                if (tuple_type->haveExplicitNames())
                    return;

                if (checkIfTypesAreEqual(tuple_type->getElements()))
                    type = std::make_shared<DataTypeArray>(tuple_type->getElements().back());
                else
                    remove_tuple_index = false;
            }
        }

        if (remove_tuple_index)
            type_indexes.erase(TypeIndex::Tuple);
    }

    template <bool is_json>
    void transformInferredTypesIfNeededImpl(DataTypes & types, const FormatSettings & settings, JSONInferenceInfo * json_info = nullptr);

    /// If we have unnamed Tuple and Array types, try to convert them all to Array
    /// if there is a common type for all nested types.
    /// For example, if we have [Tuple(Nullable(Nothing), String), Array(Date), Tuple(Date, String)]
    /// it will convert them all to Array(String)
    void transformJSONTuplesAndArraysToArrays(
        DataTypes & data_types, const FormatSettings & settings, TypeIndexesSet & type_indexes, JSONInferenceInfo * json_info)
    {
        if (!type_indexes.contains(TypeIndex::Tuple))
            return;

        bool have_arrays = type_indexes.contains(TypeIndex::Array);
        bool tuple_sizes_are_equal = true;
        size_t tuple_size = 0;
        for (const auto & type : data_types)
        {
            if (isTuple(type))
            {
                const auto & tuple_type = assert_cast<const DataTypeTuple &>(*type);
                if (tuple_type.haveExplicitNames())
                    return;

                const auto & current_tuple_size = tuple_type.getElements().size();
                if (!tuple_size)
                    tuple_size = current_tuple_size;
                else
                    tuple_sizes_are_equal &= current_tuple_size == tuple_size;
            }
        }

        /// Check if we have arrays and tuples with same size.
        if (!have_arrays && !tuple_sizes_are_equal)
            return;

        DataTypes nested_types;
        for (auto & type : data_types)
        {
            if (isArray(type))
                nested_types.push_back(assert_cast<const DataTypeArray &>(*type).getNestedType());
            else if (isTuple(type))
            {
                const auto & elements = assert_cast<const DataTypeTuple &>(*type).getElements();
                for (const auto & element : elements)
                    nested_types.push_back(element);
            }
        }

        transformInferredTypesIfNeededImpl<true>(nested_types, settings, json_info);
        if (checkIfTypesAreEqual(nested_types))
        {
            for (auto & type : data_types)
            {
                if (isArray(type) || isTuple(type))
                    type = std::make_shared<DataTypeArray>(nested_types.back());
            }

            type_indexes.erase(TypeIndex::Tuple);
        }
    }

    void transformMapsAndStringsToStrings(DataTypes & data_types, TypeIndexesSet & type_indexes)
    {
        /// Check if we have both String and Map
        if (!type_indexes.contains(TypeIndex::Map) || !type_indexes.contains(TypeIndex::String))
            return;

        for (auto & type : data_types)
        {
            if (isMap(type))
                type = std::make_shared<DataTypeString>();
        }

        type_indexes.erase(TypeIndex::Map);
    }

    void mergeJSONPaths(DataTypes & data_types, TypeIndexesSet & type_indexes, const FormatSettings & settings, JSONInferenceInfo * json_info)
    {
        if (!type_indexes.contains(TypeIndex::JSONPaths))
            return;

        std::shared_ptr<DataTypeJSONPaths> merged_type = std::make_shared<DataTypeJSONPaths>();
        auto transform_func = [&](DataTypePtr & type1, DataTypePtr & type2){ transformInferredJSONTypesIfNeeded(type1, type2, settings, json_info); };
        for (auto & type : data_types)
        {
            if (const auto * json_type = typeid_cast<const DataTypeJSONPaths *>(type.get()))
                merged_type->merge(*json_type, transform_func);
        }

        for (auto & type : data_types)
        {
            if (type->getTypeId() == TypeIndex::JSONPaths)
                type = merged_type;
        }
    }

    void mergeNamedTuples(DataTypes & data_types, TypeIndexesSet & type_indexes, const FormatSettings & settings, JSONInferenceInfo * json_info)
    {
        if (!type_indexes.contains(TypeIndex::Tuple))
            return;

        /// Collect all names and their types from all named tuples.
        std::unordered_map<String, DataTypes> names_to_types;
        /// Try to save original order of element names.
        Names element_names;
        for (auto & type : data_types)
        {
            const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get());
            if (tuple_type && tuple_type->haveExplicitNames())
            {
                const auto & elements = tuple_type->getElements();
                const auto & names = tuple_type->getElementNames();
                for (size_t i = 0; i != elements.size(); ++i)
                {
                    if (!names_to_types.contains(names[i]))
                        element_names.push_back(names[i]);
                    names_to_types[names[i]].push_back(elements[i]);
                }
            }
        }

        /// Try to find common type for each tuple element with the same name.
        DataTypes element_types;
        element_types.reserve(names_to_types.size());
        for (const auto & name : element_names)
        {
            auto & types = names_to_types[name];
            transformInferredTypesIfNeededImpl<true>(types, settings, json_info);
            /// If some element have different types in different tuples, we can't do anything
            if (!checkIfTypesAreEqual(types))
                return;
            element_types.push_back(types.front());
        }

        DataTypePtr result_tuple = std::make_shared<DataTypeTuple>(element_types, element_names);

        for (auto & type : data_types)
        {
            const auto * tuple_type = typeid_cast<const DataTypeTuple *>(type.get());
            if (tuple_type && tuple_type->haveExplicitNames())
                type = result_tuple;
        }
    }

    template <bool is_json>
    void transformInferredTypesIfNeededImpl(DataTypes & types, const FormatSettings & settings, JSONInferenceInfo * json_info)
    {
        auto transform_simple_types = [&](DataTypes & data_types, TypeIndexesSet & type_indexes)
        {
            /// Remove all Nothing type if possible.
            transformNothingSimpleTypes(data_types, type_indexes);

            if (settings.try_infer_integers)
            {
                /// Transform Int64 to UInt64 if needed.
                transformIntegers(data_types, type_indexes, json_info);
                /// Transform integers to floats if needed.
                transformIntegersAndFloatsToFloats(data_types, type_indexes, json_info);
            }

            /// Transform Date to DateTime or both to String if needed.
            if (settings.try_infer_dates || settings.try_infer_datetimes)
                transformDatesAndDateTimes(data_types, type_indexes);

            if constexpr (!is_json)
            {
                if (settings.try_infer_variant)
                    transformVariant(data_types, type_indexes);
                return;
            }

            /// Check settings specific for JSON formats.

            /// Convert numbers inferred from strings back to strings if needed.
            if (settings.json.try_infer_numbers_from_strings || settings.json.read_numbers_as_strings)
                transformJSONNumbersBackToString(data_types, settings, type_indexes, json_info);

            /// Convert Bool to number (Int64/Float64) if needed.
            if (settings.json.read_bools_as_numbers)
                transformBoolsAndNumbersToNumbers(data_types, type_indexes);

            /// Convert Bool to String if needed.
            if (settings.json.read_bools_as_strings)
                transformJSONBoolsAndStringsToString(data_types, type_indexes);

            if (settings.json.try_infer_objects_as_tuples)
                mergeJSONPaths(data_types, type_indexes, settings, json_info);

            if (settings.try_infer_variant)
                transformVariant(data_types, type_indexes);

        };

        auto transform_complex_types = [&](DataTypes & data_types, TypeIndexesSet & type_indexes)
        {
            /// Make types Nullable if needed.
            transformNullableTypes(data_types, type_indexes);

            /// If we have type Nothing, it means that we had empty Array/Map while inference.
            /// If there is at least one non Nothing type, change all Nothing types to it.
            transformNothingComplexTypes(data_types, type_indexes);

            if constexpr (!is_json)
            {
                if (settings.try_infer_variant)
                    transformVariant(data_types, type_indexes);
                return;
            }

            /// Convert JSON tuples with same nested types to arrays.
            transformTuplesWithEqualNestedTypesToArrays(data_types, type_indexes);

            /// Convert JSON tuples and arrays to arrays if possible.
            transformJSONTuplesAndArraysToArrays(data_types, settings, type_indexes, json_info);

            if (settings.json.read_objects_as_strings)
                transformMapsAndStringsToStrings(data_types, type_indexes);

            if (json_info && json_info->allow_merging_named_tuples)
                mergeNamedTuples(data_types, type_indexes, settings, json_info);

            if (settings.try_infer_variant)
                transformVariant(data_types, type_indexes);
        };

        transformTypesRecursively(types, transform_simple_types, transform_complex_types);
    }

    template <bool is_json>
    DataTypePtr tryInferDataTypeForSingleFieldImpl(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info, size_t depth = 1);

    bool tryInferDate(std::string_view field)
    {
        /// Minimum length of Date text representation is 8 (YYYY-M-D) and maximum is 10 (YYYY-MM-DD)
        if (field.size() < 8 || field.size() > 10)
            return false;

        /// Check if it's just a number, and if so, don't try to infer Date from it,
        /// because we can interpret this number as a Date (for example 20000101 will be 2000-01-01)
        /// and it will lead to inferring Date instead of simple Int64/UInt64 in some cases.
        if (std::all_of(field.begin(), field.end(), isNumericASCII))
            return false;

        ReadBufferFromString buf(field);
        DayNum tmp;
        return tryReadDateText(tmp, buf, DateLUT::instance(), /*allowed_delimiters=*/"-/:") && buf.eof();
    }

    DataTypePtr tryInferDateTimeOrDateTime64(std::string_view field, const FormatSettings & settings)
    {
        /// Don't try to infer DateTime if string is too long.
        /// It's difficult to say what is the real maximum length of
        /// DateTime we can parse using BestEffort approach.
        /// 50 symbols is more or less valid limit for date times that makes sense.
        if (field.empty() || field.size() > 50)
            return nullptr;

        /// Check that we have at least one digit, don't infer datetime form strings like "Apr"/"May"/etc.
        if (!std::any_of(field.begin(), field.end(), isNumericASCII))
            return nullptr;

        /// Check if it's just a number, and if so, don't try to infer DateTime from it,
        /// because we can interpret this number as a timestamp and it will lead to
        /// inferring DateTime instead of simple Int64 in some cases.
        if (std::all_of(field.begin(), field.end(), isNumericASCII))
            return nullptr;

        ReadBufferFromString buf(field);
        Float64 tmp_float;
        /// Check if it's a float value, and if so, don't try to infer DateTime from it,
        /// because it will lead to inferring DateTime instead of simple Float64 in some cases.
        if (tryReadFloatText(tmp_float, buf) && buf.eof())
            return nullptr;

        buf.seek(0, SEEK_SET); /// Return position to the beginning
        if (!settings.try_infer_datetimes_only_datetime64)
        {
            time_t tmp;
            switch (settings.date_time_input_format)
            {
                case FormatSettings::DateTimeInputFormat::Basic:
                    if (tryReadDateTimeText(tmp, buf, DateLUT::instance(), /*allowed_date_delimiters=*/"-/:", /*allowed_time_delimiters=*/":") && buf.eof())
                        return std::make_shared<DataTypeDateTime>();
                    break;
                case FormatSettings::DateTimeInputFormat::BestEffort:
                    if (tryParseDateTimeBestEffortStrict(tmp, buf, DateLUT::instance(), DateLUT::instance("UTC"), /*allowed_date_delimiters=*/"-/:") && buf.eof())
                        return std::make_shared<DataTypeDateTime>();
                    break;
                case FormatSettings::DateTimeInputFormat::BestEffortUS:
                    if (tryParseDateTimeBestEffortUSStrict(tmp, buf, DateLUT::instance(), DateLUT::instance("UTC"), /*allowed_date_delimiters=*/"-/:") && buf.eof())
                        return std::make_shared<DataTypeDateTime>();
                    break;
            }
        }

        buf.seek(0, SEEK_SET); /// Return position to the beginning
        DateTime64 tmp;
        switch (settings.date_time_input_format)
        {
            case FormatSettings::DateTimeInputFormat::Basic:
                if (tryReadDateTime64Text(tmp, 9, buf, DateLUT::instance(), /*allowed_date_delimiters=*/"-/:", /*allowed_time_delimiters=*/":") && buf.eof())
                    return std::make_shared<DataTypeDateTime64>(9);
                break;
            case FormatSettings::DateTimeInputFormat::BestEffort:
                if (tryParseDateTime64BestEffortStrict(tmp, 9, buf, DateLUT::instance(), DateLUT::instance("UTC"), /*allowed_date_delimiters=*/"-/:") && buf.eof())
                    return std::make_shared<DataTypeDateTime64>(9);
                break;
            case FormatSettings::DateTimeInputFormat::BestEffortUS:
                if (tryParseDateTime64BestEffortUSStrict(tmp, 9, buf, DateLUT::instance(), DateLUT::instance("UTC"), /*allowed_date_delimiters=*/"-/:") && buf.eof())
                    return std::make_shared<DataTypeDateTime64>(9);
                break;
        }

        return nullptr;
    }

    template <bool is_json>
    DataTypePtr tryInferArray(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info, size_t depth)
    {
        assertChar('[', buf);
        skipWhitespaceIfAny(buf);

        DataTypes nested_types;
        bool first = true;
        bool have_invalid_nested_type = false;
        while (!buf.eof() && *buf.position() != ']')
        {
            if (!first)
            {
                /// Skip field delimiter between array elements.
                if (!checkChar(',', buf))
                    return nullptr;
                skipWhitespaceIfAny(buf);
            }
            else
                first = false;

            auto nested_type = tryInferDataTypeForSingleFieldImpl<is_json>(buf, settings, json_info, depth + 2);

            if (nested_type)
                nested_types.push_back(nested_type);
            else
                have_invalid_nested_type = true;

            skipWhitespaceIfAny(buf);
        }

        /// No ']' at the end.
        if (buf.eof())
            return nullptr;

        assertChar(']', buf);
        skipWhitespaceIfAny(buf);

        /// Nested data is invalid.
        if (have_invalid_nested_type)
            return nullptr;

        /// Empty array has type Array(Nothing)
        if (nested_types.empty())
            return std::make_shared<DataTypeArray>(std::make_shared<DataTypeNothing>());

        if (checkIfTypesAreEqual(nested_types))
            return std::make_shared<DataTypeArray>(std::move(nested_types.back()));

        /// If element types are not equal, we should try to find common type.
        /// If after transformation element types are still different, we return Tuple for JSON and
        /// nullptr for other formats (nullptr means we couldn't infer the type).
        if constexpr (is_json)
        {
            /// For JSON if we have not complete types, we should not try to transform them
            /// and return it as a Tuple.
            /// For example, if we have types [Float64, Nullable(Nothing), Float64]
            /// it can be Array(Float64) or Tuple(Float64, <some_type>, Float64) and
            /// we can't determine which one it is. But we will be able to do it later
            /// when we will have types from other rows for this column.
            /// For example, if in the next row we will have types [Nullable(Nothing), String, Float64],
            /// we can determine the type for this column as Tuple(Nullable(Float64), Nullable(String), Float64).
            for (const auto & type : nested_types)
            {
                if (!checkIfTypeIsComplete(type))
                    return std::make_shared<DataTypeTuple>(nested_types);
            }

            auto nested_types_copy = nested_types;
            transformInferredTypesIfNeededImpl<is_json>(nested_types_copy, settings, json_info);

            if (checkIfTypesAreEqual(nested_types_copy))
                return std::make_shared<DataTypeArray>(nested_types_copy.back());
            return std::make_shared<DataTypeTuple>(nested_types);
        }
        else
        {
            transformInferredTypesIfNeededImpl<is_json>(nested_types, settings);
            if (checkIfTypesAreEqual(nested_types))
                return std::make_shared<DataTypeArray>(nested_types.back());

            /// We couldn't determine common type for array element.
            return nullptr;
        }
    }

    DataTypePtr tryInferTuple(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info, size_t depth)
    {
        assertChar('(', buf);
        skipWhitespaceIfAny(buf);

        DataTypes nested_types;
        bool first = true;
        bool have_invalid_nested_type = false;
        while (!buf.eof() && *buf.position() != ')')
        {
            if (!first)
            {
                if (!checkChar(',', buf))
                    return nullptr;
                skipWhitespaceIfAny(buf);
            }
            else
                first = false;

            auto nested_type = tryInferDataTypeForSingleFieldImpl<false>(buf, settings, json_info, depth + 1);
            if (nested_type)
                nested_types.push_back(nested_type);
            else
                have_invalid_nested_type = true;

            skipWhitespaceIfAny(buf);
        }

        /// No ')' at the end.
        if (buf.eof())
            return nullptr;

        assertChar(')', buf);
        skipWhitespaceIfAny(buf);

        /// Nested data is invalid.
        if (have_invalid_nested_type || nested_types.empty())
            return nullptr;

        return std::make_shared<DataTypeTuple>(nested_types);
    }

    template <bool is_json>
    bool tryReadFloat(Float64 & value, ReadBuffer & buf, const FormatSettings & settings, bool & has_fractional)
    {
        if (is_json || settings.try_infer_exponent_floats)
            return tryReadFloatTextExt(value, buf, has_fractional);
        return tryReadFloatTextExtNoExponent(value, buf, has_fractional);
    }

    template <bool is_json>
    DataTypePtr tryInferNumber(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info)
    {
        if (buf.eof())
            return nullptr;

        Float64 tmp_float;
        bool has_fractional;
        if (settings.try_infer_integers)
        {
            /// If we read from String, we can do it in a more efficient way.
            if (auto * /*string_buf*/ _ = dynamic_cast<ReadBufferFromString *>(&buf))
            {
                /// Remember the pointer to the start of the number to rollback to it.
                /// We can safely get back to the start of the number, because we read from a string and we didn't reach eof.
                char * number_start = buf.position();

                /// NOTE: it may break parsing of tryReadFloat() != tryReadIntText() + parsing of '.'/'e'
                /// But, for now it is true
                if (tryReadFloat<is_json>(tmp_float, buf, settings, has_fractional) && has_fractional)
                    return std::make_shared<DataTypeFloat64>();

                Int64 tmp_int;
                buf.position() = number_start;
                if (tryReadIntText(tmp_int, buf))
                {
                    auto type = std::make_shared<DataTypeInt64>();
                    if (json_info && tmp_int < 0)
                        json_info->negative_integers.insert(type.get());
                    return type;
                }

                /// In case of Int64 overflow we can try to infer UInt64.
                UInt64 tmp_uint;
                buf.position() = number_start;
                if (tryReadIntText(tmp_uint, buf))
                    return std::make_shared<DataTypeUInt64>();

                return nullptr;
            }

            /// We should use PeekableReadBuffer, because we need to
            /// rollback to the start of number to parse it as integer first
            /// and then as float.
            PeekableReadBuffer peekable_buf(buf);
            PeekableReadBufferCheckpoint checkpoint(peekable_buf);

            if (tryReadFloat<is_json>(tmp_float, peekable_buf, settings, has_fractional) && has_fractional)
                return std::make_shared<DataTypeFloat64>();
            peekable_buf.rollbackToCheckpoint(/* drop= */ false);

            Int64 tmp_int;
            if (tryReadIntText(tmp_int, peekable_buf))
            {
                auto type = std::make_shared<DataTypeInt64>();
                if (json_info && tmp_int < 0)
                    json_info->negative_integers.insert(type.get());
                return type;
            }
            peekable_buf.rollbackToCheckpoint(/* drop= */ true);

            /// In case of Int64 overflow we can try to infer UInt64.
            UInt64 tmp_uint;
            if (tryReadIntText(tmp_uint, peekable_buf))
                return std::make_shared<DataTypeUInt64>();
        }
        else if (tryReadFloat<is_json>(tmp_float, buf, settings, has_fractional))
        {
            return std::make_shared<DataTypeFloat64>();
        }

        /// This is not a number.
        return nullptr;
    }

    template <bool is_json>
    DataTypePtr tryInferNumberFromStringImpl(std::string_view field, const FormatSettings & settings, JSONInferenceInfo * json_inference_info = nullptr)
    {
        ReadBufferFromString buf(field);

        if (settings.try_infer_integers)
        {
            Int64 tmp_int;
            if (tryReadIntText(tmp_int, buf) && buf.eof())
            {
                auto type = std::make_shared<DataTypeInt64>();
                if (json_inference_info && tmp_int < 0)
                    json_inference_info->negative_integers.insert(type.get());
                return type;
            }

            /// We can safely get back to the start of buffer, because we read from a string and we didn't reach eof.
            buf.position() = buf.buffer().begin();

            /// In case of Int64 overflow, try to infer UInt64
            UInt64 tmp_uint;
            if (tryReadIntText(tmp_uint, buf) && buf.eof())
                return std::make_shared<DataTypeUInt64>();
        }

        /// We can safely get back to the start of buffer, because we read from a string and we didn't reach eof.
        buf.position() = buf.buffer().begin();

        Float64 tmp;
        bool has_fractional;
        if (tryReadFloat<is_json>(tmp, buf, settings, has_fractional) && buf.eof())
            return std::make_shared<DataTypeFloat64>();

        return nullptr;
    }

    template <bool is_json>
    DataTypePtr tryInferString(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info)
    {
        String field;
        bool ok = true;
        if constexpr (is_json)
            ok = tryReadJSONStringInto(field, buf, settings.json);
        else
            ok = tryReadQuotedString(field, buf);

        if (!ok)
            return nullptr;

        skipWhitespaceIfAny(buf);

        /// If it's object key, we should just return String type.
        if constexpr (is_json)
        {
            if (json_info->is_object_key)
                return std::make_shared<DataTypeString>();
        }

        if (auto type = tryInferDateOrDateTimeFromString(field, settings))
            return type;

        if constexpr (is_json)
        {
            if (settings.json.try_infer_numbers_from_strings)
            {
                if (auto number_type = tryInferNumberFromStringImpl<true>(field, settings, json_info))
                {
                    json_info->numbers_parsed_from_json_strings.insert(number_type.get());
                    return number_type;
                }
            }
        }

        return std::make_shared<DataTypeString>();
    }

    bool tryReadJSONObject(ReadBuffer & buf, const FormatSettings & settings, DataTypeJSONPaths::Paths & paths, const std::vector<String> & path, JSONInferenceInfo * json_info, size_t depth)
    {
        if (depth > settings.max_parser_depth)
            throw Exception(ErrorCodes::TOO_DEEP_RECURSION,
                "Maximum parse depth ({}) exceeded. Consider raising max_parser_depth setting.", settings.max_parser_depth);

        assertChar('{', buf);
        skipWhitespaceIfAny(buf);
        bool first = true;
        while (!buf.eof() && *buf.position() != '}')
        {
            if (!first)
            {
                if (!checkChar(',', buf))
                    return false;
                skipWhitespaceIfAny(buf);
            }
            else
                first = false;

            String key;
            if (!tryReadJSONStringInto(key, buf, settings.json))
                return false;

            skipWhitespaceIfAny(buf);
            if (!checkChar(':', buf))
                return false;

            std::vector<String> current_path = path;
            current_path.push_back(std::move(key));

            skipWhitespaceIfAny(buf);

            if (!buf.eof() && *buf.position() == '{')
            {
                if (!tryReadJSONObject(buf, settings, paths, current_path, json_info, depth + 1))
                    return false;
            }
            else
            {
                auto value_type = tryInferDataTypeForSingleFieldImpl<true>(buf, settings, json_info, depth + 1);
                if (!value_type)
                    return false;

                paths[std::move(current_path)] = value_type;
            }

            skipWhitespaceIfAny(buf);
        }

        /// No '}' at the end.
        if (buf.eof())
            return false;

        assertChar('}', buf);
        skipWhitespaceIfAny(buf);

        /// If it was empty object and it's not root object, treat it as null, so we won't
        /// lose this path if this key contains empty object in all sample data.
        /// This case will be processed in JSONPaths type during finalize.
        if (first && !path.empty())
            paths[path] = std::make_shared<DataTypeNothing>();
        return true;
    }

    DataTypePtr tryInferJSONPaths(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info, size_t depth)
    {
        DataTypeJSONPaths::Paths paths;
        if (!tryReadJSONObject(buf, settings, paths, {}, json_info, depth))
            return nullptr;
        return std::make_shared<DataTypeJSONPaths>(std::move(paths));
    }

    template <bool is_json>
    DataTypePtr tryInferMapOrObject(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info, size_t depth)
    {
        assertChar('{', buf);
        skipWhitespaceIfAny(buf);

        DataTypes key_types;
        DataTypes value_types;
        bool first = true;
        bool have_invalid_nested_type = false;
        while (!buf.eof() && *buf.position() != '}')
        {
            if (!first)
            {
                if (!checkChar(',', buf))
                    return nullptr;
                skipWhitespaceIfAny(buf);
            }
            else
                first = false;

            DataTypePtr key_type;
            if constexpr (is_json)
            {
                /// For JSON key type must be String.
                json_info->is_object_key = true;
                key_type = tryInferString<is_json>(buf, settings, json_info);
                json_info->is_object_key = false;
            }
            else
            {
                key_type = tryInferDataTypeForSingleFieldImpl<is_json>(buf, settings, nullptr, depth + 1);
            }

            if (key_type)
                key_types.push_back(key_type);
            else
                have_invalid_nested_type = true;

            skipWhitespaceIfAny(buf);
            if (!checkChar(':', buf))
                return nullptr;
            skipWhitespaceIfAny(buf);

            auto value_type = tryInferDataTypeForSingleFieldImpl<is_json>(buf, settings, json_info, depth + 1);
            if (value_type)
                value_types.push_back(value_type);
            else
                have_invalid_nested_type = true;
            skipWhitespaceIfAny(buf);
        }

        /// No '}' at the end.
        if (buf.eof())
            return nullptr;

        assertChar('}', buf);
        skipWhitespaceIfAny(buf);

        /// Nested data is invalid.
        if (have_invalid_nested_type)
            return nullptr;

        if (key_types.empty())
        {
            if constexpr (is_json)
            {
                if (settings.json.allow_deprecated_object_type)
                    return std::make_shared<DataTypeObjectDeprecated>("json", true);
            }

            /// Empty Map is Map(Nothing, Nothing)
            return std::make_shared<DataTypeMap>(std::make_shared<DataTypeNothing>(), std::make_shared<DataTypeNothing>());
        }

        if constexpr (is_json)
        {
            if (settings.json.allow_deprecated_object_type)
                return std::make_shared<DataTypeObjectDeprecated>("json", true);

            if (settings.json.read_objects_as_strings)
                return std::make_shared<DataTypeString>();

            transformInferredTypesIfNeededImpl<is_json>(value_types, settings, json_info);
            if (!checkIfTypesAreEqual(value_types))
                return nullptr;

            return std::make_shared<DataTypeMap>(key_types.back(), value_types.back());
        }

        if (!checkIfTypesAreEqual(key_types))
            transformInferredTypesIfNeededImpl<is_json>(key_types, settings);
        if (!checkIfTypesAreEqual(value_types))
            transformInferredTypesIfNeededImpl<is_json>(value_types, settings);

        if (!checkIfTypesAreEqual(key_types) || !checkIfTypesAreEqual(value_types))
            return nullptr;

        auto key_type = removeNullable(key_types.back());
        if (!DataTypeMap::isValidKeyType(key_type))
            return nullptr;

        return std::make_shared<DataTypeMap>(key_type, value_types.back());
    }

    template <bool is_json>
    DataTypePtr tryInferDataTypeForSingleFieldImpl(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info, size_t depth)
    {
        if (depth > settings.max_parser_depth)
            throw Exception(ErrorCodes::TOO_DEEP_RECURSION,
                "Maximum parse depth ({}) exceeded. Consider raising max_parser_depth setting.", settings.max_parser_depth);

        skipWhitespaceIfAny(buf);

        if (buf.eof())
            return nullptr;

        /// Array [field1, field2, ...]
        if (*buf.position() == '[')
            return tryInferArray<is_json>(buf, settings, json_info, depth);

        /// Tuple (field1, field2, ...), if format is not JSON
        if constexpr (!is_json)
        {
            if (*buf.position() == '(')
                return tryInferTuple(buf, settings, json_info, depth);
        }

        /// Map/Object for JSON { key1 : value1, key2 : value2, ...}
        if (*buf.position() == '{')
        {
            if constexpr (is_json)
            {
                if (!settings.json.allow_deprecated_object_type && settings.json.try_infer_objects_as_tuples)
                    return tryInferJSONPaths(buf, settings, json_info, depth);
            }

            return tryInferMapOrObject<is_json>(buf, settings, json_info, depth);
        }

        /// String
        char quote = is_json ? '"' : '\'';
        if (*buf.position() == quote)
            return tryInferString<is_json>(buf, settings, json_info);

        /// Bool
        if (checkStringCaseInsensitive("true", buf) || checkStringCaseInsensitive("false", buf))
            return DataTypeFactory::instance().get("Bool");

        /// Null or NaN
        if (checkCharCaseInsensitive('n', buf))
        {
            if (checkStringCaseInsensitive("ull", buf))
            {
                if (settings.schema_inference_make_columns_nullable == 0)
                    return std::make_shared<DataTypeNothing>();
                return makeNullable(std::make_shared<DataTypeNothing>());
            }
            if (checkStringCaseInsensitive("an", buf))
                return std::make_shared<DataTypeFloat64>();
        }

        /// Number
        return tryInferNumber<is_json>(buf, settings, json_info);
    }
}

bool checkIfTypesAreEqual(const DataTypes & types)
{
    if (types.empty())
        return true;

    for (size_t i = 1; i < types.size(); ++i)
    {
        if (!types[0]->equals(*types[i]))
            return false;
    }
    return true;
}

void transformInferredTypesIfNeeded(DataTypePtr & first, DataTypePtr & second, const FormatSettings & settings)
{
    DataTypes types = {first, second};
    transformInferredTypesIfNeededImpl<false>(types, settings, nullptr);
    first = std::move(types[0]);
    second = std::move(types[1]);
}

void transformInferredJSONTypesIfNeeded(
    DataTypePtr & first, DataTypePtr & second, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    DataTypes types = {first, second};
    transformInferredTypesIfNeededImpl<true>(types, settings, json_info);
    first = std::move(types[0]);
    second = std::move(types[1]);
}

void transformInferredJSONTypesIfNeeded(DataTypes & types, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    transformInferredTypesIfNeededImpl<true>(types, settings, json_info);
}

void transformInferredJSONTypesFromDifferentFilesIfNeeded(DataTypePtr & first, DataTypePtr & second, const FormatSettings & settings)
{
    JSONInferenceInfo json_info;
    json_info.allow_merging_named_tuples = true;
    transformInferredJSONTypesIfNeeded(first, second, settings, &json_info);
}

void transformFinalInferredJSONTypeIfNeededImpl(DataTypePtr & data_type, const FormatSettings & settings, JSONInferenceInfo * json_info, bool remain_nothing_types = false)
{
    if (!data_type)
        return;

    if (!remain_nothing_types && isNothing(data_type) && settings.json.infer_incomplete_types_as_strings)
    {
        data_type = std::make_shared<DataTypeString>();
        return;
    }

    if (const auto * nullable_type = typeid_cast<const DataTypeNullable *>(data_type.get()))
    {
        auto nested_type = nullable_type->getNestedType();
        transformFinalInferredJSONTypeIfNeededImpl(nested_type, settings, json_info, remain_nothing_types);
        data_type = std::make_shared<DataTypeNullable>(std::move(nested_type));
        return;
    }

    if (const auto * json_paths = typeid_cast<const DataTypeJSONPaths *>(data_type.get()))
    {
        /// If all objects were empty, use type String, so these JSON objects will be read as Strings.
        if (json_paths->empty() && settings.json.infer_incomplete_types_as_strings)
        {
            data_type = std::make_shared<DataTypeString>();
            return;
        }

        data_type = json_paths->finalize(settings.json.use_string_type_for_ambiguous_paths_in_named_tuples_inference_from_objects);
        transformFinalInferredJSONTypeIfNeededImpl(data_type, settings, json_info, remain_nothing_types);
        return;
    }

    if (const auto * array_type = typeid_cast<const DataTypeArray *>(data_type.get()))
    {
        auto nested_type = array_type->getNestedType();
        transformFinalInferredJSONTypeIfNeededImpl(nested_type, settings, json_info, remain_nothing_types);
        data_type = std::make_shared<DataTypeArray>(nested_type);
        return;
    }

    if (const auto * map_type = typeid_cast<const DataTypeMap *>(data_type.get()))
    {
        auto key_type = map_type->getKeyType();
        /// If all inferred Maps are empty, use type String, so these JSON objects will be read as Strings.
        if (isNothing(key_type) && settings.json.infer_incomplete_types_as_strings)
            key_type = std::make_shared<DataTypeString>();

        auto value_type = map_type->getValueType();

        transformFinalInferredJSONTypeIfNeededImpl(value_type, settings, json_info, remain_nothing_types);
        data_type = std::make_shared<DataTypeMap>(key_type, value_type);
        return;
    }

    if (const auto * tuple_type = typeid_cast<const DataTypeTuple *>(data_type.get()))
    {
        auto nested_types = tuple_type->getElements();

        if (tuple_type->haveExplicitNames())
        {
            for (auto & nested_type : nested_types)
                transformFinalInferredJSONTypeIfNeededImpl(nested_type, settings, json_info, remain_nothing_types);
            data_type = std::make_shared<DataTypeTuple>(nested_types, tuple_type->getElementNames());
            return;
        }

        /// First, try to transform nested types without final transformations to see if there is a common type.
        auto nested_types_copy = nested_types;
        transformInferredTypesIfNeededImpl<true>(nested_types_copy, settings, json_info);
        if (checkIfTypesAreEqual(nested_types_copy))
        {
            data_type = std::make_shared<DataTypeArray>(nested_types_copy.back());
            transformFinalInferredJSONTypeIfNeededImpl(data_type, settings, json_info);
            return;
        }

        /// Apply final transformation to nested types, and then try to find common type.
        for (auto & nested_type : nested_types)
            /// Don't change Nothing to String in nested types here, because we are not sure yet if it's Array or actual Tuple
            transformFinalInferredJSONTypeIfNeededImpl(nested_type, settings, json_info, /*remain_nothing_types=*/ true);

        nested_types_copy = nested_types;
        transformInferredTypesIfNeededImpl<true>(nested_types_copy, settings, json_info);
        if (checkIfTypesAreEqual(nested_types_copy))
        {
            data_type = std::make_shared<DataTypeArray>(nested_types_copy.back());
        }
        else
        {
            /// Now we should run transform one more time to convert Nothing to String if needed.
            if (!remain_nothing_types)
            {
                for (auto & nested_type : nested_types)
                    transformFinalInferredJSONTypeIfNeededImpl(nested_type, settings, json_info);
            }

            data_type = std::make_shared<DataTypeTuple>(nested_types);
        }

        return;
    }

    if (const auto * variant_type = typeid_cast<const DataTypeVariant *>(data_type.get()))
    {
        auto nested_types = variant_type->getVariants();
        for (auto & nested_type : nested_types)
            transformFinalInferredJSONTypeIfNeededImpl(nested_type, settings, json_info, remain_nothing_types);
        data_type = std::make_shared<DataTypeVariant>(nested_types);
        return;
    }
}

void transformFinalInferredJSONTypeIfNeeded(DataTypePtr & data_type, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    transformFinalInferredJSONTypeIfNeededImpl(data_type, settings, json_info);
}

DataTypePtr tryInferNumberFromString(std::string_view field, const FormatSettings & settings)
{
    return tryInferNumberFromStringImpl<false>(field, settings);
}

DataTypePtr tryInferJSONNumberFromString(std::string_view field, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    return tryInferNumberFromStringImpl<false>(field, settings, json_info);

}

DataTypePtr tryInferDateOrDateTimeFromString(std::string_view field, const FormatSettings & settings)
{
    if (settings.try_infer_dates && tryInferDate(field))
        return std::make_shared<DataTypeDate>();

    if (settings.try_infer_datetimes)
    {
        if (auto type = tryInferDateTimeOrDateTime64(field, settings))
            return type;
    }

    return nullptr;
}

DataTypePtr tryInferDataTypeForSingleField(ReadBuffer & buf, const FormatSettings & settings)
{
    return tryInferDataTypeForSingleFieldImpl<false>(buf, settings, nullptr);
}

DataTypePtr tryInferDataTypeForSingleField(std::string_view field, const FormatSettings & settings)
{
    ReadBufferFromString buf(field);
    auto type = tryInferDataTypeForSingleFieldImpl<false>(buf, settings, nullptr);
    /// Check if there is no unread data in buffer.
    if (!buf.eof())
        return nullptr;
    return type;
}

DataTypePtr tryInferDataTypeForSingleJSONField(ReadBuffer & buf, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    return tryInferDataTypeForSingleFieldImpl<true>(buf, settings, json_info);
}

DataTypePtr tryInferDataTypeForSingleJSONField(std::string_view field, const FormatSettings & settings, JSONInferenceInfo * json_info)
{
    ReadBufferFromString buf(field);
    auto type = tryInferDataTypeForSingleFieldImpl<true>(buf, settings, json_info);
    /// Check if there is no unread data in buffer.
    if (!buf.eof())
        return nullptr;
    return type;
}

DataTypePtr makeNullableRecursively(DataTypePtr type)
{
    if (!type)
        return nullptr;

    WhichDataType which(type);

    if (which.isNullable())
        return type;

    if (which.isArray())
    {
        const auto * array_type = assert_cast<const DataTypeArray *>(type.get());
        auto nested_type = makeNullableRecursively(array_type->getNestedType());
        return nested_type ? std::make_shared<DataTypeArray>(nested_type) : nullptr;
    }

    if (which.isVariant())
    {
        const auto * variant_type = assert_cast<const DataTypeVariant *>(type.get());
        DataTypes nested_types;
        for (const auto & nested_type: variant_type->getVariants())
        {
            if (!nested_type->lowCardinality() && nested_type->haveSubtypes())
                nested_types.push_back(makeNullableRecursively(nested_type));
            else
                nested_types.push_back(nested_type);
        }
        return std::make_shared<DataTypeVariant>(nested_types);
    }

    if (which.isTuple())
    {
        const auto * tuple_type = assert_cast<const DataTypeTuple *>(type.get());
        DataTypes nested_types;
        for (const auto & element : tuple_type->getElements())
        {
            auto nested_type = makeNullableRecursively(element);
            if (!nested_type)
                return nullptr;
            nested_types.push_back(nested_type);
        }

        if (tuple_type->haveExplicitNames())
            return std::make_shared<DataTypeTuple>(std::move(nested_types), tuple_type->getElementNames());

        return std::make_shared<DataTypeTuple>(std::move(nested_types));
    }

    if (which.isMap())
    {
        const auto * map_type = assert_cast<const DataTypeMap *>(type.get());
        auto key_type = makeNullableRecursively(map_type->getKeyType());
        auto value_type = makeNullableRecursively(map_type->getValueType());
        return key_type && value_type ? std::make_shared<DataTypeMap>(removeNullable(key_type), value_type) : nullptr;
    }

    if (which.isLowCardinality())
    {
        const auto * lc_type = assert_cast<const DataTypeLowCardinality *>(type.get());
        auto nested_type = makeNullableRecursively(lc_type->getDictionaryType());
        return nested_type ? std::make_shared<DataTypeLowCardinality>(nested_type) : nullptr;
    }

    if (which.isObjectDeprecated())
    {
        const auto * object_type = assert_cast<const DataTypeObjectDeprecated *>(type.get());
        if (object_type->hasNullableSubcolumns())
            return type;
        return std::make_shared<DataTypeObjectDeprecated>(object_type->getSchemaFormat(), true);
    }

    return makeNullableSafe(type);
}

NamesAndTypesList getNamesAndRecursivelyNullableTypes(const Block & header)
{
    NamesAndTypesList result;
    for (auto & [name, type] : header.getNamesAndTypesList())
        result.emplace_back(name, makeNullableRecursively(type));
    return result;
}

bool checkIfTypeIsComplete(const DataTypePtr & type)
{
    if (!type)
        return false;

    WhichDataType which(type);

    if (which.isNothing())
        return false;

    if (which.isNullable())
        return checkIfTypeIsComplete(assert_cast<const DataTypeNullable *>(type.get())->getNestedType());

    if (which.isArray())
        return checkIfTypeIsComplete(assert_cast<const DataTypeArray *>(type.get())->getNestedType());

    if (which.isTuple())
    {
        const auto * tuple_type = assert_cast<const DataTypeTuple *>(type.get());
        for (const auto & element : tuple_type->getElements())
        {
            if (!checkIfTypeIsComplete(element))
                return false;
        }
        return true;
    }

    if (which.isMap())
    {
        const auto * map_type = assert_cast<const DataTypeMap *>(type.get());
        if (!checkIfTypeIsComplete(map_type->getKeyType()))
            return false;
        return checkIfTypeIsComplete(map_type->getValueType());
    }

    return true;
}

}
