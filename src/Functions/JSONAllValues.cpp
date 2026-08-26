#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeObject.h>
#include <DataTypes/DataTypesBinaryEncoding.h>
#include <DataTypes/Serializations/SerializationDynamic.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <Formats/FormatSettings.h>
#include <IO/ReadBufferFromMemory.h>
#include <IO/WriteBufferFromString.h>
#include <DataTypes/DataTypesCache.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int INCORRECT_DATA;
}

namespace
{

/// Returns all values from a JSON column as an array of strings, in sorted path order.
class FunctionJSONAllValues : public IFunction
{
public:
    static constexpr auto name = "JSONAllValues";

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionJSONAllValues>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & data_types) const override
    {
        if (data_types.size() != 1)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                "Function {} requires single argument with type JSON", getName());

        if (data_types[0]->getTypeId() != TypeIndex::Object)
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                "Function {} requires argument with type JSON, got: {}",
                getName(), data_types[0]->getName());

        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const auto & elem = arguments[0];
        const auto * column_object = typeid_cast<const ColumnObject *>(elem.column.get());

        if (!column_object)
            throw Exception(ErrorCodes::BAD_ARGUMENTS,
                "Unexpected column type in function {}. Expected Object column, got {}",
                getName(), elem.column->getName());

        const auto & type_object = assert_cast<const DataTypeObject &>(*elem.type);
        return execute(*column_object, type_object);
    }

private:
    struct PathInfo
    {
        std::string_view path;
        const IColumn * column;
        SerializationPtr serialization;
        bool is_dynamic; /// dynamic paths need a null check before serialization
    };

    ColumnPtr execute(const ColumnObject & column_object, const DataTypeObject & type_object) const
    {
        auto res = ColumnArray::create(ColumnString::create());
        auto & offsets = res->getOffsets();
        auto & result_data = assert_cast<ColumnString &>(res->getData());

        FormatSettings format_settings;

        /// Collect typed + dynamic paths, sorted for deterministic output order.
        const auto & typed_path_types = type_object.getTypedPaths();
        const auto & typed_path_columns = column_object.getTypedPaths();
        const auto & dynamic_path_columns = column_object.getDynamicPaths();
        auto dynamic_serialization = SerializationDynamic::create();

        std::vector<PathInfo> sorted_paths;
        sorted_paths.reserve(typed_path_types.size() + dynamic_path_columns.size());

        for (const auto & [path, type] : typed_path_types)
        {
            const auto & column = typed_path_columns.at(path);
            sorted_paths.push_back({path, column.get(), type->getDefaultSerialization(), false});
        }

        for (const auto & [path, column] : dynamic_path_columns)
            sorted_paths.push_back({path, column.get(), dynamic_serialization, true});

        std::sort(sorted_paths.begin(), sorted_paths.end(),
            [](const PathInfo & a, const PathInfo & b) { return a.path < b.path; });

        /// Cache of reusable (serialization, column) structs keyed by type name,
        /// to avoid createColumn and getDefaultSerialization per shared data value.
        std::unordered_map<String, SerializationPtr> shared_serializations_cache;
        std::unordered_map<String, MutableColumnPtr> shared_columns_cache;

        const auto & shared_data_offsets = column_object.getSharedDataOffsets();
        const auto [shared_data_paths, shared_data_values] = column_object.getSharedDataPathsAndValues();

        for (size_t i = 0; i != shared_data_offsets.size(); ++i)
        {
            size_t start = shared_data_offsets[static_cast<ssize_t>(i) - 1];
            size_t end = shared_data_offsets[static_cast<ssize_t>(i)];

            /// Merge sorted typed+dynamic paths with sorted shared data paths (two-pointer merge).
            size_t sorted_paths_index = 0;
            for (size_t j = start; j != end; ++j)
            {
                auto shared_data_path = shared_data_paths->getDataAt(j);

                /// Emit typed/dynamic paths that sort before this shared data path.
                while (sorted_paths_index < sorted_paths.size() && sorted_paths[sorted_paths_index].path < shared_data_path)
                {
                    emitValue(sorted_paths[sorted_paths_index], i, format_settings, result_data);
                    ++sorted_paths_index;
                }

                /// Emit the shared data value.
                emitSharedDataValue(shared_data_values->getDataAt(j), format_settings, result_data, shared_serializations_cache, shared_columns_cache);
            }

            /// Emit remaining typed/dynamic paths after all shared data for this row.
            for (; sorted_paths_index < sorted_paths.size(); ++sorted_paths_index)
            {
                emitValue(sorted_paths[sorted_paths_index], i, format_settings, result_data);
            }

            offsets.push_back(result_data.size());
        }

        return res;
    }

    static void emitValue(
        const PathInfo & entry,
        size_t row,
        const FormatSettings & format_settings,
        ColumnString & result_data)
    {
        if (entry.is_dynamic && entry.column->isNullAt(row))
            return;

        serializeValueIntoResult(*entry.serialization, *entry.column, row, format_settings, result_data);
    }

    static void emitSharedDataValue(
        std::string_view value_data,
        const FormatSettings & format_settings,
        ColumnString & data,
        std::unordered_map<String, SerializationPtr> & shared_serializations_cache,
        std::unordered_map<String, MutableColumnPtr> & shared_columns_cache)
    {
        ReadBufferFromMemory buf(value_data);

        auto get_serialization_from_cache = [&](const String & type_name, const IDataType & type) -> const SerializationPtr &
        {
            auto [it, inserted] = shared_serializations_cache.try_emplace(type_name);
            if (inserted)
                it->second = type.getDefaultSerialization();

            return it->second;
        };

        auto get_column_from_cache = [&](const String & type_name, const IDataType & type) -> const MutableColumnPtr &
        {
            auto [it, inserted] = shared_columns_cache.try_emplace(type_name);
            if (inserted)
                it->second = type.createColumn();

            return it->second;
        };

        auto serialize = [&](const IDataType & type, const ISerialization & serialization, IColumn & temp_column)
        {
            if (isNothing(type))
                return;

            serialization.deserializeBinary(temp_column, buf, format_settings);
            serializeValueIntoResult(serialization, temp_column, 0, format_settings, data);
            temp_column.popBack(1);
        };

        char type_index;
        if (!buf.peek(type_index))
            throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot parse shared data value of JSON: no type index found");

        const auto & cache = getSimpleDataTypesCache();
        auto binary_type_index = static_cast<BinaryTypeIndex>(type_index);

        if (cache.hasElement(binary_type_index))
        {
            ++buf.position();

            const auto & element = cache.getElement(binary_type_index);
            const auto & temp_column = get_column_from_cache(element.name, *element.type);
            serialize(*element.type, *element.serialization, *temp_column);
        }
        else
        {
            auto type = decodeDataType(buf);
            auto type_name = type->getName();
            const auto & serialization = get_serialization_from_cache(type_name, *type);
            const auto & temp_column = get_column_from_cache(type_name, *type);
            serialize(*type, *serialization, *temp_column);
        }
    }

    static void serializeValueIntoResult(
        const ISerialization & serialization,
        const IColumn & source_column,
        size_t row,
        const FormatSettings & format_settings,
        ColumnString & result_data)
    {
        auto & result_chars = result_data.getChars();
        auto & result_offsets = result_data.getOffsets();

        {
            WriteBufferFromVector<ColumnString::Chars> out(result_chars, AppendModeTag());
            serialization.serializeText(source_column, row, out, format_settings);
        }

        result_offsets.push_back(result_chars.size());
    }
};

}

REGISTER_FUNCTION(JSONAllValues)
{
    FunctionDocumentation::Description description = R"(
Returns all values from each row in a JSON column as an array of strings.
Values are serialized in their text representation and ordered by their path names.
    )";
    FunctionDocumentation::Syntax syntax = "JSONAllValues(json)";
    FunctionDocumentation::Arguments arguments = {
        {"json", "JSON column.", {"JSON"}}
    };
    FunctionDocumentation::ReturnedValue returned_value = {"Returns an array of all values as strings in the JSON column.", {"Array(String)"}};
    FunctionDocumentation::Examples examples = {
    {
        "Usage example",
        R"(
CREATE TABLE test (json JSON(max_dynamic_paths=1)) ENGINE = Memory;
INSERT INTO test FORMAT JSONEachRow {"json": {"a": 42}}, {"json": {"b": "Hello"}}, {"json": {"a": [1, 2, 3], "c": "2020-01-01"}}
SELECT json, JSONAllValues(json) FROM test;
        )",
        R"(
┌─json─────────────────────────────────┬─JSONAllValues(json)──────┐
│ {"a":42}                             │ ['42']                   │
│ {"b":"Hello"}                        │ ['Hello']                │
│ {"a":[1,2,3],"c":"2020-01-01"}       │ ['[1,2,3]','2020-01-01'] │
└──────────────────────────────────────┴──────────────────────────┘
        )"
    }
    };
    FunctionDocumentation::IntroducedIn introduced_in = {26, 4};
    FunctionDocumentation::Category category = FunctionDocumentation::Category::JSON;
    FunctionDocumentation documentation = {description, syntax, arguments, {}, returned_value, examples, introduced_in, category};
    factory.registerFunction<FunctionJSONAllValues>(documentation);
}

}
