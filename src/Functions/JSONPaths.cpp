#include <Functions/IFunction.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeMap.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeObject.h>
#include <Core/ColumnNumbers.h>
#include <Columns/ColumnObject.h>
#include <Columns/ColumnMap.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnArray.h>
#include <DataTypes/DataTypesBinaryEncoding.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int BAD_ARGUMENTS;
}

namespace
{

enum class PathsMode
{
    ALL_PATHS,
    DYNAMIC_PATHS,
    SHARED_DATA_PATHS,
};

struct JSONAllPathsImpl
{
    static constexpr auto name = "JSONAllPaths";
    static constexpr auto paths_mode = PathsMode::ALL_PATHS;
    static constexpr auto with_types = false;
};

struct JSONAllPathsWithTypesImpl
{
    static constexpr auto name = "JSONAllPathsWithTypes";
    static constexpr auto paths_mode = PathsMode::ALL_PATHS;
    static constexpr auto with_types = true;
};

struct JSONDynamicPathsImpl
{
    static constexpr auto name = "JSONDynamicPaths";
    static constexpr auto paths_mode = PathsMode::DYNAMIC_PATHS;
    static constexpr auto with_types = false;
};

struct JSONDynamicPathsWithTypesImpl
{
    static constexpr auto name = "JSONDynamicPathsWithTypes";
    static constexpr auto paths_mode = PathsMode::DYNAMIC_PATHS;
    static constexpr auto with_types = true;
};

struct JSONSharedDataPathsImpl
{
    static constexpr auto name = "JSONSharedDataPaths";
    static constexpr auto paths_mode = PathsMode::SHARED_DATA_PATHS;
    static constexpr auto with_types = false;
};

struct JSONSharedDataPathsWithTypesImpl
{
    static constexpr auto name = "JSONSharedDataPathsWithTypes";
    static constexpr auto paths_mode = PathsMode::SHARED_DATA_PATHS;
    static constexpr auto with_types = true;
};

/// Implements functions that extracts paths and types from JSON object column.
template <typename Impl>
class FunctionJSONPaths : public IFunction
{
public:
    static constexpr auto name = Impl::name;

    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionJSONPaths>(); }

    std::string getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override { return 1; }
    bool useDefaultImplementationForConstants() const override { return true; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & data_types) const override
    {
        if (data_types.size() != 1 || data_types[0]->getTypeId() != TypeIndex::Object)
            throw Exception(ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH, "Function {} requires single argument with type JSON", getName());

        if constexpr (Impl::with_types)
            return std::make_shared<DataTypeMap>(std::make_shared<DataTypeString>(), std::make_shared<DataTypeString>());
        return std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>());
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const ColumnWithTypeAndName & elem = arguments[0];
        const auto * column_object = typeid_cast<const ColumnObject *>(elem.column.get());
        if (!column_object)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected column type in function {}. Expected Object column, got {}", getName(), elem.column->getName());

        const auto & type_object = assert_cast<const DataTypeObject &>(*elem.type);
        if constexpr (Impl::with_types)
            return executeWithTypes(*column_object, type_object);
        return executeWithoutTypes(*column_object);
    }

private:
    ColumnPtr executeWithoutTypes(const ColumnObject & column_object) const
    {
        if constexpr (Impl::paths_mode == PathsMode::SHARED_DATA_PATHS)
        {
            /// No need to do anything, we already have a column with all sorted paths in shared data.
            const auto & shared_data_array = column_object.getSharedDataNestedColumn();
            const auto & shared_data_paths = assert_cast<const ColumnTuple &>(shared_data_array.getData()).getColumnPtr(0);
            return ColumnArray::create(shared_data_paths, shared_data_array.getOffsetsPtr());
        }

        auto res = ColumnArray::create(ColumnString::create());
        auto & offsets = res->getOffsets();
        ColumnString & data = assert_cast<ColumnString &>(res->getData());

        if constexpr (Impl::paths_mode == PathsMode::DYNAMIC_PATHS)
        {
            /// Collect all dynamic paths.
            const auto & dynamic_path_columns = column_object.getDynamicPaths();
            std::vector<String> dynamic_paths;
            dynamic_paths.reserve(dynamic_path_columns.size());
            for (const auto & [path, _] : dynamic_path_columns)
                dynamic_paths.push_back(path);
            /// We want the resulting arrays of paths to be sorted for consistency.
            std::sort(dynamic_paths.begin(), dynamic_paths.end());

            for (const auto & path : dynamic_paths)
                data.insertData(path.data(), path.size());
            offsets.push_back(data.size());
            return res->replicate(IColumn::Offsets(1, column_object.size()));
        }

        /// Collect all paths: typed, dynamic and paths from shared data.
        std::vector<StringRef> sorted_dynamic_and_typed_paths;
        const auto & typed_path_columns = column_object.getTypedPaths();
        const auto & dynamic_path_columns = column_object.getDynamicPaths();
        for (const auto & [path, _] : typed_path_columns)
            sorted_dynamic_and_typed_paths.push_back(path);
        for (const auto & [path, _] : dynamic_path_columns)
            sorted_dynamic_and_typed_paths.push_back(path);

        /// We want the resulting arrays of paths to be sorted for consistency.
        std::sort(sorted_dynamic_and_typed_paths.begin(), sorted_dynamic_and_typed_paths.end());

        const auto & shared_data_offsets = column_object.getSharedDataOffsets();
        const auto [shared_data_paths, _] = column_object.getSharedDataPathsAndValues();
        for (size_t i = 0; i != shared_data_offsets.size(); ++i)
        {
            size_t start = shared_data_offsets[ssize_t(i) - 1];
            size_t end = shared_data_offsets[ssize_t(i)];
            /// Merge sorted list of paths from shared data and sorted_dynamic_and_typed_paths
            size_t sorted_paths_index = 0;
            for (size_t j = start; j != end; ++j)
            {
                auto shared_data_path = shared_data_paths->getDataAt(j);
                while (sorted_paths_index != sorted_dynamic_and_typed_paths.size() && sorted_dynamic_and_typed_paths[sorted_paths_index] < shared_data_path)
                {
                    auto path = sorted_dynamic_and_typed_paths[sorted_paths_index];
                    data.insertData(path.data, path.size);
                    ++sorted_paths_index;
                }

                data.insertData(shared_data_path.data, shared_data_path.size);
            }

            for (; sorted_paths_index != sorted_dynamic_and_typed_paths.size(); ++sorted_paths_index)
            {
                auto path = sorted_dynamic_and_typed_paths[sorted_paths_index];
                data.insertData(path.data, path.size);
            }

            offsets.push_back(data.size());
        }

        return res;
    }

    ColumnPtr executeWithTypes(const ColumnObject & column_object, const DataTypeObject & type_object) const
    {
        auto offsets_column = ColumnArray::ColumnOffsets::create();
        auto & offsets = offsets_column->getData();
        auto paths_column = ColumnString::create();
        auto types_column = ColumnString::create();

        if constexpr (Impl::paths_mode == PathsMode::DYNAMIC_PATHS)
        {
            const auto & dynamic_path_columns = column_object.getDynamicPaths();
            std::vector<String> sorted_dynamic_paths;
            sorted_dynamic_paths.reserve(dynamic_path_columns.size());
            for (const auto & [path, _] : dynamic_path_columns)
                sorted_dynamic_paths.push_back(path);
            /// We want the resulting arrays of paths and values to be sorted for consistency.
            std::sort(sorted_dynamic_paths.begin(), sorted_dynamic_paths.end());

            /// Iterate over all rows and extract types from dynamic columns.
            for (size_t i = 0; i != column_object.size(); ++i)
            {
                for (auto & path : sorted_dynamic_paths)
                {
                    auto type = getDynamicValueType(dynamic_path_columns.at(path), i);
                    paths_column->insertData(path.data(), path.size());
                    types_column->insertData(type.data(), type.size());
                }

                offsets.push_back(types_column->size());
            }

            return ColumnMap::create(ColumnPtr(std::move(paths_column)), ColumnPtr(std::move(types_column)), ColumnPtr(std::move(offsets_column)));
        }

        if constexpr (Impl::paths_mode == PathsMode::SHARED_DATA_PATHS)
        {
            const auto & shared_data_offsets = column_object.getSharedDataOffsets();
            const auto [shared_data_paths, shared_data_values] = column_object.getSharedDataPathsAndValues();
            /// Iterate over all rows and extract types from dynamic values in shared data.
            for (size_t i = 0; i != shared_data_offsets.size(); ++i)
            {
                size_t start = shared_data_offsets[ssize_t(i) - 1];
                size_t end = shared_data_offsets[ssize_t(i)];
                for (size_t j = start; j != end; ++j)
                {
                    paths_column->insertFrom(*shared_data_paths, j);
                    auto type_name = getDynamicValueTypeFromSharedData(shared_data_values->getDataAt(j));
                    types_column->insertData(type_name.data(), type_name.size());
                }

                offsets.push_back(paths_column->size());
            }

            return ColumnMap::create(ColumnPtr(std::move(paths_column)), ColumnPtr(std::move(types_column)), ColumnPtr(std::move(offsets_column)));
        }

        /// Iterate over all rows and extract types from dynamic columns from dynamic paths and from values in shared data.
        std::vector<std::pair<String, String>> sorted_typed_and_dynamic_paths_with_types;
        const auto & typed_path_types = type_object.getTypedPaths();
        const auto & dynamic_path_columns = column_object.getDynamicPaths();
        sorted_typed_and_dynamic_paths_with_types.reserve(typed_path_types.size() + dynamic_path_columns.size());
        for (const auto & [path, type] : typed_path_types)
            sorted_typed_and_dynamic_paths_with_types.emplace_back(path, type->getName());
        for (const auto & [path, _] : dynamic_path_columns)
            sorted_typed_and_dynamic_paths_with_types.emplace_back(path, "");

        /// We want the resulting arrays of paths and values to be sorted for consistency.
        std::sort(sorted_typed_and_dynamic_paths_with_types.begin(), sorted_typed_and_dynamic_paths_with_types.end());

        const auto & shared_data_offsets = column_object.getSharedDataOffsets();
        const auto [shared_data_paths, shared_data_values] = column_object.getSharedDataPathsAndValues();
        for (size_t i = 0; i != shared_data_offsets.size(); ++i)
        {
            size_t start = shared_data_offsets[ssize_t(i) - 1];
            size_t end = shared_data_offsets[ssize_t(i)];
            /// Merge sorted list of paths and values from shared data and sorted_typed_and_dynamic_paths_with_types
            size_t sorted_paths_index = 0;
            for (size_t j = start; j != end; ++j)
            {
                auto shared_data_path = shared_data_paths->getDataAt(j);
                auto type_name = getDynamicValueTypeFromSharedData(shared_data_values->getDataAt(j));

                while (sorted_paths_index != sorted_typed_and_dynamic_paths_with_types.size() && sorted_typed_and_dynamic_paths_with_types[sorted_paths_index].first < shared_data_path)
                {
                    auto & [path, type] = sorted_typed_and_dynamic_paths_with_types[sorted_paths_index];
                    paths_column->insertData(path.data(), path.size());
                    /// Update type for path from dynamic paths.
                    if (auto it = dynamic_path_columns.find(path); it != dynamic_path_columns.end())
                        type = getDynamicValueType(it->second, i);
                    types_column->insertData(type.data(), type.size());
                    ++sorted_paths_index;
                }

                paths_column->insertData(shared_data_path.data, shared_data_path.size);
                types_column->insertData(type_name.data(), type_name.size());
            }

            for (; sorted_paths_index != sorted_typed_and_dynamic_paths_with_types.size(); ++sorted_paths_index)
            {
                auto & [path, type] = sorted_typed_and_dynamic_paths_with_types[sorted_paths_index];
                paths_column->insertData(path.data(), path.size());
                if (auto it = dynamic_path_columns.find(path); it != dynamic_path_columns.end())
                    type = getDynamicValueType(it->second, i);
                types_column->insertData(type.data(), type.size());
            }

            offsets.push_back(paths_column->size());
        }

        return ColumnMap::create(ColumnPtr(std::move(paths_column)), ColumnPtr(std::move(types_column)), ColumnPtr(std::move(offsets_column)));
    }

    String getDynamicValueType(const ColumnPtr & column, size_t i) const
    {
        const ColumnDynamic * dynamic_column = checkAndGetColumn<ColumnDynamic>(column.get());
        const auto & variant_info = dynamic_column->getVariantInfo();
        const auto & variant_column = dynamic_column->getVariantColumn();
        auto global_discr = variant_column.globalDiscriminatorAt(i);
        if (global_discr == ColumnVariant::NULL_DISCRIMINATOR)
            return "None";

        return variant_info.variant_names[global_discr];
    }

    String getDynamicValueTypeFromSharedData(StringRef value) const
    {
        ReadBufferFromMemory buf(value.data, value.size);
        auto type = decodeDataType(buf);
        return isNothing(type) ? "None" : type->getName();
    }
};

}

REGISTER_FUNCTION(JSONPaths)
{
    factory.registerFunction<FunctionJSONPaths<JSONAllPathsImpl>>();
    factory.registerFunction<FunctionJSONPaths<JSONAllPathsWithTypesImpl>>();
    factory.registerFunction<FunctionJSONPaths<JSONDynamicPathsImpl>>();
    factory.registerFunction<FunctionJSONPaths<JSONDynamicPathsWithTypesImpl>>();
    factory.registerFunction<FunctionJSONPaths<JSONSharedDataPathsImpl>>();
    factory.registerFunction<FunctionJSONPaths<JSONSharedDataPathsWithTypesImpl>>();
}

}
