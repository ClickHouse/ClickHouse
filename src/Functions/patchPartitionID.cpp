#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <Columns/ColumnString.h>
#include <base/find_symbols.h>
#include <Common/FunctionDocumentation.h>
#include <Storages/MergeTree/MergeTreePartInfo.h>

namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}

namespace
{

/// A special internal function that calculates the partition id of patch part.
/// Arguments: string column with the part name; const string column with hash of column names in the patch part.
class FunctionPatchPartitionID : public IFunction
{
public:
    static constexpr auto name = "__patchPartitionID";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionPatchPartitionID>(); }

    String getName() const override { return name; }
    size_t getNumberOfArguments() const override { return 2; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1}; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo &) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]) || !isString(arguments[1]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The arguments of function {} must be 2 strings", name);

        return std::make_shared<DataTypeString>();
    }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t input_rows_count) const override
    {
        const auto * column_part_name = checkAndGetColumn<ColumnString>(arguments[0].column.get());
        if (!column_part_name)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The first argument of function {} must be a string with the part name", name);

        const auto * column_structure_hash = checkAndGetColumnConstData<ColumnString>(arguments[1].column.get());
        if (!column_structure_hash)
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The second argument of function {} must be a const string with hash of patch column names", name);

        String patch_prefix{MergeTreePartInfo::PATCH_PART_PREFIX};
        patch_prefix += column_structure_hash->getDataAt(0).toString();
        patch_prefix += "-";

        if (patch_prefix.size() != MergeTreePartInfo::PATCH_PART_PREFIX_SIZE)
            throw Exception(ErrorCodes::BAD_ARGUMENTS, "Invalid prefix of patch part's partition id: {}", patch_prefix);

        auto result_column = ColumnString::create();
        result_column->reserve(input_rows_count);

        auto & out_chars = result_column->getChars();
        auto & out_offsets = result_column->getOffsets();

        for (size_t i = 0; i < input_rows_count; ++i)
        {
            auto ref = column_part_name->getDataAt(i);

            /// Allow empty part name to allow execute this function over the dictionary of LowCardinality.
            if (ref.size > 0)
            {
                const auto * pos = find_first_symbols<'_'>(ref.data, ref.data + ref.size);
                size_t bytes_to_copy = pos - ref.data;

                if (bytes_to_copy == ref.size)
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Incorrect name of part: {}", ref.toString());

                size_t old_size = out_chars.size();
                out_chars.resize(old_size + patch_prefix.size() + bytes_to_copy);

                memcpy(out_chars.data() + old_size, patch_prefix.data(), patch_prefix.size());
                memcpySmallAllowReadWriteOverflow15(out_chars.data() + old_size + patch_prefix.size(), ref.data, bytes_to_copy);
            }

            out_offsets.push_back(out_chars.size());
        }

        return result_column;
    }
};

}

REGISTER_FUNCTION(PatchPartitionID)
{
    factory.registerFunction<FunctionPatchPartitionID>(FunctionDocumentation
    {
        .description = R"(
Internal function. Receives the name of a part and a hash of patch part's column names. Returns the name of partition of patch part. The argument must be a correct name of part, the behaviour is undefined otherwise.
        )",
        .introduced_in = {25, 5},
        .category = FunctionDocumentation::Category::Other,
    });
}

}
