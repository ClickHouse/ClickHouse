#include <mutex>
#include <base/bit_cast.h>

#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnDecimal.h>
#include <Columns/ColumnNullable.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunction.h>
#include <Interpreters/castColumn.h>
#include <Interpreters/convertFieldToType.h>
#include <base/StringRef.h>
#include <Common/Arena.h>
#include <Common/FieldVisitorConvertToNumber.h>
#include <Common/FieldVisitorDump.h>
#include <Common/HashTable/HashMap.h>
#include <Common/typeid_cast.h>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_COLUMN;
}

namespace
{
    /** transform(x, from_array, to_array[, default]) - convert x according to an explicitly passed match.
  */
    /** transform(x, [from...], [to...], default)
  * - converts the values according to the explicitly specified mapping.
  *
  * x - what to transform.
  * from - a constant array of values for the transformation.
  * to - a constant array of values into which values from `from` must be transformed.
  * default - what value to use if x is not equal to any of the values in `from`.
  * `from` and `to` - arrays of the same size.
  *
  * Types:
  * transform(T, Array(T), Array(U), U) -> U
  *
  * transform(x, [from...], [to...])
  * - if `default` is not specified, then for values of `x` for which there is no corresponding element in `from`, the unchanged value of `x` is returned.
  *
  * Types:
  * transform(T, Array(T), Array(T)) -> T
  *
  * Note: the implementation is rather cumbersome.
  */
    class FunctionTransform : public IFunction
    {
    public:
        static constexpr auto name = "transform";
        static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionTransform>(); }

        String getName() const override { return name; }

        bool isVariadic() const override { return true; }
        bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
        size_t getNumberOfArguments() const override { return 0; }
        bool useDefaultImplementationForConstants() const override { return false; }
        bool useDefaultImplementationForNulls() const override { return false; }
        bool useDefaultImplementationForNothing() const override { return false; }
        ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {1, 2}; }

        DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
        {
            const auto args_size = arguments.size();
            if (args_size != 3 && args_size != 4)
                throw Exception(
                    ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH,
                    "Number of arguments for function {} doesn't match: "
                    "passed {}, should be 3 or 4",
                    getName(),
                    args_size);

            const DataTypePtr & type_x = arguments[0];
            const auto & type_x_nn = removeNullable(type_x);

            if (!type_x_nn->isValueRepresentedByNumber() && !isString(type_x_nn) && !isNothing(type_x_nn))
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Unsupported type {} of first argument "
                    "of function {}, must be numeric type or Date/DateTime or String",
                    type_x->getName(),
                    getName());

            const DataTypeArray * type_arr_from = checkAndGetDataType<DataTypeArray>(arguments[1].get());

            if (!type_arr_from)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Second argument of function {}, must be array of source values to transform from.",
                    getName());

            const auto type_arr_from_nested = type_arr_from->getNestedType();

            if ((type_x->isValueRepresentedByNumber() != type_arr_from_nested->isValueRepresentedByNumber())
                || (isString(type_x) != isString(type_arr_from_nested)))
            {
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "First argument and elements of array "
                    "of second argument of function {} must have compatible types: "
                    "both numeric or both strings.",
                    getName());
            }

            const DataTypeArray * type_arr_to = checkAndGetDataType<DataTypeArray>(arguments[2].get());

            if (!type_arr_to)
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Third argument of function {}, must be array of destination values to transform to.",
                    getName());

            const DataTypePtr & type_arr_to_nested = type_arr_to->getNestedType();

            if (args_size == 3)
            {
                auto ret = tryGetLeastSupertype(DataTypes{type_arr_to_nested, type_x});
                if (!ret)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Function {} has signature: "
                        "transform(T, Array(T), Array(U), U) -> U; "
                        "or transform(T, Array(T), Array(T)) -> T; where T and U are types.",
                        getName());
                return ret;
            }
            else
            {
                auto ret = tryGetLeastSupertype(DataTypes{type_arr_to_nested, arguments[3]});
                if (!ret)
                    throw Exception(
                        ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                        "Function {} have signature: "
                        "transform(T, Array(T), Array(U), U) -> U; "
                        "or transform(T, Array(T), Array(T)) -> T; where T and U are types.",
                        getName());
                return ret;
            }
        }

        ColumnPtr
        executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count) const override
        {
            initialize(arguments, result_type);

            const auto * in = arguments.front().column.get();

            if (isColumnConst(*in))
                return executeConst(arguments, result_type, input_rows_count);

            ColumnPtr default_non_const;
            if (!cache.default_column && arguments.size() == 4)
                default_non_const = castColumn(arguments[3], result_type);

            auto column_result = result_type->createColumn();
            if (!executeNum<ColumnVector<UInt8>>(in, *column_result, default_non_const)
                && !executeNum<ColumnVector<UInt16>>(in, *column_result, default_non_const)
                && !executeNum<ColumnVector<UInt32>>(in, *column_result, default_non_const)
                && !executeNum<ColumnVector<UInt64>>(in, *column_result, default_non_const)
                && !executeNum<ColumnVector<Int8>>(in, *column_result, default_non_const)
                && !executeNum<ColumnVector<Int16>>(in, *column_result, default_non_const)
                && !executeNum<ColumnVector<Int32>>(in, *column_result, default_non_const)
                && !executeNum<ColumnVector<Int64>>(in, *column_result, default_non_const)
                && !executeNum<ColumnVector<Float32>>(in, *column_result, default_non_const)
                && !executeNum<ColumnVector<Float64>>(in, *column_result, default_non_const)
                && !executeNum<ColumnDecimal<Decimal32>>(in, *column_result, default_non_const)
                && !executeNum<ColumnDecimal<Decimal64>>(in, *column_result, default_non_const)
                && !executeString(in, *column_result, default_non_const))
            {
                throw Exception(ErrorCodes::ILLEGAL_COLUMN, "Illegal column {} of first argument of function {}", in->getName(), getName());
            }
            return column_result;
        }

    private:
        static ColumnPtr executeConst(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t input_rows_count)
        {
            /// Materialize the input column and compute the function as usual.

            ColumnsWithTypeAndName args = arguments;
            args[0].column = args[0].column->cloneResized(input_rows_count)->convertToFullColumnIfConst();

            auto impl = FunctionToOverloadResolverAdaptor(std::make_shared<FunctionTransform>()).build(args);

            return impl->execute(args, result_type, input_rows_count);
        }

        template <typename T>
        bool executeNum(const IColumn * in_untyped, IColumn & column_result, const ColumnPtr default_non_const) const
        {
            const auto in = checkAndGetColumn<T>(in_untyped);
            if (!in)
                return false;
            const auto & pod = in->getData();
            const size_t size = pod.size();
            const auto & table = *cache.table_num_to_idx;
            column_result.reserve(size);
            for (size_t i = 0; i < size; ++i)
            {
                const auto * it = table.find(bit_cast<UInt64>(pod[i]));
                if (it)
                    column_result.insertFrom(*cache.to_columns, it->getMapped());
                else if (cache.default_column)
                    column_result.insertFrom(*cache.default_column, 0);
                else if (default_non_const)
                    column_result.insertFrom(*default_non_const, i);
                else
                    column_result.insertFrom(*in, i);
            }
            return true;
        }

        bool executeString(const IColumn * in_untyped, IColumn & column_result, const ColumnPtr default_non_const) const
        {
            const auto in = checkAndGetColumn<ColumnString>(in_untyped);
            if (!in)
                return false;
            const auto & data = in->getChars();
            const auto & offsets = in->getOffsets();
            const size_t size = offsets.size();
            const auto & table = *cache.table_string_to_idx;
            column_result.reserve(size);
            ColumnString::Offset current_offset = 0;
            for (size_t i = 0; i < size; ++i)
            {
                const StringRef ref{&data[current_offset], offsets[i] - current_offset};
                current_offset = offsets[i];
                const auto * it = table.find(ref);
                if (it)
                    column_result.insertFrom(*cache.to_columns, it->getMapped());
                else if (cache.default_column)
                    column_result.insertFrom(*cache.default_column, 0);
                else if (default_non_const)
                    column_result.insertFrom(*default_non_const, 0);
                else
                    column_result.insertFrom(*in, i);
            }
            return true;
        }

        /// Different versions of the hash tables to implement the mapping.

        struct Cache
        {
            using NumToIdx = HashMap<UInt64, size_t, HashCRC32<UInt64>>;
            using StringToIdx = HashMap<StringRef, size_t, StringRefHash>;

            std::unique_ptr<NumToIdx> table_num_to_idx;
            std::unique_ptr<StringToIdx> table_string_to_idx;

            ColumnPtr to_columns;
            ColumnPtr default_column;

            Arena string_pool;

            std::atomic<bool> initialized{false};
            std::mutex mutex;
        };

        mutable Cache cache;


        static UInt64 bitCastToUInt64(const Field & x)
        {
            switch (x.getType())
            {
                case Field::Types::UInt64:
                    return x.get<UInt64>();
                case Field::Types::Int64:
                    return x.get<Int64>();
                case Field::Types::Float64:
                    return std::bit_cast<UInt64>(x.get<Float64>());
                case Field::Types::Bool:
                    return x.get<bool>();
                case Field::Types::Decimal32:
                    return x.get<DecimalField<Decimal32>>().getValue();
                case Field::Types::Decimal64:
                    return x.get<DecimalField<Decimal64>>().getValue();
                default:
                    throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unexpected type in function 'transform'");
            }
        }

        /// Can be called from different threads. It works only on the first call.
        void initialize(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type) const
        {
            const ColumnConst * array_from = checkAndGetColumnConst<ColumnArray>(arguments[1].column.get());
            const ColumnConst * array_to = checkAndGetColumnConst<ColumnArray>(arguments[2].column.get());

            if (!array_from || !array_to)
                throw Exception(
                    ErrorCodes::ILLEGAL_COLUMN, "Second and third arguments of function {} must be constant arrays.", getName());

            if (cache.initialized)
                return;

            const auto & from = array_from->getValue<Array>();
            const size_t size = from.size();
            if (0 == size)
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Empty arrays are illegal in function {}", getName());

            std::lock_guard lock(cache.mutex);

            if (cache.initialized)
                return;

            const auto & to = array_to->getValue<Array>();
            if (size != to.size())
                throw Exception(
                    ErrorCodes::BAD_ARGUMENTS, "Second and third arguments of function {} must be arrays of same size", getName());

            /// Whether the default value is set.

            if (arguments.size() == 4)
            {
                const IColumn * default_col = arguments[3].column.get();
                if (default_col && isColumnConst(*default_col))
                {
                    auto default_column = result_type->createColumn();
                    if (!default_col->onlyNull())
                    {
                        Field f = convertFieldToType((*default_col)[0], *result_type);
                        default_column->insert(f);
                    }
                    else
                        default_column->insertDefault();
                    cache.default_column = std::move(default_column);
                }
            }

            /// Note: Doesn't check the duplicates in the `from` array.

            const IDataType & from_type = *arguments[0].type;

            if (from[0].getType() != Field::Types::String)
            {
                cache.table_num_to_idx = std::make_unique<Cache::NumToIdx>();
                auto & table = *cache.table_num_to_idx;
                for (size_t i = 0; i < size; ++i)
                {
                    Field key = convertFieldToType(from[i], from_type);
                    if (key.isNull())
                        continue;

                    /// Field may be of Float type, but for the purpose of bitwise equality we can treat them as UInt64
                    table[bitCastToUInt64(key)] = i;
                }
            }
            else
            {
                cache.table_string_to_idx = std::make_unique<Cache::StringToIdx>();
                auto & table = *cache.table_string_to_idx;
                for (size_t i = 0; i < size; ++i)
                {
                    const String & str_from = from[i].get<const String &>();
                    StringRef ref{cache.string_pool.insert(str_from.data(), str_from.size() + 1), str_from.size() + 1};
                    table[ref] = i;
                }
            }

            auto to_columns = result_type->createColumn();
            for (size_t i = 0; i < size; ++i)
            {
                Field to_value = convertFieldToType(to[i], *result_type);
                to_columns->insert(to_value);
            }
            cache.to_columns = std::move(to_columns);

            cache.initialized = true;
        }
};

}

REGISTER_FUNCTION(Transform)
{
    factory.registerFunction<FunctionTransform>();
}

}
