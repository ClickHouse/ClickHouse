#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <Eigen/Core>

namespace DB
{
namespace ErrorCodes
{
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <const int N>
struct LpDistance
{
    static inline String name = "L" + std::to_string(N);
    template <typename X>
    static void compute(const Eigen::MatrixX<X> & left, const Eigen::MatrixX<X> & right, MutableColumnPtr & column)
    {
        auto & data = assert_cast<ColumnFloat64 &>(*column).getData();
        for (auto col : left.colwise())
        {
            auto norms = (right.colwise() - col).colwise().template lpNorm<N>();
            for (auto n : norms)
            {
                data.emplace_back(n);
            }
        }
    }
};

struct L2Distance
{
    static inline String name = "L2";
    template <typename X>
    static void compute(const Eigen::MatrixX<X> & left, const Eigen::MatrixX<X> & right, MutableColumnPtr & column)
    {
        auto & data = assert_cast<ColumnFloat64 &>(*column).getData();
        for (auto col : left.colwise())
        {
            auto norms = (right.colwise() - col).colwise().norm();
            for (auto n : norms)
            {
                data.emplace_back(n);
            }
        }
    }
};

struct LinfDistance : LpDistance<Eigen::Infinity>
{
    static inline String name = "Linf";
};

struct CosineDistance
{
    static inline String name = "Cosine";
    template <typename X>
    static void compute(const Eigen::MatrixX<X> & left, const Eigen::MatrixX<X> & right, MutableColumnPtr & column)
    {
        auto & data = assert_cast<ColumnFloat64 &>(*column).getData();
        auto x = left.colwise().normalized();
        auto y = right.colwise().normalized();
        auto & prod = x.cwiseProduct(y).colwise().sum();
        for (auto p : prod)
        {
            data.emplace_back(1.0 - p);
        }
    }
};

template <class Label>
class FunctionArrayDistance : public IFunction
{
public:
    static inline auto name = "array" + Label::name + "Distance";
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayDistance<Label>>(); }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        DataTypePtr first_nested_type = nullptr;
        for (const auto & argument : arguments)
        {
            const DataTypeArray * array_type = checkAndGetDataType<DataTypeArray>(argument.type.get());
            if (!array_type)
            {
                throw Exception(
                    "Arguments of function " + getName() + " must be array. Found " + argument.type->getName() + " instead.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }

            const auto & nested_type = array_type->getNestedType();

            if (!first_nested_type)
            {
                first_nested_type = nested_type;
            }
            else if (!nested_type->equals(*first_nested_type))
            {
                throw Exception("Arguments of function " + getName() + " must have identical type.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }

        switch (first_nested_type->getTypeId())
        {
            case TypeIndex::UInt8:
            case TypeIndex::UInt32:
            case TypeIndex::Int8:
            case TypeIndex::Int32:
            case TypeIndex::Float32:
            case TypeIndex::Float64:
                break;
            default:
                throw Exception(
                    "Array nested type " + first_nested_type->getName() + ". Support: UInt8, UInt32, Int8, Int32, Float32, Float64.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        //return std::make_shared<DataTypeArray>(std::make_shared<DataTypeFloat64>());
        return std::make_shared<DataTypeFloat64>();
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        auto res_ptr = result_type->createColumn();
        //auto & res = assert_cast<ColumnFloat64 &>(*res_ptr).getData();
        const auto & first_col = arguments[0].column;
        const auto & second_col = arguments[1].column;
        const auto & nested_type = checkAndGetDataType<DataTypeArray>(arguments[0].type.get())->getNestedType();
        switch (nested_type->getTypeId())
        {
            case TypeIndex::UInt8:
                executeDistance<UInt8, float>(first_col, second_col, res_ptr);
                break;
            case TypeIndex::UInt32:
                executeDistance<UInt32, float>(first_col, second_col, res_ptr);
                break;
            case TypeIndex::Int8:
                executeDistance<Int8, float>(first_col, second_col, res_ptr);
                break;
            case TypeIndex::Int32:
                executeDistance<Int32, float>(first_col, second_col, res_ptr);
                break;
            case TypeIndex::Float32:
                executeDistance<Float32, float>(first_col, second_col, res_ptr);
                break;
            case TypeIndex::Float64:
                executeDistance<Float64, double>(first_col, second_col, res_ptr);
                break;
            default:
                throw Exception(
                    "Array nested type " + nested_type->getName() + ". Support: UInt8, UInt32, Int8, Int32, Float32, Float64.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        return res_ptr;
    }

private:
    template <typename T, typename X>
    static void executeDistance(const ColumnPtr & first, const ColumnPtr & second, MutableColumnPtr & column)
    {
        Eigen::MatrixX<X> mx, my;
        columnToMatrix<T, X>(first, mx);
        columnToMatrix<T, X>(second, my);

        if (mx.rows() && my.rows() && mx.rows() != my.rows())
        {
            throw Exception(
                "Arguments of function " + String(name) + " have different array sizes.", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
        }
        Label::compute(mx, my, column);
    }

    template <typename T, typename X>
    static void columnToMatrix(const ColumnPtr column, Eigen::MatrixX<X> & mat)
    {
        ColumnPtr offsets;
        const ColumnVector<T> * vec;

        if (const ColumnArray * column_array = checkAndGetColumn<ColumnArray>(column.get()))
        {
            offsets = column_array->getOffsetsPtr();
            vec = checkAndGetColumn<ColumnVector<T>>(column_array->getData());
        }
        else if (const ColumnArray * const_array = checkAndGetColumnConstData<ColumnArray>(column.get()))
        {
            offsets = const_array->getOffsetsPtr();
            vec = checkAndGetColumn<ColumnVector<T>>(const_array->getData());
        }
        else
        {
            throw Exception("Argument of function " + String(name) + " must be array. ", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        const auto & off = assert_cast<const ColumnArray::ColumnOffsets &>(*offsets).getData();
        fillMatrix(mat, vec->getData(), off);
    }

    template <typename T, typename X>
    static void fillMatrix(Eigen::MatrixX<X> & mat, const PaddedPODArray<T> & data, const ColumnArray::Offsets & offsets)
    {
        auto row_size = offsets.front();
        mat.resize(row_size, offsets.size());
        ColumnArray::Offset prev = 0, col = 0;
        for (auto offset : offsets)
        {
            /*
            if (offset - prev != row_size) {
                throw Exception("Array must have equal sizes", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
            */
            for (ColumnArray::Offset row = 0; row < offset - prev; ++row)
            {
                mat(row, col) = X(data[prev + row]);
            }
            ++col;
            prev = offset;
        }
    }
};

void registerFunctionArrayDistance(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayDistance<L2Distance>>();
    factory.registerFunction<FunctionArrayDistance<LpDistance<1>>>();
    factory.registerFunction<FunctionArrayDistance<LinfDistance>>();
    factory.registerFunction<FunctionArrayDistance<CosineDistance>>();
}

}
