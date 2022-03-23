#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/getLeastSupertype.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>

#include <Eigen/Core>

namespace DB
{
namespace ErrorCodes
{
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

template <const int N>
struct LpDistance
{
    static inline String name = "L" + std::to_string(N);
    template <typename L, typename R, typename T>
    static void compute(const Eigen::MatrixBase<L> & left, const Eigen::MatrixBase<R> & right, PaddedPODArray<T> & array)
    {
        auto & norms = (left - right).colwise().template lpNorm<N>();
        for (auto n : norms)
            array.emplace_back(n);
    }
};

struct LinfDistance : LpDistance<Eigen::Infinity>
{
    static inline String name = "Linf";
};

struct CosineDistance
{
    static inline String name = "Cosine";
    template <typename L, typename R, typename T>
    static void compute(const Eigen::MatrixBase<L> & left, const Eigen::MatrixBase<R> & right, PaddedPODArray<T> & array)
    {
        // auto & nx = left.colwise().normalized().eval();
        // auto & ny = right.colwise().normalized().eval();
        // auto & dist = 1.0 - x.cwiseProduct(y).colwise().sum().array();
        auto & prod = left.cwiseProduct(right).colwise().sum();
        auto & nx = left.colwise().norm();
        auto & ny = right.colwise().norm();
        auto & nm = nx.cwiseProduct(ny).cwiseInverse();
        auto & dist = 1.0 - prod.cwiseProduct(nm).array();
        for (auto d : dist)
            array.emplace_back(d);
    }
};

template <class Kernel>
class FunctionArrayDistance : public IFunction
{
public:
    static inline auto name = "array" + Kernel::name + "Distance";
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayDistance<Kernel>>(); }
    size_t getNumberOfArguments() const override { return 2; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & /*arguments*/) const override
    {
        return std::make_shared<DataTypeFloat32>();
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        const auto & left = arguments[0];
        const auto & right = arguments[1];

        const auto & col_x = left.column->convertToFullColumnIfConst();
        const auto & col_y = right.column->convertToFullColumnIfConst();
        const auto * arr_x = checkAndGetColumn<ColumnArray>(col_x.get());
        const auto * arr_y = checkAndGetColumn<ColumnArray>(col_y.get());
        if (!arr_x || !arr_y)
        {
            throw Exception("Argument of function " + String(name) + " must be array. ", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }

        const auto & type_x = checkAndGetDataType<DataTypeArray>(left.type.get())->getNestedType();
        const auto & type_y = checkAndGetDataType<DataTypeArray>(right.type.get())->getNestedType();

        const auto & common_type = getLeastSupertype(DataTypes{type_x, type_y});
        switch (common_type->getTypeId())
        {
            case TypeIndex::UInt8:
            case TypeIndex::UInt16:
            case TypeIndex::UInt32:
            case TypeIndex::Int8:
            case TypeIndex::Int16:
            case TypeIndex::Int32:
            case TypeIndex::Float32:
                return executeWithType<float>(*arr_x, *arr_y, type_x, type_y, result_type);
            case TypeIndex::UInt64:
            case TypeIndex::Int64:
            case TypeIndex::Float64:
                return executeWithType<double>(*arr_x, *arr_y, type_x, type_y, result_type);
            default:
                throw Exception(
                    "Array nested type " + common_type->getName()
                        + ". Support: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

private:
    template <typename MatrixType>
    static ColumnPtr executeWithType(
        const ColumnArray & array_x,
        const ColumnArray & array_y,
        const DataTypePtr & type_x,
        const DataTypePtr & type_y,
        const DataTypePtr & result_type)
    {
        auto result = result_type->createColumn();
        auto & array = typeid_cast<ColumnFloat32 &>(*result).getData();

        Eigen::MatrixX<MatrixType> mx, my;
        columnToMatrix(array_x, type_x, mx);
        columnToMatrix(array_y, type_y, my);

        if (mx.rows() && my.rows() && mx.rows() != my.rows())
        {
            throw Exception(
                "Arguments of function " + String(name) + " have different array sizes.", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
        }

        Kernel::compute(mx, my, array);
        return result;
    }

    template <typename MatrixType>
    static void columnToMatrix(const ColumnArray & array, const DataTypePtr & nested_type, Eigen::MatrixX<MatrixType> & mat)
    {
        switch (nested_type->getTypeId())
        {
            case TypeIndex::UInt8:
                fillMatrix<MatrixType, UInt8>(mat, array);
                break;
            case TypeIndex::UInt16:
                fillMatrix<MatrixType, UInt16>(mat, array);
                break;
            case TypeIndex::UInt32:
                fillMatrix<MatrixType, UInt32>(mat, array);
                break;
            case TypeIndex::UInt64:
                fillMatrix<MatrixType, UInt64>(mat, array);
                break;
            case TypeIndex::Int8:
                fillMatrix<MatrixType, Int8>(mat, array);
                break;
            case TypeIndex::Int16:
                fillMatrix<MatrixType, Int16>(mat, array);
                break;
            case TypeIndex::Int32:
                fillMatrix<MatrixType, Int32>(mat, array);
                break;
            case TypeIndex::Int64:
                fillMatrix<MatrixType, Int64>(mat, array);
                break;
            case TypeIndex::Float32:
                fillMatrix<MatrixType, Float32>(mat, array);
                break;
            case TypeIndex::Float64:
                fillMatrix<MatrixType, Float64>(mat, array);
                break;
            default:
                throw Exception(
                    "Array nested type " + nested_type->getName()
                        + ". Support: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    // optimize for float/ double
    template <typename MatrixType, typename DataType, typename std::enable_if<std::is_same<MatrixType, DataType>::value>::type>
    static void fillMatrix(Eigen::MatrixX<MatrixType> & mat, const ColumnArray & array)
    {
        const auto & vec = typeid_cast<const ColumnVector<DataType> &>(array.getData());
        const auto & data = vec.getData();
        const auto & offsets = array.getOffsets();
        mat = Eigen::Map<const Eigen::MatrixX<MatrixType>>(data.data(), offsets.front(), offsets.size());
    }

    template <typename MatrixType, typename DataType>
    static void fillMatrix(Eigen::MatrixX<MatrixType> & mat, const ColumnArray & array)
    {
        const auto & vec = typeid_cast<const ColumnVector<DataType> &>(array.getData());
        const auto & data = vec.getData();
        const auto & offsets = array.getOffsets();
        mat.resize(offsets.front(), offsets.size());

        ColumnArray::Offset prev = 0, col = 0;
        for (auto off : offsets)
        {
            for (ColumnArray::Offset row = 0; row < off - prev; ++row)
            {
                mat(row, col) = static_cast<MatrixType>(data[prev + row]);
            }
            ++col;
            prev = off;
        }
    }
};

void registerFunctionArrayDistance(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayDistance<LpDistance<1>>>();
    factory.registerFunction<FunctionArrayDistance<LpDistance<2>>>();
    factory.registerFunction<FunctionArrayDistance<LinfDistance>>();
    factory.registerFunction<FunctionArrayDistance<CosineDistance>>();
}

}
