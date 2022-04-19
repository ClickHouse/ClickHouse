#include <Columns/ColumnArray.h>
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
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int LOGICAL_ERROR;
    extern const int SIZES_OF_ARRAYS_DOESNT_MATCH;
}

template <const int N>
struct LpDistance
{
    static inline String name = "L" + std::to_string(N);
    template <typename T>
    static void compute(const Eigen::MatrixX<T> & left, const Eigen::MatrixX<T> & right, PaddedPODArray<T> & array)
    {
        auto & norms = (left - right).colwise().template lpNorm<N>();
        // array.insert() failed to work with Eigen iterators
        for (auto n : norms)
            array.push_back(n);
    }
};

struct LinfDistance : LpDistance<Eigen::Infinity>
{
    static inline String name = "Linf";
};

struct CosineDistance
{
    static inline String name = "Cosine";
    template <typename T>
    static void compute(const Eigen::MatrixX<T> & left, const Eigen::MatrixX<T> & right, PaddedPODArray<T> & array)
    {
        auto & prod = left.cwiseProduct(right).colwise().sum();
        auto & nx = left.colwise().norm();
        auto & ny = right.colwise().norm();
        auto & nm = nx.cwiseProduct(ny).cwiseInverse();
        auto & dist = 1.0 - prod.cwiseProduct(nm).array();
        for (auto d : dist)
            array.push_back(d);
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

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        DataTypes types;
        for (const auto & argument : arguments)
        {
            const auto * array_type = checkAndGetDataType<DataTypeArray>(argument.type.get());
            if (!array_type)
                throw Exception("Argument of function " + getName() + " must be array. ", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

            types.push_back(array_type->getNestedType());
        }
        const auto & common_type = getLeastSupertype(types);
        switch (common_type->getTypeId())
        {
            case TypeIndex::UInt8:
            case TypeIndex::UInt16:
            case TypeIndex::UInt32:
            case TypeIndex::Int8:
            case TypeIndex::Int16:
            case TypeIndex::Int32:
            case TypeIndex::Float32:
                return std::make_shared<DataTypeFloat32>();
            case TypeIndex::UInt64:
            case TypeIndex::Int64:
            case TypeIndex::Float64:
                return std::make_shared<DataTypeFloat64>();
            default:
                throw Exception(
                    "Arguments of function " + getName() + " has nested type " + common_type->getName()
                        + ". Support: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        const auto & type_x = typeid_cast<const DataTypeArray *>(arguments[0].type.get())->getNestedType();
        const auto & type_y = typeid_cast<const DataTypeArray *>(arguments[1].type.get())->getNestedType();

        const auto & col_x = arguments[0].column->convertToFullColumnIfConst();
        const auto & col_y = arguments[1].column->convertToFullColumnIfConst();

        const auto * arr_x = assert_cast<const ColumnArray *>(col_x.get());
        const auto * arr_y = assert_cast<const ColumnArray *>(col_y.get());

        auto result = result_type->createColumn();
        switch (result_type->getTypeId())
        {
            case TypeIndex::Float32:
                executeWithType<Float32>(*arr_x, *arr_y, type_x, type_y, result);
                break;
            case TypeIndex::Float64:
                executeWithType<Float64>(*arr_x, *arr_y, type_x, type_y, result);
                break;
            default:
                throw Exception("Unexpected result type.", ErrorCodes::LOGICAL_ERROR);
        }
        return result;
    }

private:
    template <typename MatrixType>
    void executeWithType(
        const ColumnArray & array_x,
        const ColumnArray & array_y,
        const DataTypePtr & type_x,
        const DataTypePtr & type_y,
        MutableColumnPtr & column) const
    {
        Eigen::MatrixX<MatrixType> mx, my;
        columnToMatrix(array_x, type_x, mx);
        columnToMatrix(array_y, type_y, my);

        if (mx.rows() && my.rows() && mx.rows() != my.rows())
        {
            throw Exception(
                "Arguments of function " + getName() + " have different array sizes.", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);
        }
        auto & data = assert_cast<ColumnVector<MatrixType> &>(*column).getData();
        Kernel::compute(mx, my, data);
    }

    template <typename MatrixType>
    void columnToMatrix(const ColumnArray & array, const DataTypePtr & nested_type, Eigen::MatrixX<MatrixType> & mat) const
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
                    "Arguments of function " + getName() + " has nested type " + nested_type->getName()
                        + ". Support: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.",
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        }
    }

    // optimize for float/ double
    template <typename MatrixType, typename DataType, typename std::enable_if<std::is_same<MatrixType, DataType>::value>::type>
    void fillMatrix(Eigen::MatrixX<MatrixType> & mat, const ColumnArray & array) const
    {
        const auto & data = typeid_cast<const ColumnVector<DataType> &>(array.getData()).getData();
        const auto & offsets = array.getOffsets();
        mat = Eigen::Map<const Eigen::MatrixX<MatrixType>>(data.data(), offsets.front(), offsets.size());
    }

    template <typename MatrixType, typename DataType>
    void fillMatrix(Eigen::MatrixX<MatrixType> & mat, const ColumnArray & array) const
    {
        const auto & data = typeid_cast<const ColumnVector<DataType> &>(array.getData()).getData();
        const auto & offsets = array.getOffsets();
        size_t rows = offsets.front(), cols = offsets.size();
        mat.resize(rows, cols);

        ColumnArray::Offset prev = 0, col = 0;
        for (ColumnArray::Offset off : offsets)
        {
            if (off - prev != rows)
                throw Exception(
                    "Arguments of function " + getName() + " have different array sizes.", ErrorCodes::SIZES_OF_ARRAYS_DOESNT_MATCH);

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
