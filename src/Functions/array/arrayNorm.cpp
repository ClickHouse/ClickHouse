#include <Columns/ColumnArray.h>
#include <Columns/IColumn.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/IDataType.h>
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
}

template <const int N>
struct LpNorm
{
    static inline String name = "L" + std::to_string(N);
    template <typename T>
    static void compute(const std::vector<Eigen::VectorX<T>> & vec, PaddedPODArray<T> & array)
    {
        array.reserve(vec.size());
        for (const auto & v : vec)
        {
            array.push_back(v.template lpNorm<N>());
        }
    }
};

struct LinfNorm : LpNorm<Eigen::Infinity>
{
    static inline String name = "Linf";
};

template <class Kernel>
class FunctionArrayNorm : public IFunction
{
public:
    static inline auto name = "array" + Kernel::name + "Norm";
    String getName() const override { return name; }
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionArrayNorm<Kernel>>(); }
    size_t getNumberOfArguments() const override { return 1; }
    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return true; }
    bool useDefaultImplementationForConstants() const override { return true; }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        DataTypes types;
        for (const auto & argument : arguments)
        {
            const auto * array_type = checkAndGetDataType<DataTypeArray>(argument.type.get());
            if (!array_type)
                throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "Argument of function {} must be array.", getName());

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
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} has nested type {}. "
                    "Support: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.",
                    getName(), common_type->getName());
        }
    }

    ColumnPtr
    executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr & result_type, size_t /*input_rows_count*/) const override
    {
        DataTypePtr type = typeid_cast<const DataTypeArray *>(arguments[0].type.get())->getNestedType();
        ColumnPtr column = arguments[0].column->convertToFullColumnIfConst();
        const auto * arr = assert_cast<const ColumnArray *>(column.get());

        auto result = result_type->createColumn();
        switch (result_type->getTypeId())
        {
            case TypeIndex::Float32:
                executeWithType<Float32>(*arr, type, result);
                break;
            case TypeIndex::Float64:
                executeWithType<Float64>(*arr, type, result);
                break;
            default:
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Unexpected result type.");
        }
        return result;
    }

private:
    template <typename MatrixType>
    void executeWithType(const ColumnArray & array, const DataTypePtr & type, MutableColumnPtr & column) const
    {
        std::vector<Eigen::VectorX<MatrixType>> vec;
        columnToVectors(array, type, vec);
        auto & data = assert_cast<ColumnVector<MatrixType> &>(*column).getData();
        Kernel::compute(vec, data);
    }

    template <typename MatrixType>
    void columnToVectors(const ColumnArray & array, const DataTypePtr & nested_type, std::vector<Eigen::VectorX<MatrixType>> & vec) const
    {
        switch (nested_type->getTypeId())
        {
            case TypeIndex::UInt8:
                fillVectors<MatrixType, UInt8>(vec, array);
                break;
            case TypeIndex::UInt16:
                fillVectors<MatrixType, UInt16>(vec, array);
                break;
            case TypeIndex::UInt32:
                fillVectors<MatrixType, UInt32>(vec, array);
                break;
            case TypeIndex::UInt64:
                fillVectors<MatrixType, UInt64>(vec, array);
                break;
            case TypeIndex::Int8:
                fillVectors<MatrixType, Int8>(vec, array);
                break;
            case TypeIndex::Int16:
                fillVectors<MatrixType, Int16>(vec, array);
                break;
            case TypeIndex::Int32:
                fillVectors<MatrixType, Int32>(vec, array);
                break;
            case TypeIndex::Int64:
                fillVectors<MatrixType, Int64>(vec, array);
                break;
            case TypeIndex::Float32:
                fillVectors<MatrixType, Float32>(vec, array);
                break;
            case TypeIndex::Float64:
                fillVectors<MatrixType, Float64>(vec, array);
                break;
            default:
                throw Exception(
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT,
                    "Arguments of function {} has nested type {}. "
                    "Support: UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64, Float32, Float64.",
                    getName(), nested_type->getName());
        }
    }

    template <typename MatrixType, typename DataType>
    requires std::is_same_v<MatrixType, DataType>
    void fillVectors(std::vector<Eigen::VectorX<MatrixType>> & vec, const ColumnArray & array) const
    {
        const auto & data = typeid_cast<const ColumnVector<DataType> &>(array.getData()).getData();
        const auto & offsets = array.getOffsets();
        vec.reserve(offsets.size());
        ColumnArray::Offset prev = 0;
        for (auto off : offsets)
        {
            vec.emplace_back(Eigen::Map<const Eigen::VectorX<MatrixType>>(data.data() + prev, off - prev));
            prev = off;
        }
    }

    template <typename MatrixType, typename DataType>
    void fillVectors(std::vector<Eigen::VectorX<MatrixType>> & vec, const ColumnArray & array) const
    {
        const auto & data = typeid_cast<const ColumnVector<DataType> &>(array.getData()).getData();
        const auto & offsets = array.getOffsets();
        vec.reserve(offsets.size());

        ColumnArray::Offset prev = 0;
        for (auto off : offsets)
        {
            Eigen::VectorX<MatrixType> mat(off - prev);
            for (ColumnArray::Offset row = 0; row + prev < off; ++row)
            {
                mat[row] = static_cast<MatrixType>(data[prev + row]);
            }
            prev = off;
            vec.emplace_back(mat);
        }
    }
};

void registerFunctionArrayNorm(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayNorm<LpNorm<1>>>();
    factory.registerFunction<FunctionArrayNorm<LpNorm<2>>>();
    factory.registerFunction<FunctionArrayNorm<LinfNorm>>();
}

}
