#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/GatherUtils/GatherUtils.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/getLeastSupertype.h>
#include <Interpreters/castColumn.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>
#include <ext/range.h>
/// my code
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnsNumber.h>
/// my code

///
namespace DB{
namespace FunctionsInterpolation{
class FunctionArrayInterpalationLineary
{
public: 
virtual ~FunctionArrayInterpalationLineary()=default;
    
    virtual Float64 valueInterpolate( const Float64* xData,  const Float64* yData,  Float64 x, bool extrapolate
                                    , size_t size )
    {
//        int size = xData.size();
    
       size_t i = 0;                                                                  // find left end of interval for interpolation
       if ( x >= xData[size - 2] )                                                 // special case: beyond right end
       {
          i = size - 2;
       }
       else
       {
          while ( x > xData[i+1] ) i++;
       }
       Float64 xL = xData[i], yL = yData[i], xR = xData[i+1], yR = yData[i+1];      // points on either side (unless beyond ends)
       if ( !extrapolate )                                                         // if beyond ends of array and not extrapolating
       {
          if ( x < xL ) yR = yL;
          if ( x > xR ) yL = yR;
       }
    
       Float64 dydx = ( yR - yL ) / ( xR - xL );                                    // gradient
    
       return yL + dydx * ( x - xL );                                              // linear interpolation
    }    
    
    virtual void arrayInterpolate( const Float64* xData,  const Float64* yData,  const Float64* x, bool extrapolate
                                    , size_t size, size_t size_out, Float64* y )
    {
        for (size_t i = 0; i < size_out; ++i)
        {
        
            y[i] = valueInterpolate( xData,  yData,  x[i], extrapolate, size );
        }
    }
    
    
    
};

}
}
///


namespace DB
{

namespace ErrorCodes
{
    extern const int LOGICAL_ERROR;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
}


/// arrayInterpolationLinear(arr0, arr1, arr2) - interpolation arr2.
class FunctionArrayInterpolationLinear : public IFunction
{
public:
    static constexpr auto name = "arrayInterpolationLinear";
    static FunctionPtr create(const Context & context) { return std::make_shared<FunctionArrayInterpolationLinear>(context); }
    FunctionArrayInterpolationLinear(const Context & context) : context(context) {}

    String getName() const override { return name; }

//     bool isVariadic() const override { return true; }
//     size_t getNumberOfArguments() const override { return 0; }
    bool isVariadic() const override { return false; }
    size_t getNumberOfArguments() const override { return 3; } /// waaiting only 3 arrays

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.empty())
            throw Exception{"Function " + getName() + " requires at least tree argument.", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH};

        for (auto i : ext::range(0, arguments.size()))
        {
            auto array_type = typeid_cast<const DataTypeArray *>(arguments[i].get());
            if (!array_type)
                throw Exception("Argument " + std::to_string(i) + " for function " + getName() + " must be an array but it has type "
                                + arguments[i]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);

// my_code
            if (array_type -> getNestedType() -> getName() != "Float64")
                throw Exception("Argument " + std::to_string(i) + " for function " + getName() + " must be an array with type Float64"
                                + arguments[i]->getName() + ".", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
// my_code
            
        }

                
//         return getLeastSupertype(arguments);
//         return std::make_shared<DataTypeFloat64>();
        return arguments[0];

    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
        const DataTypePtr & return_type = block.getByPosition(result).type;

        if (return_type->onlyNull())
        {
            block.getByPosition(result).column = return_type->createColumnConstWithDefaultValue(input_rows_count);
            return;
        }

//         auto result_column = return_type->createColumn();

        size_t rows = input_rows_count;



//         block.getByPosition(result).column = std::move(block.getByPosition(arguments[2]).column);


        const auto & arg0 = block.getByPosition(arguments[0]).column;
        const auto * arg0_type = typeid_cast<const ColumnArray *>(arg0.get());
        const auto * arg0_data = typeid_cast<const ColumnFloat64 *>( (arg0_type -> getDataPtr()).get() );
//             const auto & arg_offsets = arg -> getOffsetsPtr();
//             const auto * arg_length = <const ColumnUInt64 *>(arg_offsets.get()));
        const auto & arg1 = block.getByPosition(arguments[1]).column;
        const auto * arg1_type = typeid_cast<const ColumnArray *>(arg1.get());
        const auto * arg1_data = typeid_cast<const ColumnFloat64 *>( (arg1_type -> getDataPtr()).get() );
        
        const auto & arg2 = block.getByPosition(arguments[2]).column;
        const auto * arg2_type = typeid_cast<const ColumnArray *>(arg2.get());
        const auto * arg2_data = typeid_cast<const ColumnFloat64 *>( (arg2_type -> getDataPtr()).get() );
        
        for (size_t i = 0; i < rows; ++i)
        {
//                 size_t arg_cur_offset =  arg_length -> getElement(i);
//                 if i != 0:
            size_t arg0_cur_offset = arg0_type -> sizeAt(i);
            size_t arg1_cur_offset = arg1_type -> sizeAt(i);
            if (arg0_cur_offset != arg1_cur_offset)
                throw Exception("Argument in row " + std::to_string(i) + " has not equal lengths."
                                            , ErrorCodes::BAD_ARGUMENTS); /// TODO Create self code error                
        }

// my_code
        /// Call function interpolation
        FunctionsInterpolation::FunctionArrayInterpalationLineary arrayInterpolateClass;
        
        bool extrapolate = false;

        size_t size_in = 0;
        size_t size_out = 0;

//         auto y_return = block.getByPosition(arguments[2]).column -> clone();
        auto y_return_temp = block.getByPosition(arguments[2]).column;
        auto y_return = (*std::move(y_return_temp)).mutate();
        
        auto * y_return_type = typeid_cast<ColumnArray *>(y_return.get());
        auto * y_return_data = typeid_cast<ColumnFloat64 *>( & (y_return_type -> getData()) );
        size_t n = y_return_data -> size();
        (y_return_data -> getData()).resize_fill( n );
        
        for (size_t i = 0; i < rows; ++i)
        {
            size_in  = arg0_type -> sizeAt(i);
            size_out = arg2_type -> sizeAt(i);

            auto xData = arg0_data -> getData().data() + arg0_type -> offsetAt(i);
            auto yData = arg1_data -> getData().data() + arg1_type -> offsetAt(i);
            auto x = arg2_data -> getData().data() + arg2_type -> offsetAt(i);
            auto y = y_return_data -> getData().data() + y_return_type -> offsetAt(i);

            arrayInterpolateClass.arrayInterpolate( xData,  yData,  x, extrapolate
                                        , size_in, size_out, y );
        }

//         return y_return;
        block.getByPosition(result).column = std::move(y_return);

    }
        /// Call function interpolation end
// my_code

    bool useDefaultImplementationForConstants() const override { return true; }

private:
    const Context & context;
};


void registerFunctionArrayInterpolationLinear(FunctionFactory & factory)
{
    factory.registerFunction<FunctionArrayInterpolationLinear>();
}

}
