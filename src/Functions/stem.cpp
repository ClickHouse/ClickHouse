#include <Functions/FunctionStringToString.h>
#include <Functions/FunctionFactory.h>
//#include <Poco/Unicode.h>


#include <Columns/ColumnString.h>
#include <Columns/ColumnVector.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/IFunctionImpl.h>
#include <Functions/castTypeToEither.h>

#include <sphinxstemen.cpp>
#include <sphinxstemru.cpp>


namespace DB
{
namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int UNSUPPORTED_LANGUAGE;
}

namespace
{

struct StemImpl
{
    
    static void vector(
        const ColumnString::Chars & data,
        const ColumnString::Offsets & offsets,
        ColumnString::Chars & res_data,
        ColumnString::Offsets & res_offsets,
        // TODO: Change this to function stem_lang
        const String & language)
    {
        /// Add 2 because some realizations of stem need 
        res_data.resize(data.size() + 2);
        res_offsets.assign(offsets);
        
        // change
        if (language == "en") {
            stem_en_init();
        } else if (language == "ru") {
            stem_ru_init();
        }
        

        UInt64 data_size = 0;
        for (UInt64 i = 0; i < offsets.size(); ++i)
        {
            /// Note that accessing -1th element is valid for PaddedPODArray.
            size_t original_size = offsets[i] - offsets[i - 1];
            memcpy(res_data.data() + data_size, data.data() + offsets[i - 1], original_size);
            
            stemImpl(res_data.data() + data_size, original_size - 1, language);

            UInt64 new_size = length(res_data.data() + data_size);
            data_size += new_size;
            res_offsets[i] = data_size;
        }
        res_data.resize(data_size);
    }

    static UInt64 length(const UInt8 * src) {
        UInt64 res = 0;
        while (*src++ != 0) {
            res++;
        }
        return res + 1;
    }

private:

    static void stemImpl(UInt8 * dst, UInt64 length, const String & language) {
        if (language == "en") {
            stem_en(reinterpret_cast<unsigned char *>(dst), length);
        } else if (language == "ru") {
            stem_ru_utf8(reinterpret_cast<unsigned short *>(dst));
        }
    }

};


class FunctionStem : public IFunction
{
    template <typename F>
    static bool castType(const IDataType * type, F && f)
    {
        return castTypeToEither<DataTypeUInt8, DataTypeUInt16, DataTypeUInt32, DataTypeUInt64>(type, std::forward<F>(f));
    }

public:
    static constexpr auto name = "stem";
    static FunctionPtr create(ContextPtr) { return std::make_shared<FunctionStem>(); }

    String getName() const override { return name; }

    size_t getNumberOfArguments() const override { return 2; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(
                "Illegal type " + arguments[0]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (!isString(arguments[1]))
            throw Exception(
                "Illegal type " + arguments[1]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        return arguments[1];
    }

    bool useDefaultImplementationForConstants() const override { return true; }

    ColumnPtr executeImpl(const ColumnsWithTypeAndName & arguments, const DataTypePtr &, size_t) const override
    {
        const auto & langcolumn = arguments[0].column;
        const auto & strcolumn = arguments[1].column;
        ColumnPtr res;

        if (const ColumnString * col = checkAndGetColumn<ColumnString>(strcolumn.get())) 
        {
            if (const ColumnConst * scale_column_num = checkAndGetColumn<ColumnConst>(langcolumn.get()))
            {
                String language = scale_column_num->getValue<String>();
                if (language == "en") {
                     auto col_res = ColumnString::create();
                    StemImpl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets(), language);
                    return col_res;
                } else if (language == "ru") {
                     auto col_res = ColumnString::create();
                    StemImpl::vector(col->getChars(), col->getOffsets(), col_res->getChars(), col_res->getOffsets(), language);
                    return col_res;
                }
                 else {
                    throw Exception(
                    "Language " + language + " is not supported for function " + getName(),
                    ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
                }
               
            }
        }

        throw Exception(
            "Illegal column " + arguments[0].column->getName() + " of argument of function " + getName(),
            ErrorCodes::ILLEGAL_COLUMN);
    }
};

}

void registerFunctionStem(FunctionFactory & factory)
{
    factory.registerFunction<FunctionStem>(FunctionFactory::CaseInsensitive);
}

}
