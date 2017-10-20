#include <Functions/FunctionsExternalModels.h>
#include <Functions/FunctionHelpers.h>
#include <Functions/FunctionFactory.h>

#include <Interpreters/Context.h>
#include <Interpreters/ExternalModels.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <ext/range.h>
#include <iostream>
#include <boost/dll/import.hpp>

namespace DB
{

FunctionPtr FunctionModelEvaluate::create(const Context & context)
{
    return std::make_shared<FunctionModelEvaluate>(reinterpret_cast<const ExternalModels &>((void*)(0)), //context.getExternalModels(),
                                                   context.getConfigRef().getString("catboost_dynamic_library_path"));
}

namespace ErrorCodes
{
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int TOO_LESS_ARGUMENTS_FOR_FUNCTION;
    extern const int ILLEGAL_COLUMN;
}

DataTypePtr FunctionModelEvaluate::getReturnTypeImpl(const DataTypes & arguments) const
{
    if (arguments.size() < 2)
        throw Exception("Function " + getName() + " expects at least 2 arguments",
                        ErrorCodes::TOO_LESS_ARGUMENTS_FOR_FUNCTION);

    if (!checkDataType<DataTypeString>(arguments[0].get()))
        throw Exception("Illegal type " + arguments[0]->getName() + " of first argument of function " + getName()
                        + ", expected a string.", ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);


    return std::make_shared<DataTypeFloat64>();
}

void FunctionModelEvaluate::executeImpl(Block & block, const ColumnNumbers & arguments, size_t result)
{
    const auto name_col = checkAndGetColumnConst<ColumnString>(block.getByPosition(arguments[0]).column.get());
    if (!name_col)
        throw Exception("First argument of function " + getName() + " must be a constant string",
                        ErrorCodes::ILLEGAL_COLUMN);


    typedef void ModelCalcerHandle;

    std::string lib_path = path;
    std::string model_path = name_col->getValue<String>();

    std::cout << "Catboost lib path: " << lib_path << std::endl;
    std::cout << "Model path: " << model_path << std::endl;

    std::vector<int> v = {3, 4, 5};
    std::vector<int> vv = v;
    for (auto num : v)
        std::cout << num << std::endl;

    boost::dll::shared_library lib(lib_path);
    auto creater = lib.get<ModelCalcerHandle *()>("ModelCalcerCreate");
    auto destroyer = lib.get<void(ModelCalcerHandle *)>("ModelCalcerDelete");
    auto loader = lib.get<bool(ModelCalcerHandle *, const char*)>("LoadFullModelFromFile");

    std::cout << "Loaded lib" << std::endl;

    auto model = creater();
    std::cout << "created model" << std::endl;
    loader(model, model_path.c_str());
    std::cout << "loaded model" << std::endl;
    destroyer(model);
    std::cout << "destroyed model" << std::endl;


//    auto model = models.getModel(name_col->getValue<String>());
//
//    Columns columns(arguments.size() - 1);
//    for (auto i : ext::range(0, columns.size()))
//        columns[i] = block.getByPosition(arguments[i + 1]).column;
//
//    block.getByPosition(result).column = model->evaluate(columns);
}

void registerFunctionsExternalModels(FunctionFactory & factory)
{
    factory.registerFunction<FunctionModelEvaluate>();
}

}
