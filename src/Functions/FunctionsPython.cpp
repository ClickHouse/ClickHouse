#include <Functions/FunctionsPython.h>

#include <mutex>

#include <Functions/IFunctionImpl.h>

#include <DataTypes/DataTypeFactory.h>
#include <Common/Exception.h>

#if defined(__clang__)
#pragma clang diagnostic ignored "-Wold-style-cast"
#endif

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"

//#include <python3.8/pyconfig.h>
extern "C"
{
#include <Python.h>
}
#include <boost/python.hpp>
#include <boost/python/numpy.hpp>

#pragma GCC diagnostic pop


namespace DB
{


namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
}

namespace bpython = boost::python;
namespace bnumpy = boost::python::numpy;

static class Python {
public:
    Python() {
        Py_Initialize();
        PyEval_InitThreads();
        bnumpy::initialize();
    }
    ~Python() {
        /// Boost currently does not support python finalize. Can not call this...
        /// https://www.boost.org/doc/libs/1_73_0/libs/python/doc/html/tutorial/tutorial/embedding.html#tutorial.embedding.getting_started
        // Py_Finalize();
    }
} python;

class FunctionPython : public IFunction
{
public:
    struct DataTypeWithName {
        DataTypePtr type;
        bnumpy::dtype numpy_type;
        std::string name;
    };

    explicit FunctionPython(std::string name_, std::string body_, std::vector<DataTypeWithName> args_, DataTypePtr return_type_)
        : name(name_), body(body_), args(std::move(args_)), return_type(return_type_) {}

    std::string getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (arguments.size() != args.size()) {
            throw Exception("Function " + getName() + " require " + std::to_string(args.size()) + " arguments", ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH);
        }
        for (size_t i = 0; i != arguments.size(); ++i) {
            if (!arguments[i]->equals(*args[i].type)) {
                throw Exception("Illegal type " + arguments[i]->getName() + " of argument of function " + getName(), ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
            }
        }
        return return_type;
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; } /// @TODO Igr

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result, size_t input_rows_count) override
    {
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wold-style-cast"
        auto column = block.getByPosition(result).type->createColumn()->cloneResized(input_rows_count);

        try
        {
            static std::mutex locker;
            std::unique_lock lock(locker);
            bpython::dict locals = get_locals_from_arguments(block, arguments, input_rows_count);
            bpython::exec(body.c_str(), locals, locals);
            if (!locals.has_key("result")) {
                throw Exception("Wrong result type", 0);
            }
            auto result_array = bpython::extract<bnumpy::ndarray>(locals["result"]);
            std::cerr << input_rows_count << ' ' << column->getRawData().size << std::endl;
            memcpy(const_cast<char*>(column->getRawData().data), result_array().get_data(), column->getRawData().size);
        }
        catch (const bpython::error_already_set &)
        {
            PyObject *ptype, *pvalue, *ptraceback;
            PyErr_Fetch(&ptype, &pvalue, &ptraceback);

            bpython::handle<> hType(ptype);
            bpython::object extype(hType);
            bpython::handle<> hTraceback(ptraceback);
            bpython::object traceback(hTraceback);

            std::string strError = bpython::extract<std::string>(pvalue);
            long lineno = bpython::extract<long> (traceback.attr("tb_lineno"));

            throw Exception("Python error: " + strError + " at line " + std::to_string(lineno), 0);
        }

#pragma GCC diagnostic pop

        block.getByPosition(result).column = std::move(column);
    }

private:
    bpython::dict get_locals_from_arguments(Block & block, const ColumnNumbers & arguments, size_t input_rows_count) const {
        bpython::dict locals;

        for (auto argument_pos : arguments) {
            const auto & argument_name = args[argument_pos].name;
            if (!block.getByPosition(argument_pos).column->isFixedAndContiguous()) {
                throw Exception("Unsupported value type " + block.getByPosition(argument_pos).type->getName() + " as argument: not FixedAndContiguous " + argument_name, 0);
            }
            locals[argument_name] = bnumpy::from_data(block.getByPosition(argument_pos).column->getRawData().data,
                                                      args[argument_pos].numpy_type, bpython::make_tuple(input_rows_count),
                                                      bpython::make_tuple(input_rows_count), bpython::object());
        }

        return locals;
    }

    std::string name;
    std::string body;
    std::vector<DataTypeWithName> args;
    DataTypePtr return_type;
};

void testRegisterFunctionsPython(FunctionFactory & factory)
{
    factory.registerUserDefinedFunction("python_test", [](const Context &){
        std::string text = "    print(x)\n"
                           "    return x * 2";
        return std::make_unique<DefaultOverloadResolver>(std::make_unique<FunctionPython>("python_test", "def f():\n" + text + "\nresult = f()",
                                                                                          std::vector<FunctionPython::DataTypeWithName>{{DataTypeFactory::instance().get("Int32"), bnumpy::dtype::get_builtin<int32_t>(), "x"}},
                                                                                          DataTypeFactory::instance().get("Int32")));
    });
    factory.registerUserDefinedFunction("python_test2", [](const Context &){
        std::string text = "    print(x)\n"
                           "    return numpy.array((i for i in range(x.shape[0])))";
        return std::make_unique<DefaultOverloadResolver>(std::make_unique<FunctionPython>("python_test", "import numpy\ndef f():\n" + text + "\nresult = f()",
                                                                                          std::vector<FunctionPython::DataTypeWithName>{{DataTypeFactory::instance().get("Int32"), bnumpy::dtype::get_builtin<int32_t>(), "x"}},
                                                                                          DataTypeFactory::instance().get("Int32")));
    });
}

}

