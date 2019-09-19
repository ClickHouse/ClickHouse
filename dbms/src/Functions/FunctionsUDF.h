#pragma once

#include <Columns/ColumnConst.h>
#include <Common/typeid_cast.h>
#include <Functions/IFunction.h>
#include <Interpreters/Compiler.h>

#include <sstream>


namespace DB
{

namespace ErrorCodes
{
    extern const int CANNOT_COMPILE_CODE;
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
}

template <typename Name, const char * Header, const char * Body>
class FunctionUDF final : public IFunction
{
private:
    mutable SharedLibraryPtr lib;

    mutable void (* funcType)(
        DataTypePtr &,
        const ColumnsWithTypeAndName &
    );
    mutable void (* funcExec)(
        Block &,
        const ColumnNumbers &,
        size_t,
        size_t
    );

public:
    static constexpr auto name = Name::name;

    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionUDF>();
    }

    String getName() const override
    {
        return Name::name;
    }

    bool isDeterministic() const override
    {
        return false;
    }

    bool isDeterministicInScopeOfQuery() const override
    {
        return false;
    }

    bool isVariadic() const override
    {
        return true;
    }

    size_t getNumberOfArguments() const override
    {
        return 0;
    }

    // bool useDefaultImplementationForConstants() const override
    // {
    //     return true;
    // }

    DataTypePtr getReturnTypeImpl(
        const ColumnsWithTypeAndName & arguments
    ) const override
    {
        if (arguments.size() < 1)
            throw Exception {
                "Function " + getName() + " requires at least one arguments",
                ErrorCodes::NUMBER_OF_ARGUMENTS_DOESNT_MATCH
            };

        auto col_code_const = typeid_cast<const ColumnConst *>(
            arguments.front().column.get()
        );

        if (!col_code_const)
            throw Exception {
                "Illegal non-const column "
                    + arguments.front().column->getName()
                    + " of argument of function " + getName(),
                ErrorCodes::ILLEGAL_COLUMN
            };

        String code {
            col_code_const->getValue<String>()
        };

        static Compiler compiler {
            "/dev/shm/ClickHouseUDF",
            1
        };

        lib = compiler.getOrCount(
            getName() + ':' + code,
            0,
            "",
            [code]() -> std::string
            {
                std::ostringstream out;

                out << Header << std::endl;
                out << std::endl;

                out << code << std::endl;
                out << std::endl;

                out << Body << std::endl;

                return out.str();
            },
            [](SharedLibraryPtr &)
            {
                // nothing
            }
        );

        if (!lib)
            throw Exception {
                "Code of user-defined function " + getName()
                    + " does not compile",
                ErrorCodes::CANNOT_COMPILE_CODE
            };

        funcType = lib->template get<decltype(funcType)>("funcType");
        funcExec = lib->template get<decltype(funcExec)>("funcExec");

        DataTypePtr result;
        funcType(result, arguments);

        if (!result)
            throw Exception {
                "Argument types of user-defined function " + getName()
                    + " does not match",
                ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT
            };

        return result;
    }

    void executeImpl(
        Block & block,
        const ColumnNumbers & arguments,
        size_t result_pos,
        size_t input_rows_count
    ) override
    {
        funcExec(block, arguments, result_pos, input_rows_count);
    }
};

}
