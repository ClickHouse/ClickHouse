#include <Functions/FunctionFactory.h>
#include <Functions/registerFunctions.h>

struct RegisteredFunctionsState
{
    RegisteredFunctionsState()
    {
        DB::registerFunctions();
    }

    RegisteredFunctionsState(RegisteredFunctionsState &&) = default;
};

inline void tryRegisterFunctions()
{
    static RegisteredFunctionsState registered_functions_state;
}
