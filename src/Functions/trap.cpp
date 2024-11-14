#if 0

#include <Functions/IFunction.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Columns/ColumnString.h>
#include <Interpreters/Context.h>
#include <base/scope_guard.h>

#include <thread>
#include <memory>
#include <cstdlib>
#include <unistd.h>
#include <sys/mman.h>
#include <dlfcn.h>


namespace DB
{

namespace ErrorCodes
{
    extern const int ILLEGAL_COLUMN;
    extern const int ILLEGAL_TYPE_OF_ARGUMENT;
    extern const int BAD_ARGUMENTS;
    extern const int CANNOT_ALLOCATE_MEMORY;
    extern const int CANNOT_DLOPEN;
    extern const int LOGICAL_ERROR;
}


/// Various illegal actions to test diagnostic features of ClickHouse itself. Should not be enabled in production builds.
class FunctionTrap : public IFunction
{
private:
    ContextPtr context;

public:
    static constexpr auto name = "trap";
    static FunctionPtr create(ContextPtr context)
    {
        return std::make_shared<FunctionTrap>(context);
    }

    FunctionTrap(ContextPtr context_) : context(context_) {}

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 1;
    }

    bool isSuitableForShortCircuitArgumentsExecution(const DataTypesWithConstInfo & /*arguments*/) const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override
    {
        if (!isString(arguments[0]))
            throw Exception(ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT, "The only argument for function {} must be constant String", getName());

        return std::make_shared<DataTypeUInt8>();
    }

    [[clang::optnone]]
    ColumnPtr executeImpl(const ColumnsWithTypeAndName & block, const DataTypePtr & result_type, size_t input_rows_count) const override
    {
        if (const ColumnConst * column = checkAndGetColumnConst<ColumnString>(block[0].column.get()))
        {
            String mode = column->getValue<String>();

            if (mode == "read nullptr c++")
            {
                volatile int x = *reinterpret_cast<const volatile int *>(0);
                (void)x;
            }
            else if (mode == "read nullptr asm")
            {
                __asm__ volatile ("movq $0, %rax");
                __asm__ volatile ("movq (%rax), %rax");
            }
            else if (mode == "illegal instruction")
            {
                __asm__ volatile ("ud2a");
            }
            else if (mode == "abort")
            {
                abort();
            }
            else if (mode == "std::terminate")
            {
                std::terminate();
            }
            else if (mode == "use after free")
            {
                int * x_ptr;
                {
                    auto x = std::make_unique<int>();
                    x_ptr = x.get();
                }
                *x_ptr = 1;
                (void)x_ptr;
            }
            else if (mode == "use after scope")
            {
                volatile int * x_ptr;
                [&]{
                    volatile int x = 0;
                    x_ptr = &x;
                    (void)x;
                }();
                [&]{
                    volatile int y = 1;
                    *x_ptr = 2;
                    (void)y;
                }();
                (void)x_ptr;
            }
            else if (mode == "uninitialized memory")
            {
                int x;
                (void)write(2, &x, sizeof(x));
            }
            else if (mode == "data race")
            {
                int x = 0;
                std::thread t1([&]{ ++x; });
                std::thread t2([&]{ ++x; });
                t1.join();
                t2.join();
            }
            else if (mode == "throw exception")
            {
                std::vector<int>().at(0);
            }
            else if (mode == "access context")
            {
                (void)context->getCurrentQueryId();
            }
            else if (mode == "stack overflow")
            {
                executeImpl(block, result_type, input_rows_count);
            }
            else if (mode == "harmful function")
            {
                double res = drand48();
                (void)res;
            }
            else if (mode == "mmap many")
            {
                std::vector<void *> maps;
                SCOPE_EXIT(
                {
                    //for (void * map : maps)
                    //    munmap(map, 4096);
                });

                while (true)
                {
                    void * hint = reinterpret_cast<void *>(
                        std::uniform_int_distribution<intptr_t>(0x100000000000UL, 0x700000000000UL)(thread_local_rng));
                    void * map = mmap(hint, 4096, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
                    if (MAP_FAILED == map)
                        throw ErrnoException(ErrorCodes::CANNOT_ALLOCATE_MEMORY, "Allocator: Cannot mmap");
                    maps.push_back(map);
                }
            }
            else if (mode == "dlopen")
            {
                void * handle = dlopen("libc.so.6", RTLD_NOW);
                if (!handle)
                    throw Exception(ErrorCodes::CANNOT_DLOPEN, "Cannot dlopen: ({})", dlerror()); // NOLINT(concurrency-mt-unsafe) // MT-Safe on Linux, see man dlerror
            }
            else if (mode == "logical error")
            {
                throw Exception(ErrorCodes::LOGICAL_ERROR, "Trap");
            }
            else
                throw Exception(ErrorCodes::BAD_ARGUMENTS, "Unknown trap mode");
        }
        else
            throw Exception(ErrorCodes::ILLEGAL_COLUMN, "The only argument for function {} must be constant String", getName());

        return result_type->createColumnConst(input_rows_count, 0ULL);
    }
};


REGISTER_FUNCTION(Trap)
{
    factory.registerFunction<FunctionTrap>();
}

}

#endif
