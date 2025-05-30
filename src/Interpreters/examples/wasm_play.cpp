#include <iostream>
#include <memory>
#include <random>
#include <utility>

#include <fmt/chrono.h>
#include <fmt/printf.h>

#include <Poco/AutoPtr.h>
#include <Poco/ConsoleChannel.h>


#include <AggregateFunctions/registerAggregateFunctions.h>
#include <Columns/ColumnsNumber.h>
#include <DataTypes/DataTypesNumber.h>
#include <Formats/formatBlock.h>
#include <Formats/registerFormats.h>
#include <Functions/registerFunctions.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/WriteBufferFromOStream.h>
#include <IO/WriteBufferFromString.h>
#include <IO/copyData.h>
#include <Interpreters/Context.h>
#include <Common/CurrentThread.h>
#include <Common/MemoryTracker.h>
#include <Common/ThreadStatus.h>
#include <Common/formatReadable.h>
#include <Common/logger_useful.h>

#include <Functions/UserDefined/UserDefinedWebAssembly.h>
#include <Common/tests/gtest_global_context.h>

#include <Interpreters/WasmModuleManager.h>
#include <Interpreters/WebAssembly/HostApi.h>
#include <Interpreters/WebAssembly/WasmEdgeRuntime.h>
#include <Interpreters/WebAssembly/WasmTimeRuntime.h>
#include <Interpreters/WebAssembly/WasmEngine.h>

using namespace DB;
using namespace DB::WebAssembly;

struct CmdArgs
{
    std::string wasm_file;
    std::string function_name;
    std::string data_size;

    uint32_t verbose = 0;
    uint32_t pause = 0;
};

std::unordered_map<std::string_view, std::string CmdArgs::*> string_arg_map = {
    {"--wasm-file", &CmdArgs::wasm_file},
    {"--function-name", &CmdArgs::function_name},
    {"--size", &CmdArgs::data_size},
};

std::unordered_map<std::string_view, uint32_t CmdArgs::*> flag_arg_map = {
    {"-v", &CmdArgs::verbose},
    {"-p", &CmdArgs::pause},
};

CmdArgs parseCmdArgs(int argc, const char ** argv)
{
    CmdArgs args;
    for (int i = 1; i < argc; ++i)
    {
        std::string_view arg = argv[i];

        if (string_arg_map.contains(arg))
        {
            if (i + 1 >= argc)
                throw std::runtime_error("Missing value for argument: " + std::string(arg));
            args.*(string_arg_map.at(arg)) = argv[++i];
        }
        else if (flag_arg_map.contains(arg))
        {
            args.*(flag_arg_map.at(arg)) += 1;
        }
        else if (arg.size() > 2 && arg[0] == '-' && arg[1] != '-')
        {
            for (size_t j = 1; j < arg.size(); ++j)
            {
                std::string short_flag = std::string("-") + arg[j];
                if (flag_arg_map.contains(short_flag))
                    args.*(flag_arg_map.at(short_flag)) += 1;
                else
                    throw std::runtime_error("Unknown argument: " + short_flag);
            }
        }
        else
        {
            throw std::runtime_error("Unknown argument: " + std::string(arg));
        }
    }

    if (args.wasm_file.empty())
        throw std::runtime_error("Missing value for argument: --wasm-file");
    if (args.function_name.empty())
        args.function_name = "some_func";

    return args;
}


void printMemoryUsage(const char * file, int line)
{
    fmt::print("{}:{}: Memory usage: {}\n", file, line, ReadableSize(total_memory_tracker.get()));
}


void pauseForInput(bool pause)
{
    if (!pause)
        return;
    fmt::print("Press any key to continue\n");
    std::cin.get();
}


DB::ContextMutablePtr & getTestContext()
{
    static ContextHolder holder;
    static struct Register
    {
        Register() { DB::registerFormats(); }
    } registered;
    return holder.context;
}


void formatBlockToBuffer(const String & format_name, WriteBuffer & buf, const Block & block)
{
    auto context = getTestContext();
    if (!context)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Global context not initialized");
    auto out = context->getOutputFormat(format_name, buf, block.cloneEmpty());
    formatBlock(out, block);
}

void printBlock(const Block & block, const String & format_name = "PrettyCompactMonoBlock")
{
    WriteBufferFromOStream cout_buf(std::cout);
    formatBlockToBuffer(format_name, cout_buf, block);
    cout_buf.finalize();
}

template <typename T>
DB::ColumnVector<T>::Ptr getRandomColumn(size_t size, T min, T max)
{
    std::random_device rd;
    std::mt19937 gen(rd());
    auto column = DB::ColumnVector<T>::create(size);
    auto & data = column->getData();
    for (size_t i = 0; i < size; ++i)
        data[i] = std::uniform_int_distribution<T>(min, max)(gen);
    return column;
}


int main(int argc [[maybe_unused]], const char ** argv [[maybe_unused]])
try
{
    ThreadStatus thread_status;

    Poco::AutoPtr<Poco::ConsoleChannel> channel(new Poco::ConsoleChannel(std::cerr));
    Poco::Logger::root().setChannel(channel);
    Poco::Logger::root().setLevel("trace");

    auto args = parseCmdArgs(argc, argv);

    WasmEdgeRuntime::setLogLevel(args.verbose >= 2 ? LogsLevel::trace : args.verbose == 1 ? LogsLevel::error : LogsLevel::none);

    printMemoryUsage(__FILE__, __LINE__);

    // std::unique_ptr<IWasmEngine> wasm_engine = std::make_unique<WasmEdgeRuntime>();
    std::unique_ptr<IWasmEngine> wasm_engine = std::make_unique<WasmTimeRuntime>();



    std::shared_ptr<WasmModule> wasm_module;
    {
        ReadBufferFromFile file(args.wasm_file);
        WriteBufferFromOwnString wasm_code_buffer;
        copyData(file, wasm_code_buffer);
        wasm_code_buffer.finalize();
        wasm_module = wasm_engine->createModule(wasm_code_buffer.stringView());
        WasmModuleManager::addImportsTo(*wasm_module);
    }

    WasmAbiVersion abi_ver = WasmAbiVersion::Plain;
    if (args.function_name.starts_with("vector_"))
        abi_ver = WasmAbiVersion::V1;
    auto wasm_func = UserDefinedWebAssemblyFunction::create(
        wasm_module,
        args.function_name,
        {"arg1", "arg2"},
        {std::make_shared<DataTypeUInt64>(), std::make_shared<DataTypeUInt64>()},
        std::make_shared<DataTypeUInt64>(),
        abi_ver,
        {});

    {
        Block block;
        size_t data_size = args.data_size.empty() ? 8 : std::stoul(args.data_size);
        block.insert(DB::ColumnWithTypeAndName(getRandomColumn<UInt64>(data_size, 1, 99), std::make_shared<DB::DataTypeUInt64>(), "x"));
        block.insert(DB::ColumnWithTypeAndName(getRandomColumn<UInt64>(data_size, 1, 99), std::make_shared<DB::DataTypeUInt64>(), "y"));

        auto wasm_inst = wasm_func->getModule()->instantiate({});

        auto context = getTestContext();
        Stopwatch watch;
        auto result_column = wasm_func->executeOnBlock(wasm_inst.get(), block, context, block.rows());
        watch.stop();
        fmt::print("Elapsed: {} ms, {:.2f} row/sec\n", watch.elapsedMilliseconds(), data_size / watch.elapsedSeconds());
        block.insert(ColumnWithTypeAndName{std::move(result_column), std::make_shared<DataTypeUInt64>(), "result"});

        printBlock(block);
    }

    pauseForInput(args.pause > 1);
    printMemoryUsage(__FILE__, __LINE__);

    return 0;
}
catch (...)
{
    std::cerr << getCurrentExceptionMessage(true) << std::endl;
    return 1;
}
