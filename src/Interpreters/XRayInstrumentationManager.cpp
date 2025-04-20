#include "XRayInstrumentationManager.h"
#include <filesystem>
#include <stdexcept>
#include <thread>

namespace DB
{

using namespace llvm;
using namespace llvm::object;
using namespace llvm::xray;
using namespace llvm::symbolize;

void XRayInstrumentationManager::registerHandler(const std::string & name, XRayHandlerFunction handler)
{
    std::lock_guard<std::mutex> lock(mutex);
    xrayHandlerNameToFunction[name] = handler;
}


XRayInstrumentationManager::XRayInstrumentationManager()
{
    // maybe would be better to map not handler names but smth else(smth that would be more convenient to use in system statement)
    registerHandler("logEntry", &logEntry);
    registerHandler("logAndSleep", &logAndSleep);
    registerHandler("logEntryExit", &logEntryExit);
    parseXRayInstrumentationMap();
}

XRayInstrumentationManager & XRayInstrumentationManager::instance()
{
    static XRayInstrumentationManager instance;
    return instance;
}


void XRayInstrumentationManager::setHandlerAndPatch(const std::string & function_name, const std::string & handler_name)
{
    std::lock_guard lock(mutex);
    auto handler_function = xrayHandlerNameToFunction[handler_name]; // if not in a map?
    auto function_id = functionNameToXRayID[function_name]; // if not in a map?
    instrumented_functions.emplace_front(function_id, function_name, handler_name);
    functionIdToInstrumentPoint[function_id] = instrumented_functions.begin();
    __xray_set_handler(handler_function);
    __xray_patch(function_id);
    // add/update a row "functionId | functionName | handlerName" to system.instrument
}


void XRayInstrumentationManager::unpatchFunction(const std::string & function_name)
{
    std::lock_guard lock(mutex);
    auto function_id = functionNameToXRayID[function_name]; // if not in a map?
    instrumented_functions.erase(functionIdToInstrumentPoint[function_id]);
    functionIdToInstrumentPoint.erase(function_id);
    __xray_unpatch(function_id);
    // delete a row "functionId | functionName | handlerName" in system.instrument by functionId
}


XRayHandlerFunction XRayInstrumentationManager::getHandler(const std::string & name) const
{
    std::lock_guard lock(mutex);
    auto it = xrayHandlerNameToFunction.find(name);
    if (it == xrayHandlerNameToFunction.end())
        throw std::runtime_error("Handler not found: " + name);
    return it->second;
}


//Takes path to the elf-binary file(that should contain xray_instr_map section),
// and gets mapping of functionIDs to the addresses, then resolves IDs into human-readable names
void XRayInstrumentationManager::parseXRayInstrumentationMap()
{
    auto binary_path = std::filesystem::canonical(std::filesystem::path("/proc/self/exe")).string(); // TODO: fix

    // Load the XRay instrumentation map from the binary
    auto instr_map_or_error = loadInstrumentationMap(binary_path);
    if (!instr_map_or_error)
    {
        // errs() << "Failed to load instrumentation map: " << toString(instr_map_or_error.takeError()) << "\n"; // TODO: fix
        errs() << toString(instr_map_or_error.takeError()) << "\n";
    }
    auto &instr_map = *instr_map_or_error;

    // Retrieve the mapping of function IDs to addresses
    auto function_addresses = instr_map.getFunctionAddresses();

    // Initialize the LLVM symbolizer to resolve function names
    LLVMSymbolizer symbolizer;


    // Iterate over all instrumented functions
    for (const auto &[FuncID, Addr] : function_addresses)
    {
        // Create a SectionedAddress structure to hold the function address
        object::SectionedAddress ModuleAddress;
        ModuleAddress.Address = Addr;
        ModuleAddress.SectionIndex = object::SectionedAddress::UndefSection;

        // Default function name if symbolization fails
        std::string FunctionName = "<unknown>";

        // Attempt to symbolize the function address (resolve its name)
        if (auto ResOrErr = symbolizer.symbolizeCode(binary_path, ModuleAddress))
        {
            auto &DI = *ResOrErr;
            if (DI.FunctionName != DILineInfo::BadString)
                FunctionName = DI.FunctionName;
        }

        // map function ID to its resolved name and vice versa
        if (FunctionName != "<unknown>")
        {
            functionNameToXRayID[FunctionName] = FuncID;
            xrayIdToFunctionName[FuncID] = FunctionName;
        }
    }
}

[[clang::xray_never_instrument]] void XRayInstrumentationManager::logEntry(int32_t FuncId, XRayEntryType Type)
{
    static thread_local bool in_hook = false;
    if (in_hook || Type != XRayEntryType::ENTRY)
    {
        return;
    }
    in_hook = true;
    {
        std::lock_guard<std::mutex> lock(log_mutex);
        printf("[logEntry] Entered Function ID %d\n", FuncId);
    }
    in_hook = false;
}

[[clang::xray_never_instrument]] void XRayInstrumentationManager::logAndSleep(int32_t FuncId, XRayEntryType Type)
{
    static thread_local bool in_hook = false;
    if (in_hook || Type != XRayEntryType::ENTRY)
    {
        return;
    }

    in_hook = true;
    {
        std::lock_guard<std::mutex> lock(log_mutex);
        printf("[logAndSleep] Function ID %d entered. Sleeping...\n", FuncId);
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
    in_hook = false;
}

[[clang::xray_never_instrument]] void XRayInstrumentationManager::logEntryExit(int32_t FuncId, XRayEntryType Type)
{
    static thread_local bool in_hook = false;
    if (in_hook)
    {
        return;
    }

    in_hook = true;
    {
        std::lock_guard<std::mutex> lock(log_mutex);
        if (Type == XRayEntryType::ENTRY)
        {
            printf("[logEntryExit] Entering Function ID %d\n", FuncId);
        }
        else if (Type == XRayEntryType::EXIT)
        {
            printf("[logEntryExit] Exiting Function ID %d\n", FuncId);
        }
    }
    in_hook = false;
}


}
