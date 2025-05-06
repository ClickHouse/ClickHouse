#pragma once

#include <xray/xray_interface.h>

#include <llvm/DebugInfo/Symbolize/Symbolize.h>
#include <llvm/Object/ObjectFile.h>
#include <llvm/Support/Error.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/XRay/InstrumentationMap.h>

#include <dlfcn.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace DB
{

static uint64_t getRelocationOffset()
{
    // Get info about the executable
    Dl_info info;
    dladdr(reinterpret_cast<void *>(&getRelocationOffset), &info);

    // Calculate the offset between the loaded address and the expected address
    uint64_t load_addr = reinterpret_cast<uint64_t>(info.dli_fbase);

    // On some systems, you need to subtract the base address of the executable
    // from the addresses in the instrumentation map
    return load_addr;
}

class XRayFunctionMapper
{
public:
    explicit XRayFunctionMapper()
        : executable_path(getSelfPath())
    {
        loadInstrumentationMap();
        buildFunctionNameMaps();
    }

    void listAllFunctions() const
    {
        LOG_DEBUG(&Poco::Logger::get("debug"), "Available instrumented functions:");
        for (const auto & [name, id] : func_addr_to_id)
            LOG_DEBUG(&Poco::Logger::get("debug"), "ID: {} - Function: {}", id, name);
    }

    bool patchFunction(uint64_t func_addr) const
    {
        int32_t func_id = getFunctionId(func_addr);
        if (func_id == -1)
        {
            LOG_DEBUG(&Poco::Logger::get("debug"), "Function '{}' not found in instrumentation map", func_addr);
            return false;
        }

        auto status = __xray_patch_function(func_id);
        return (status == XRayPatchingStatus::SUCCESS);
    }

    bool unpatchFunction(uint64_t func_addr) const
    {
        int32_t func_id = getFunctionId(func_addr);
        if (func_id == -1)
        {
            LOG_DEBUG(&Poco::Logger::get("debug"), "Function '{}' not found in instrumentation map", func_addr);
            return false;
        }

        auto status = __xray_unpatch_function(func_id);
        return (status == XRayPatchingStatus::SUCCESS);
    }

    static XRayFunctionMapper & getXrayFunctionMapper()
    {
        static XRayFunctionMapper instance;
        return instance;
    }

private:
    std::string getSelfPath()
    {
        char result[PATH_MAX];
        ssize_t count = readlink("/proc/self/exe", result, PATH_MAX);
        return std::string(result, (count > 0) ? count : 0);
    }

    bool loadInstrumentationMap()
    {
        auto error_or_instr_map = llvm::xray::loadInstrumentationMap(executable_path);
        if (auto err = error_or_instr_map.takeError())
        {
            LOG_DEBUG(&Poco::Logger::get("debug"), "Error loading instrumentation map: {}", llvm::toString(std::move(err)));
            return false;
        }

        instr_map = std::move(error_or_instr_map.get());
        return true;
    }

    void buildFunctionNameMaps()
    {
        for (const auto & [func_id, func_addr] : instr_map.getFunctionAddresses())
            func_addr_to_id[func_addr + getRelocationOffset()] = func_id;
    }

    int32_t getFunctionId(uint64_t func_addr) const
    {
        auto it = func_addr_to_id.find(func_addr);
        return it != func_addr_to_id.end() ? it->second : -1;
    }

    std::string executable_path;
    llvm::xray::InstrumentationMap instr_map;
    std::unordered_map<uint64_t, int32_t> func_addr_to_id;
};

struct TraceScope
{
    explicit TraceScope(uint64_t func_addr_)
        : function_name(func_addr_)
        , mapper(XRayFunctionMapper::getXrayFunctionMapper())
    {
        if (!mapper.patchFunction(function_name))
            LOG_DEBUG(&Poco::Logger::get("debug"), "Failed to patch function: {}", function_name);
    }

    ~TraceScope()
    {
        // if (!mapper.unpatchFunction(function_name))
        //     LOG_DEBUG(&Poco::Logger::get("debug"), "Failed to unpatch function: {}", function_name);
    }

private:
    uint64_t function_name;
    const XRayFunctionMapper & mapper;
};

template <typename... Args>
struct Sig;

template <typename Ret, typename... Args>
struct Sig<Ret(Args...)>
{
    using type = Ret (*)(Args...);
};

template <typename Ret, typename... Args>
struct Sig<Ret (*)(Args...)>
{
    using type = Ret (*)(Args...);
};

template <typename Class, typename Ret, typename... Args>
struct Sig<Ret (Class::*)(Args...)>
{
    using type = Ret (Class::*)(Args...);
};

template <typename Class, typename Ret, typename... Args>
struct Sig<Ret (Class::*)(Args...) const>
{
    using type = Ret (Class::*)(Args...) const;
};

template <typename Ptr>
int get_vtable_index(Ptr func)
{
    unsigned char bytes[sizeof(func)];
    std::memcpy(bytes, &func, sizeof(func));

    // The first byte (in little-endian systems) is the vtable index
    return static_cast<int>(bytes[0]);
}

template <typename Class, typename Ptr>
uint64_t get_virtual_address(const Class * instance, Ptr func)
{
    const auto vtable_index = get_vtable_index(func);
    const auto true_index = vtable_index / sizeof(void *);
    LOG_DEBUG(&Poco::Logger::get("debug"), "vtable_index={}, true_index={}", vtable_index, true_index);

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wundefined-reinterpret-cast"

    const void * const * vtable = *reinterpret_cast<const void * const * const *>(instance);
    const void * function_ptr = vtable[true_index];

#pragma clang diagnostic pop

    return reinterpret_cast<uint64_t>(function_ptr);
}

#define OMG(foo) \
    using FuncType = Sig<decltype(&(foo))>::type; \
    FuncType func_ptr = reinterpret_cast<FuncType>(&(foo)); \
    uint64_t func_addr = reinterpret_cast<uint64_t>(func_ptr); \
    TraceScope scope(func_addr);

#define OMG_MEMBER(class_name, member_func) \
    using FuncType = Sig<decltype(&class_name::member_func)>::type; \
    union \
    { \
        FuncType ptr; \
        uint64_t value; \
    } converter; \
    converter.ptr = &class_name::member_func; \
    uint64_t func_addr = converter.value; \
    TraceScope scope(func_addr);

#define OMG_VIRT_MEMBER(class_name, member_func) \
    using FuncType = Sig<decltype(&class_name::member_func)>::type; \
    FuncType func_ptr = &class_name::member_func; \
    uint64_t func_addr = get_virtual_address(this, func_ptr); \
    TraceScope scope(func_addr);
}
