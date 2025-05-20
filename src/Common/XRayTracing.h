#pragma once

#include <xray/xray_interface.h>

#include <llvm/DebugInfo/Symbolize/Symbolize.h>
#include <llvm/Object/ObjectFile.h>
#include <llvm/Support/Error.h>
#include <llvm/Support/raw_ostream.h>
#include <llvm/XRay/InstrumentationMap.h>

#include <fmt/ranges.h>

#include <dlfcn.h>

#include <Poco/Logger.h>
#include <Common/logger_useful.h>

namespace XRayTracing
{

// On some systems, you need to subtract the base address of the executable
// from the addresses in the instrumentation map
static uint64_t getRelocationOffset()
{
    // Get info about the executable
    Dl_info info;
    dladdr(reinterpret_cast<void *>(&getRelocationOffset), &info);

    // Calculate the offset between the loaded address and the expected address
    return reinterpret_cast<uint64_t>(info.dli_fbase);
}

class XRayFunctionMapper
{
    using FuncId = int32_t;

    std::string getSelfPath()
    {
        char result[PATH_MAX];
        ssize_t count = readlink("/proc/self/exe", result, PATH_MAX);
        return std::string(result, (count > 0) ? count : 0);
    }

public:
    explicit XRayFunctionMapper()
        : executable_path(getSelfPath())
    {
        loadInstrumentationMap();
        buildFunctionNameMaps();
    }

    void listAllFunctions() const
    {
        LOG_DEBUG(&Poco::Logger::get("debug"), "Available instrumented functions: \n{}", fmt::join(func_addr_to_id, ","));
    }

    bool patchFunction(uint64_t func_addr) const
    {
        if (const auto func_id = getFunctionId(func_addr))
            return __xray_patch_function(*func_id) == XRayPatchingStatus::SUCCESS;
        LOG_DEBUG(&Poco::Logger::get("debug"), "Function '{}' not found in instrumentation map", func_addr);
        return false;
    }

    bool unpatchFunction(uint64_t func_addr) const
    {
        if (const auto func_id = getFunctionId(func_addr))
            return __xray_unpatch_function(*func_id) == XRayPatchingStatus::SUCCESS;
        LOG_DEBUG(&Poco::Logger::get("debug"), "Function '{}' not found in instrumentation map", func_addr);
        return false;
    }

    static XRayFunctionMapper & getXRayFunctionMapper()
    {
        static XRayFunctionMapper instance;
        return instance;
    }

private:
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
        static const auto relocation_offset = getRelocationOffset();
        for (const auto & [func_id, func_addr] : instr_map.getFunctionAddresses())
            func_addr_to_id[func_addr + relocation_offset] = func_id;
    }

    std::optional<FuncId> getFunctionId(uint64_t func_addr) const
    {
        const auto it = func_addr_to_id.find(func_addr);
        return it != func_addr_to_id.end() ? std::make_optional(it->second) : std::nullopt;
    }

    std::string executable_path;
    llvm::xray::InstrumentationMap instr_map;
    std::unordered_map<uint64_t, FuncId> func_addr_to_id;
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
unsigned char get_vtable_index(Ptr func)
{
    unsigned char bytes[sizeof(func)];
    std::memcpy(bytes, &func, sizeof(func));

    // The first byte (in little-endian systems) is the vtable index
    return static_cast<unsigned char>(bytes[0]);
}

template <typename Class, typename Ptr>
uint64_t get_virtual_address(const Class * instance, Ptr func)
{
    const auto vtable_offset = get_vtable_index(func);
    const auto vtable_index = vtable_offset / sizeof(void *);

    const void * const * vtable = *std::bit_cast<const void * const * const *>(instance);
    const void * function_ptr = vtable[vtable_index];
    return reinterpret_cast<uint64_t>(function_ptr);
}

}

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-macros"

#define OMG(foo) \
    using namespace XRayTracing; \
    using FuncType = Sig<decltype(&(foo))>::type; \
    const auto func_ptr = reinterpret_cast<FuncType>(&(foo)); \
    const auto func_addr = reinterpret_cast<uint64_t>(func_ptr); \
    XRayFunctionMapper::getXRayFunctionMapper().patchFunction(func_addr);

#define OMG_MEMBER(class_name, member_func) \
    using namespace XRayTracing; \
    using FuncType = Sig<decltype(&class_name::member_func)>::type; \
    union \
    { \
        const FuncType ptr; \
        const uint64_t func_addr; \
    } converter{.ptr = &class_name::member_func}; \
    static constexpr uint64_t REAL_FUNC_ADDR_THRESHOLD = 0x00400000; \
    const auto func_addr \
        = converter.func_addr >= REAL_FUNC_ADDR_THRESHOLD ? converter.func_addr : get_virtual_address(this, converter.ptr); \
    XRayFunctionMapper::getXRayFunctionMapper().patchFunction(func_addr);

#pragma clang diagnostic pop
