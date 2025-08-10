#pragma once

#include <memory>
#include <map>
#include <filesystem>
#include <shared_mutex>

#include <Common/Logger.h>
#include <Disks/IDisk.h>

namespace DB
{

namespace WebAssembly
{
    class WasmModule;
    class IWasmEngine;
}

/** WasmModuleManager manages a collection of WasmModules storing them on disk, loading and basic validation.
  * It serves as the entry point for interacting with WebAssembly modules within the rest of the system.
  */
class WasmModuleManager
{
public:
    WasmModuleManager(DiskPtr user_scripts_disk_, std::filesystem::path user_scripts_path_, std::string_view engine_name);

    void saveModule(std::string_view module_name, std::string_view wasm_code, UInt256 expected_hash = {});

    using ModulePtr = std::shared_ptr<WebAssembly::WasmModule>;
    std::pair<ModulePtr, UInt256> getModule(std::string_view module_name);
    std::vector<std::pair<std::string, UInt256>> getModulesList() const;

    static void addImportsTo(WebAssembly::WasmModule & module);

    void deleteModuleIfExists(std::string_view module_name);

    WasmModuleManager(const WasmModuleManager &) = delete;
    WasmModuleManager & operator=(const WasmModuleManager &) = delete;
    WasmModuleManager(WasmModuleManager &&) = delete;
    WasmModuleManager & operator=(WasmModuleManager &&) = delete;

    ~WasmModuleManager();

protected:
    std::string loadModuleImpl(std::string_view module_name);
    std::string getFilePath(std::string_view module_name) const;
    void registerExistingModules();


    DiskPtr user_scripts_disk;
    std::filesystem::path user_scripts_path;

    mutable std::shared_mutex modules_mutex;

    struct ModuleRef
    {
        std::weak_ptr<WebAssembly::WasmModule> ptr;
        UInt256 hash;
    };

    std::map<std::string, ModuleRef, std::less<>> modules;

    std::unique_ptr<WebAssembly::IWasmEngine> engine;

    LoggerPtr log = getLogger("WasmModuleManager");
};


}
