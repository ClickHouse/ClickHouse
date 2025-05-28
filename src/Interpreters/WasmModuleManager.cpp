#include <Interpreters/WasmModuleManager.h>
#include <Interpreters/WebAssembly/HostApi.h>
#include <Interpreters/WebAssembly/WasmTimeRuntime.h>

#include <Interpreters/Context.h>

#include <Disks/DiskLocal.h>
#include <IO/Operators.h>
#include <IO/ReadBufferFromFile.h>
#include <IO/ReadBufferFromString.h>
#include <IO/WriteBufferFromString.h>
#include <IO/WriteHelpers.h>
#include <IO/copyData.h>

#include <Common/OpenSSLHelpers.h>
#include <Common/StringUtils.h>
#include <Common/escapeForFileName.h>
#include <Common/logger_useful.h>
#include <Common/scope_guard_safe.h>

namespace DB
{

using WebAssembly::WasmModule;
using WebAssembly::WasmTimeRuntime;

namespace ErrorCodes
{
extern const int RESOURCE_NOT_FOUND;
extern const int WASM_ERROR;
extern const int FILE_ALREADY_EXISTS;
extern const int FUNCTION_ALREADY_EXISTS;
extern const int CANNOT_DROP_FUNCTION;
extern const int TYPE_MISMATCH;
extern const int SUPPORT_IS_DISABLED;
extern const int INCORRECT_DATA;
}

constexpr auto FILE_EXTENSION = ".wasm";

template <typename ResultType, typename... Args>
ResultType onError(int error_code [[maybe_unused]], FormatStringHelper<Args...> fmt [[maybe_unused]], Args &&... args [[maybe_unused]])
{
    if constexpr (std::is_same_v<ResultType, void>)
        throw Exception(error_code, std::move(fmt), std::forward<Args>(args)...);
    else
        return ResultType(false);
}

static String trimAndEscape(std::string_view name, size_t max_length = 128)
{
    if (name.size() > max_length)
    {
        name = name.substr(0, max_length);
        return escapeForFileName(std::string(name)) + "...";
    }
    return escapeForFileName(std::string(name));
}

UInt256 caclculateHash(std::string_view data [[maybe_unused]])
{
#if USE_SSL
    UInt256 hash;
    encodeSHA256(data, reinterpret_cast<unsigned char *>(hash.items));
    return hash;
#else
    throw Exception(ErrorCodes::SUPPORT_IS_DISABLED, "SHA256 hash support is disabled, because ClickHouse was built without SSL library");
#endif
}

std::string hashToHex(const UInt256 & hash)
{
    std::string hash_hex;
    hash_hex.resize(2 * sizeof(UInt256));

    const char * hash_data = reinterpret_cast<const char *>(hash.items);
    for (size_t i = 0; i < sizeof(UInt256); ++i)
        writeHexByteLowercase(hash_data[i], &hash_hex[2 * i]);
    return hash_hex;
}


template <typename ResultType>
ResultType checkValidWasmCode(std::string_view name, std::string_view wasm_code)
{
    if (name.size() < 1 || 128 < name.size() || !std::all_of(name.data(), name.data() + name.size(), isWordCharASCII))
    {
        return onError<ResultType>(
            ErrorCodes::INCORRECT_DATA,
            "Name of a WebAssembly module must be a non-empty string of length at most 128 consisting of word characters only, got '{}'",
            trimAndEscape(name));
    }

    /// Detect magic number for WebAssembly module
    /// Reference: https://webassembly.github.io/spec/core/binary/modules.html#binary-module
    constexpr std::array<uint8_t, 8> wasm_magic_number = {0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00};
    if (wasm_code.size() < wasm_magic_number.size() || !std::equal(wasm_magic_number.begin(), wasm_magic_number.end(), wasm_code.data()))
    {
        return onError<ResultType>(
            ErrorCodes::INCORRECT_DATA,
            "Cannot read magic number for WebAssembly module '{}', binary module version 0x1 is expected, got '{}'",
            name,
            trimAndEscape(wasm_code, 16));
    }

    return ResultType(true);
}

WasmModuleManager::WasmModuleManager(DiskPtr user_sciptrs_disk_, fs::path user_sciptrs_path_)
    : user_scripts_disk(std::move(user_sciptrs_disk_))
    , user_scripts_path(std::move(user_sciptrs_path_))
    , engine(std::make_unique<WasmTimeRuntime>())
{
    registerExistingModules();
}

WasmModuleManager::~WasmModuleManager() = default;

void WasmModuleManager::saveModule(std::string_view module_name, std::string_view wasm_code, UInt256 expected_hash)
{
    checkValidWasmCode<void>(module_name, wasm_code);

    UInt256 actual_hash = caclculateHash(wasm_code);
    if (expected_hash && actual_hash != expected_hash)
        throw Exception(
            ErrorCodes::INCORRECT_DATA,
            "Hash mismatch for WebAssembly module '{}', expected {}, got {}",
            module_name,
            hashToHex(expected_hash),
            hashToHex(actual_hash));

    {
        std::unique_lock lock(modules_mutex);
        auto [existing_module, inserted] = modules.insert({std::string(module_name), ModuleRef{std::weak_ptr<WasmModule>{}, actual_hash}});
        if (!inserted)
        {
            UInt256 existing_hash = existing_module->second.hash;
            if (!existing_hash)
            {
                existing_hash = caclculateHash(loadModuleImpl(module_name));
                existing_module->second.hash = existing_hash;
            }
            if (!actual_hash)
                actual_hash = caclculateHash(wasm_code);

            if (existing_hash == actual_hash)
            {
                LOG_DEBUG(log, "WebAssembly module '{}' with the same hash already exists, skipping saving", module_name);
                return;
            }
            throw Exception(ErrorCodes::FILE_ALREADY_EXISTS, "WebAssembly module '{}' already exists", module_name);
        }
    }

    bool is_written = false;
    SCOPE_EXIT_SAFE({
        if (is_written)
            return;
        std::unique_lock lock(modules_mutex);
        if (auto it = modules.find(module_name); it != modules.end())
            modules.erase(it);
    });

    auto out_buf = user_scripts_disk->writeFile(getFilePath(module_name), DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
    out_buf->write(wasm_code.data(), wasm_code.size());
    out_buf->finalize();
    is_written = true;
}


void WasmModuleManager::addImportsTo(WasmModule & module)
{
    for (const auto & declaration : module.getImports())
    {
        auto host_func = WebAssembly::getHostFunction(declaration->getName());
        if (!host_func)
            throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "WebAssembly host function '{}' not found", declaration->getName());
        checkFunctionDeclarationMatches(*declaration, *host_func);
        module.addImport(std::move(host_func));
    }
}

std::string WasmModuleManager::loadModuleImpl(std::string_view module_name)
{
    LOG_DEBUG(log, "Loading WebAssembly module '{}' from disk", module_name);
    ReadSettings read_settings;
    auto in_buf = user_scripts_disk->readFile(getFilePath(module_name), read_settings);
    WriteBufferFromOwnString wasm_code_buffer;
    copyData(*in_buf, wasm_code_buffer);
    wasm_code_buffer.finalize();
    auto wasm_code = std::move(wasm_code_buffer.str());
    return wasm_code;
}

std::pair<std::shared_ptr<WasmModule>, UInt256> WasmModuleManager::getModule(std::string_view module_name)
{
    {
        std::shared_lock lock(modules_mutex);

        auto it = modules.find(module_name);
        if (it == modules.end())
            throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "WebAssembly module '{}' not found", module_name);

        if (auto module = it->second.ptr.lock())
            return {module, it->second.hash};
    }

    std::unique_lock write_lock(modules_mutex);

    auto wasm_code = loadModuleImpl(module_name);
    std::shared_ptr<WasmModule> module = engine->createModule(wasm_code);
    UInt256 module_hash = caclculateHash(wasm_code);

    modules[std::string(module_name)] = {module, module_hash};
    addImportsTo(*module);

    return {module, module_hash};
}

void WasmModuleManager::deleteModuleIfExists(std::string_view module_name)
{
    std::unique_lock lock(modules_mutex);
    auto it = modules.find(module_name);
    if (it == modules.end())
        return;

    if (!it->second.ptr.expired())
        throw Exception(
            ErrorCodes::CANNOT_DROP_FUNCTION,
            "Cannot delete WebAssembly module '{}' while it is in use. "
            "Drop all functions referring to it first",
            module_name);

    modules.erase(it);
    user_scripts_disk->removeFileIfExists(getFilePath(module_name));
}

void WasmModuleManager::registerExistingModules()
{
    std::unique_lock lock(modules_mutex);
    LOG_DEBUG(
        log,
        "Loading WASM modules from '{}/{}' at disk '{}'",
        user_scripts_disk->getPath(),
        user_scripts_path,
        user_scripts_disk->getName());

    auto files_it = user_scripts_disk->iterateDirectory(user_scripts_path);
    for (; files_it->isValid(); files_it->next())
    {
        const auto & file_path = files_it->path();
        const auto & file_name = files_it->name();
        if (!endsWith(file_name, FILE_EXTENSION))
            continue;

        ReadSettings read_settings;
        auto read_buf = user_scripts_disk->readFile(file_path, read_settings);
        std::string file_header(16, '\0');
        size_t n = read_buf->read(&file_header[0], file_header.size());
        if (n != file_header.size())
        {
            LOG_DEBUG(log, "Ignoring file '{}' with illegal header", file_path);
            continue;
        }

        const auto module_name = file_name.substr(0, file_name.size() - strlen(FILE_EXTENSION));
        if (!checkValidWasmCode<bool>(module_name, file_header))
        {
            LOG_DEBUG(log, "Ignoring file '{}' with illegal header", file_path);
            continue;
        }

        auto [_, inserted] = modules.insert({std::string(module_name), {}});
        if (!inserted)
        {
            LOG_DEBUG(log, "Ignoring file '{}' with duplicate module name", file_path);
            continue;
        }
    }

    LOG_DEBUG(log, "Loaded {} WASM modules", modules.size());
}

std::string WasmModuleManager::getFilePath(std::string_view module_name) const
{
    return user_scripts_path / fmt::format("{}{}", module_name, FILE_EXTENSION);
}

std::vector<std::pair<std::string, UInt256>> WasmModuleManager::getModulesList() const
{
    std::shared_lock lock(modules_mutex);

    std::vector<std::pair<std::string, UInt256>> result;
    result.reserve(modules.size());
    for (const auto & [name, module] : modules)
        result.push_back({name, module.hash});
    return result;
}

}
