#include <Interpreters/WasmModuleManager.h>
#include <Interpreters/WebAssembly/HostApi.h>
#include <Interpreters/WebAssembly/WasmTimeRuntime.h>
#include <Interpreters/WebAssembly/WasmEdgeRuntime.h>

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
#include <Common/UniqueLock.h>
#include <Common/SharedLockGuard.h>

#include <ranges>
#include <expected>

namespace DB
{

using WebAssembly::WasmModule;
using WebAssembly::WasmTimeRuntime;
using WebAssembly::WasmEdgeRuntime;

namespace ErrorCodes
{
    extern const int RESOURCE_NOT_FOUND;
    extern const int FILE_ALREADY_EXISTS;
    extern const int CANNOT_DROP_FUNCTION;
    extern const int SUPPORT_IS_DISABLED;
    extern const int INCORRECT_DATA;
    extern const int UNKNOWN_ELEMENT_IN_CONFIG;
}

constexpr auto FILE_EXTENSION = ".wasm";

template <typename... Args>
auto formatUnexpected(FormatStringHelper<Args...> fmt, Args && ...args)
{
    return std::unexpected(PreformattedMessage::create(std::move(fmt), std::forward<Args>(args)...));
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

UInt256 calculateHash(std::string_view data [[maybe_unused]])
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


std::expected<void, PreformattedMessage> checkValidWasmCode(std::string_view name, std::string_view wasm_code)
{
    if (name.empty() || 128 < name.size() || !std::all_of(name.data(), name.data() + name.size(), isWordCharASCII))
    {
        return formatUnexpected(
            "Name of a WebAssembly module must be a non-empty string of length at most 128 consisting of word characters only, got '{}'",
            trimAndEscape(name));
    }

    /// Detect magic number for WebAssembly module
    /// Reference: https://webassembly.github.io/spec/core/binary/modules.html#binary-module
    constexpr std::array<uint8_t, 8> wasm_magic_number = {0x00, 0x61, 0x73, 0x6D, 0x01, 0x00, 0x00, 0x00};
    if (!std::ranges::equal(wasm_magic_number, std::views::take(wasm_code, wasm_magic_number.size())))
    {
        return formatUnexpected(
            "Cannot read magic number for WebAssembly module '{}', binary module version 0x1 is expected, got '{}'",
            name, trimAndEscape(wasm_code, 16));
    }

    return {};
}

std::expected<String, PreformattedMessage> validateModuleFile(const DiskPtr & user_scripts_disk, const String & path)
{
    std::filesystem::path file_path(path);

    const auto & file_name = file_path.filename().string();

    if (!endsWith(file_name, FILE_EXTENSION))
        return formatUnexpected("Unexpected file extension '{}', expected '{}'", file_path.extension().string(), FILE_EXTENSION);

    ReadSettings read_settings;
    auto read_buf = user_scripts_disk->readFile(path, read_settings);
    std::string file_header(16, '\0');
    size_t n = read_buf->read(file_header.data(), file_header.size());
    if (n != file_header.size())
        return formatUnexpected("File '{}' is too small to be a valid WebAssembly module", path);

    auto module_name = file_name.substr(0, file_name.size() - strlen(FILE_EXTENSION));
    if (auto res = checkValidWasmCode(module_name, file_header); !res)
        return std::unexpected(std::move(res.error()));
    return module_name;
}

static std::unique_ptr<WebAssembly::IWasmEngine> createEngine(std::string_view engine_name)
{
    if (engine_name == "wasmtime")
        return std::make_unique<WasmTimeRuntime>();
    if (engine_name == "wasmedge")
        return std::make_unique<WasmEdgeRuntime>();
    throw Exception(ErrorCodes::UNKNOWN_ELEMENT_IN_CONFIG,
        "Unknown WebAssembly engine '{}', available engines: 'wasmtime', 'wasmedge'",
        engine_name);
}

WasmModuleManager::WasmModuleManager(DiskPtr user_scripts_disk_, std::filesystem::path user_scripts_path_, std::string_view engine_name)
    : user_scripts_disk(std::move(user_scripts_disk_))
    , user_scripts_path(std::move(user_scripts_path_))
    , engine(createEngine(engine_name))
{
    user_scripts_disk->createDirectories(user_scripts_path);
    registerExistingModules();
}

WasmModuleManager::~WasmModuleManager() = default;

void WasmModuleManager::saveModule(std::string_view module_name, std::string_view wasm_code, UInt256 expected_hash)
{
    if (auto res = checkValidWasmCode(module_name, wasm_code); !res)
        throw Exception(std::move(res.error()), ErrorCodes::INCORRECT_DATA);

    UInt256 actual_hash = calculateHash(wasm_code);
    if (expected_hash && actual_hash != expected_hash)
        throw Exception(ErrorCodes::INCORRECT_DATA,
            "Hash mismatch for WebAssembly module '{}', expected {}, got {}",
            module_name, hashToHex(expected_hash), hashToHex(actual_hash));

    {
        UniqueLock lock(modules_mutex);
        auto [existing_module, inserted] = modules.insert({std::string(module_name), ModuleRef{std::weak_ptr<WasmModule>{}, actual_hash}});
        if (!inserted)
        {
            UInt256 existing_hash = existing_module->second.hash;
            if (!existing_hash)
            {
                existing_hash = calculateHash(loadModuleImpl(module_name));
                existing_module->second.hash = existing_hash;
            }
            if (!actual_hash)
                actual_hash = calculateHash(wasm_code);

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
        UniqueLock lock(modules_mutex);
        if (auto it = modules.find(module_name); it != modules.end())
            modules.erase(it);
    });

    auto out_buf = user_scripts_disk->writeFile(getFilePath(module_name), DBMS_DEFAULT_BUFFER_SIZE, WriteMode::Rewrite, {});
    out_buf->write(wasm_code.data(), wasm_code.size());
    out_buf->finalize();
    is_written = true;
}


void linkHostFunctions(WasmModule & module)
{
    for (const auto & declaration : module.getImports())
    {
        auto host_func = WebAssembly::getHostFunction(declaration.getName());
        checkFunctionDeclarationMatches(declaration, host_func.getFunctionDeclaration());
        module.linkFunction(std::move(host_func));
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
        SharedLockGuard lock(modules_mutex);

        auto it = modules.find(module_name);
        if (it == modules.end())
        {
            auto module_path = getFilePath(module_name);
            if (!user_scripts_disk->existsFile(module_path))
                throw Exception(ErrorCodes::RESOURCE_NOT_FOUND, "WebAssembly module '{}' not found", module_name);
            if (auto res = validateModuleFile(user_scripts_disk, module_path); !res)
                throw Exception(ErrorCodes::INCORRECT_DATA, "Cannot load WebAssembly module '{}': {}", module_name, res.error().text);
            /// If file exists on disk but is not registered yet, we proceed to load it below
        } else if (auto module = it->second.ptr.lock())
            return {module, it->second.hash};
    }

    UniqueLock write_lock(modules_mutex);

    auto wasm_code = loadModuleImpl(module_name);
    std::shared_ptr<WasmModule> module = engine->compileModule(wasm_code);
    UInt256 module_hash = calculateHash(wasm_code);

    modules[std::string(module_name)] = {module, module_hash};
    linkHostFunctions(*module);

    return {module, module_hash};
}

void WasmModuleManager::deleteModuleIfExists(std::string_view module_name)
{
    UniqueLock lock(modules_mutex);
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
    UniqueLock lock(modules_mutex);
    LOG_DEBUG(log, "Loading WASM modules from '{}/{}' at disk '{}'", user_scripts_disk->getPath(), user_scripts_path, user_scripts_disk->getName());

    auto files_it = user_scripts_disk->iterateDirectory(user_scripts_path);
    for (; files_it->isValid(); files_it->next())
    {
        const auto & file_path = files_it->path();
        const auto & file_name = files_it->name();
        auto res = validateModuleFile(user_scripts_disk, file_path);
        if (!res)
        {
            LOG_DEBUG(log, "Ignoring file '{}' which is not a valid WASM module: {}", file_path, res.error().text);
            continue;
        }

        auto [_, inserted] = modules.insert({res.value(), {}});
        if (!inserted)
        {
            LOG_DEBUG(log, "Ignoring file '{}' with duplicate module name", file_path);
            continue;
        }
    }

    LOG_DEBUG(log, "Found {} WASM modules", modules.size());
}

std::string WasmModuleManager::getFilePath(std::string_view module_name) const
{
    return user_scripts_path / fmt::format("{}{}", module_name, FILE_EXTENSION);
}

std::vector<std::pair<std::string, UInt256>> WasmModuleManager::getModulesList() const
{
    SharedLockGuard lock(modules_mutex);

    std::vector<std::pair<std::string, UInt256>> result;
    result.reserve(modules.size());
    for (const auto & [name, module] : modules)
        result.push_back({name, module.hash});
    return result;
}

}
