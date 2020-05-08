#include <dlfcn.h>

#include "UDFManager.h"
#include <Functions/IFunctionImpl.h>
#include <DataTypes/DataTypeFactory.h>
#include <ext/scope_guard.h>

#include <sstream>
#include <Functions/UserDefinedFunctions/UDFAPI.h>
#include <Functions/FunctionFactory.h>


std::ostream& operator<<(std::ostream &out, const DB::UDFManager::UDFControlCommand &command) {
    out << command.request_id << ' ';
    out << command.name << ' ';
    for (auto && arg : command.args) {
        out << arg << ' ';
    }
    out << '\n';
    return out;
}

std::istream& operator>>(std::istream &in, DB::UDFManager::UDFControlCommand &command) {
    in >> command.request_id;
    if (in.peek() == ' ') {
        in.ignore();
    }
    std::string line;
    std::getline(in, line);
    auto it = line.find(' ');
    command.name = line.substr(0, it);
    if (it == std::string::npos) {
        command.args.clear();
    } else {
        command.args = line.substr(it + 1);
    }
    return in;
}

std::ostream& operator<<(std::ostream &out, const DB::UDFManager::UDFControlCommandResult &result) {
    out << result.request_id << ' ';
    out << result.code << ' ' << result.message << '\n';
    return out;
}

std::istream& operator>>(std::istream &in, DB::UDFManager::UDFControlCommandResult &result) {
    in >> result.request_id;
    if (in.peek() == ' ') {
        in.ignore();
    }
    std::string line;
    std::getline(in, line);
    auto it = line.find(' ');
    result.code = strtoul(line.c_str(), nullptr, 10);
    if (it == std::string::npos) {
        result.message.clear();
    } else {
        result.message = line.substr(it + 1);
    }
    return in;
}

namespace DB
{
class FunctionUDF : public IFunction
{
public:
    explicit FunctionUDF(std::string name_, UDFManager &manager_) : name(name_), manager(manager_) {}

    std::string getName() const override { return name; }

    bool isVariadic() const override { return true; }
    size_t getNumberOfArguments() const override { return 0; }

    DataTypePtr getReturnTypeImpl(const DataTypes & /* arguments */) const override
    {
        DataTypeFactory::instance();
        throw Exception("Not implemented", ErrorCodes::NOT_IMPLEMENTED); /// @TODO Igr
    }

    bool useDefaultImplementationForConstants() const override { return true; }
    ColumnNumbers getArgumentsThatAreAlwaysConstant() const override { return {}; } /// @TODO Igr

    void executeImpl(Block & /* block */, const ColumnNumbers & /* arguments */, size_t /* result */, size_t /* input_rows_count */) override
    {
        manager.call(name); /// @TODO Igr
    }

private:
    std::string name;
    UDFManager &manager;
};

[[noreturn]] void UDFManager::run()
{
    UDFControlCommandResult result;
    /// Process results
    while (true)
    {
        inCommands >> result;
        std::unique_lock lock(mx);
        results[result.request_id] = result;
        waiters[result.request_id].notify_all();
    }
}

class UDFLib {
public:
    explicit UDFLib(std::string_view fname) : filename(fname) { }

    UDFLib(UDFLib &&other) : filename(other.filename), name(other.name), funcs(std::move(other.funcs)), dl_handle(other.dl_handle) {
        other.dl_handle = nullptr;
    }

    ~UDFLib() {
        unload();
    }

    UDFManager::UDFControlCommandResult load() {
        UDFManager::UDFControlCommandResult res;
        dl_handle = dlopen(filename.data(), RTLD_LAZY);
        if (!dl_handle)
        {
            res.code = 1;
            res.message = std::string("Can not load lib ") + filename;
            return res;
        }
        constexpr auto init_function_name = "UDFInit";
        UDFInit init_func = reinterpret_cast<UDFInit>(dlsym(dl_handle, init_function_name));
        if (init_func == nullptr) {
            res.code = 1;
            res.message = std::string("Can not find init function ") + init_function_name + " in file " + filename;
            unload();
            return res;
        }
        constexpr auto free_function_name = "UDFListFree";
        UDFListFree free_func = reinterpret_cast<UDFListFree>(dlsym(dl_handle, free_function_name));
        if (free_func == nullptr) {
            res.code = 1;
            res.message = std::string("Can not find init function ") + free_function_name + " in file " + filename;;
            unload();
            return res;
        }
        UDFList info = init_func();
        auto guard = ext::make_scope_guard([=](){ free_func(info); });
        name = info.name;
        res.code = 0;
        res.message.clear();
        auto *ptr = info.functions;
        while (ptr != nullptr) {
            if (ptr->name == nullptr) {
                res.code = 1;
                res.message = "Null pointer for function name in lib " + name;
                unload();
                return res;
            }
            if (ptr->exec_impl == nullptr) {
                res.code = 1;
                res.message = "Null pointer for function " + std::string(ptr->name) + " in lib " + name;
                unload();
                return res;
            }
            funcs[ptr->name] = ptr->exec_impl;
            res.message.append(ptr->name);
            res.message.push_back(' ');
            ++ptr;
        }
        return res;
    }

    void unload() {
        if (dl_handle == nullptr) {
            return;
        }
        funcs.clear();
        dlclose(dl_handle);  /// @TODO Igr Errors ignored
        dl_handle = nullptr;
    }

    bool isInited() {
        return dl_handle != nullptr;
    }

    std::string_view getName() const {
        return name;
    }

    UDFManager::UDFControlCommandResult exec(std::string_view func_name) {
        if (!isInited())
        {
            auto res = load();
            if (!res.isSuccess())
                return res;
        }
        UDFManager::UDFControlCommandResult res;
        auto func = funcs.find(std::string(func_name));
        if (func == funcs.end())
        {
            res.code = 1;
            res.message = std::string("No such function ") + std::string(func_name);
            return res;
        }
        func->second(nullptr); /// @TODO Igr args
        res.code = 0;
        return res;
    }

private:
    std::string filename;
    std::string name;
    std::unordered_map<std::string, ExecImpl> funcs;
    void * dl_handle = nullptr;
};


void UDFManager::load(std::string_view filename)
{
    std::unique_lock lock(mx);
    UDFControlCommand cmd;
    cmd.name = "InitLib";
    cmd.args = filename;
    cmd.request_id = last_id++;
    auto &cv = waiters[cmd.request_id];
    outCommands << cmd;
    while (results.find(cmd.request_id) == results.end()) {
        cv.wait(lock);
    }
    auto res = results[cmd.request_id];
    results.erase(cmd.request_id);
    waiters.erase(cmd.request_id);
    lock.unlock();

    if (!res.isSuccess()) {
        throw Exception(res.message, res.code);
    }

    std::string_view funcs(res.message);
    while (!funcs.empty()) {
        auto pos = funcs.find(' ');
        auto name = std::string(funcs.substr(0, pos));
        funcs = funcs.substr(pos + 1 ? pos != std::string::npos : pos);
        if (!name.empty()) {
            FunctionFactory::instance().registerUserDefinedFunction(name, [=, this](const Context &) {
                return std::make_unique<DefaultOverloadResolver>(std::make_unique<FunctionUDF>(name, *this));
            });
        }
    }
}

void UDFManager::call(std::string_view func_name)
{
    UDFControlCommand cmd;
    cmd.name = "ExecFunc";
    /// @TODO Igr send data
    cmd.args = "0x0 " + std::string(func_name);
    std::unique_lock lock(mx);
    cmd.request_id = last_id++;
    auto &cv = waiters[cmd.request_id];
    outCommands << cmd;
    while (results.find(cmd.request_id) == results.end()) {
        cv.wait(lock);
    }
    auto res = results[cmd.request_id];
    results.erase(cmd.request_id);
    waiters.erase(cmd.request_id);
    lock.unlock();

    if (!res.isSuccess()) {
        throw Exception(res.message, res.code);
    }

    /// @TODO Igr process result
}

void UDFManager::stop() {
    UDFControlCommand cmd{};
    cmd.name = "Stop";
    outCommands << cmd;
}

[[noreturn]] void UDFManager::runIsolated()
{
    UDFControlCommand command;
    UDFControlCommandResult result;
    std::unordered_map<std::string, UDFLib> libs;
    while (true)
    {
        result.message.clear();
        result.code = 0;
        inCommands >> command;
        if (command.name == "InitLib")
        {
            /// Arg is filename
            UDFLib lib(command.args);
            result = lib.load();
            if (result.isSuccess()) {
                std::string name = std::string(lib.getName());
                if (libs.find(command.args) != libs.end()) {
                    result.code = 1;
                    result.message = "Lib " + name + " already exists";
                }
                else
                {
                    libs.insert({std::move(name), std::move(lib)});
                }
            }
        }
        else if (command.name == "ExecFunc")
        {
            /// Arg is data_pointer + " " + library_name + "_" + function_name
            std::istringstream iss(std::move(command.args));
            void * data_pointer;
            std::string name;
            if (!(iss >> data_pointer >> name))
            {
                result.code = 1;
                result.message = "Wrong arguments";
            }
            else
            {
                auto pos = name.find('_');
                if (pos == std::string::npos)
                {
                    result.code = 1;
                    result.message = "Wrong function name";
                }
                else
                {
                    auto lib_name = name.substr(0, pos);
                    auto func_name = std::string_view(name).substr(pos + 1);
                    auto lib = libs.find(lib_name);
                    if (lib == libs.end()) {
                        result.code = 1;
                        result.message = "No such library " + lib_name;
                    }
                    else
                        result = lib->second.exec(func_name);
                }
            }

        }
        result.request_id = command.request_id;
        outCommands << result;
    }
}

//UDFManager & UDFManager::Instance()
//{
//    static UDFManager ret;
//    return ret;
//}
}
