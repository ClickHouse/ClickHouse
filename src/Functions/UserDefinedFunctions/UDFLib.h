#ifndef CLICKHOUSE_UDFLIB_H
#define CLICKHOUSE_UDFLIB_H

#include <dlfcn.h>

#include <ext/scope_guard.h>

#include "UDFControlCommand.h"
#include "UDFAPI.h"

namespace DB
{

class UDFLib
{
public:
    explicit UDFLib(std::string_view fname) : filename(fname) { }

    UDFLib(UDFLib && other) : filename(other.filename), name(other.name), exec_funcs(std::move(other.exec_funcs)), get_return_type_funcs(std::move(other.get_return_type_funcs)), dl_handle(other.dl_handle)
    {
        other.dl_handle = nullptr;
    }

    ~UDFLib() { unload(); }

    UDFControlCommandResult load()
    {
        UDFControlCommandResult res;
        dl_handle = dlopen(filename.data(), RTLD_LAZY);
        if (!dl_handle)
        {
            res.code = 1;
            res.message = std::string("Can not load lib ") + filename;
            return res;
        }
        constexpr auto init_function_name = "UDFInit";
        UDFInit init_func = reinterpret_cast<UDFInit>(dlsym(dl_handle, init_function_name));
        if (init_func == nullptr)
        {
            res.code = 1;
            res.message = std::string("Can not find init function ") + init_function_name + " in file " + filename;
            unload();
            return res;
        }
        constexpr auto free_function_name = "UDFListFree";
        UDFListFree free_func = reinterpret_cast<UDFListFree>(dlsym(dl_handle, free_function_name));
        if (free_func == nullptr)
        {
            res.code = 1;
            res.message = std::string("Can not find init function ") + free_function_name + " in file " + filename;
            ;
            unload();
            return res;
        }
        UDFList info = init_func();
        auto guard = ext::make_scope_guard([=]() { free_func(info); });
        name = info.name;
        res.code = 0;
        res.message.clear();
        auto * ptr = info.functions;
        while (ptr != nullptr)
        {
            if (ptr->name == nullptr)
            {
                res.code = 1;
                res.message = "Null pointer for function name in lib " + name;
                unload();
                return res;
            }
            if (ptr->exec_impl == nullptr)
            {
                res.code = 1;
                res.message = "Null pointer for function " + std::string(ptr->name) + " in lib " + name;
                unload();
                return res;
            }
            exec_funcs[ptr->name] = ptr->exec_impl;
            get_return_type_funcs[ptr->name] = ptr->get_return_type_impl;

            /// Full function name is libname + '_' + funcname
            res.message.append(name);
            res.message.push_back('_');
            res.message.append(ptr->name);
            res.message.push_back(' ');
            ++ptr;
        }
        return res;
    }

    void unload()
    {
        if (dl_handle == nullptr)
        {
            return;
        }
        exec_funcs.clear();
        get_return_type_funcs.clear();
        dlclose(dl_handle); /// @TODO Igr Errors ignored
        dl_handle = nullptr;
    }

    bool isInited() { return dl_handle != nullptr; }

    std::string_view getName() const { return name; }

    UDFControlCommandResult exec(std::string_view func_name)
    {
        if (!isInited())
        {
            auto res = load();
            if (!res.isSuccess())
                return res;
        }
        UDFControlCommandResult res;
        auto func = exec_funcs.find(std::string(func_name));
        if (func == exec_funcs.end())
        {
            res.code = 1;
            res.message = std::string("No such function ") + std::string(func_name);
            return res;
        }
        func->second(nullptr, nullptr); /// @TODO Igr args
        res.code = 0;
        return res;
    }

    UDFControlCommandResult get_return_type(std::string_view func_name)
    {
        if (!isInited())
        {
            auto res = load();
            if (!res.isSuccess())
                return res;
        }
        UDFControlCommandResult res;
        auto func = get_return_type_funcs.find(std::string(func_name));
        if (func == get_return_type_funcs.end())
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
    std::unordered_map<std::string, ExecImpl> exec_funcs;
    std::unordered_map<std::string, GetReturnTypeImpl> get_return_type_funcs;
    void * dl_handle = nullptr;
};

}

#endif //CLICKHOUSE_UDFLIB_H
