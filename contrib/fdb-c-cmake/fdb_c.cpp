#include "foundationdb/fdb_c.h"
#include <exception>
#include <fstream>
#include <stdexcept>
#include <string_view>
#include <dlfcn.h>
#include <unistd.h>
#include <Common/getResource.h>
#include <iostream>

namespace
{
class FDBLibrary
{
public:
    static FDBLibrary instance;

    FDBLibrary()
    {
        auto data = getResource(FDB_C_RESOURCE_NAME);
        if (data.empty())
        {
            std::cerr << "fdb is not loaded: libfdb_c.so is not embeded" << std::endl;
            return;
        }

        try
        {
            loadDLFromMemory(data);
            loadApis();
        }
        catch (std::exception & e)
        {
            std::cerr << "fdb is not loaded: " << e.what() << std::endl;
        }
    }

    ~FDBLibrary()
    {
        // FIXME: dlcose return is ignored
        if (handle)
            dlclose(handle);
    }

private:
    void * handle = nullptr;

    void loadDLFromMemory(const std::string_view & data);

    /// Load fdb apis from handle
    void loadApis();
};

FDBLibrary FDBLibrary::instance;

void fdb_unavailable()
{
    throw std::runtime_error("fdb api is unavailable");
}

void FDBLibrary::loadDLFromMemory(const std::string_view & data)
{
#ifdef __linux__
    char filename[] = "/tmp/libfdb_c.so.XXXXXXX";
    int fd = mkstemp(filename);
    if (fd == -1)
        throw std::runtime_error("fdb_c: failed to create temporary file");
    std::ofstream ofs(filename);
    ofs << data;
    ofs.close();

    handle = dlopen(filename, RTLD_LAZY);
    if (!handle)
        throw std::runtime_error(std::string("fdb_c: failed to dlopen ") + dlerror());

    unlink(filename);
#else
    throw std::runtime_error("Loading fdb_c library on non-linux system is not support");
#endif
}

void FDBLibrary::loadApis()
{
    if (!handle)
        throw std::runtime_error("dl is not opened");

    dlerror();
    void * symbol = nullptr;

#define M(name) \
    symbol = dlsym(handle, #name); \
    if (char * error = dlerror()) \
        throw std::runtime_error("failed to dlsym " #name); \
    else \
        name = reinterpret_cast<FDB_C_API_TYPE(name)>(symbol);
    APPLY_FDB_C_APIS(M)
#undef M
}
}

#define M(name) FDB_C_API_TYPE(name) name = reinterpret_cast<FDB_C_API_TYPE(name)>(&fdb_unavailable);
APPLY_FDB_C_APIS(M)
#undef M