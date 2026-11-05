#include <Parsers/SecretArguments.h>
#include <Storages/ColumnsDescription.h>

#include <iostream>

/// No engine is registered, so the secrets of their arguments are shown.
[[maybe_unused]] static const bool secret_arguments_finder_installed
    = (DB::setSecretArgumentsFinder(&DB::NoSecretArgumentsFinder::instance()), true);

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        using namespace DB;
        ColumnsDescription columns = ColumnsDescription::parse(std::string(reinterpret_cast<const char *>(data), size));
        std::cerr << columns.toString(true) << "\n";
    }
    catch (...)
    {
        // Ok
    }

    return 0;
}
