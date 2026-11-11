#include <Core/NamesAndTypes.h>
#include <IO/ReadBufferFromMemory.h>
#include <Parsers/SecretArguments.h>

/// No engine is registered, so the secrets of their arguments are shown.
[[maybe_unused]] static const bool secret_arguments_finder_installed
    = (DB::setSecretArgumentsFinder(&DB::NoSecretArgumentsFinder::instance()), true);

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size);

extern "C" int LLVMFuzzerTestOneInput(const uint8_t * data, size_t size)
{
    try
    {
        DB::ReadBufferFromMemory in(data, size);
        DB::NamesAndTypesList res;
        res.readText(in);
    }
    catch (...)
    {
        // Ok
    }

    return 0;
}
