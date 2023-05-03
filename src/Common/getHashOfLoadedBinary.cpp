#include <Common/getHashOfLoadedBinary.h>

#if defined(OS_LINUX)

#include <link.h>
#include <array>
#include <Common/hex.h>


static int callback(dl_phdr_info * info, size_t, void * data)
{
    SipHash & hash = *reinterpret_cast<SipHash*>(data);

    for (size_t header_index = 0; header_index < info->dlpi_phnum; ++header_index)
    {
        const auto & phdr = info->dlpi_phdr[header_index];

        if (phdr.p_type == PT_LOAD && (phdr.p_flags & PF_X))
        {
            hash.update(phdr.p_filesz);
            hash.update(reinterpret_cast<const char *>(info->dlpi_addr + phdr.p_vaddr), phdr.p_filesz);
        }
    }

    return 1;   /// Do not continue iterating.
}


SipHash getHashOfLoadedBinary()
{
    SipHash hash;
    dl_iterate_phdr(callback, &hash);
    return hash;
}


std::string getHashOfLoadedBinaryHex()
{
    SipHash hash = getHashOfLoadedBinary();
    std::array<UInt64, 2> checksum;
    hash.get128(checksum);
    return getHexUIntUppercase(checksum);
}

#else

SipHash getHashOfLoadedBinary()
{
    return {};
}


std::string getHashOfLoadedBinaryHex()
{
    return {};
}

#endif
