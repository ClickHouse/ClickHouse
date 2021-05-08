#if defined(OS_LINUX)

#include "DiskStatisticsOS.h"

#include <sys/statvfs.h>

#include <Core/Types.h>

#include <IO/ReadBuffer.h>
#include <IO/ReadHelpers.h>

namespace DB 
{

namespace ErrorCodes 
{
    extern const int CANNOT_STATVFS;
}

static constexpr auto mounts_filename = "/proc/mounts";

static constexpr std::size_t READ_BUFFER_BUF_SIZE = (64 << 10);

void readStringUntilWhitespaceAndSkipWhitespaceIfAny(String & s, ReadBuffer & buf) 
{
    readStringUntilWhitespace(s, buf);
    skipWhitespaceIfAny(buf);
}

DiskStatisticsOS::DiskStatisticsOS()
    : mounts_in(mounts_filename, READ_BUFFER_BUF_SIZE, O_RDONLY | O_CLOEXEC)
{}

DiskStatisticsOS::~DiskStatisticsOS() {}

DiskStatisticsOS::Data DiskStatisticsOS::get() 
{
    mounts_in.seek(0, SEEK_SET);

    DiskStatisticsOS::Data data = {0, 0};

    while (!mounts_in.eof()) 
    {
        String filesystem = readNextFilesystem();
        
        struct statvfs stat;

        if (statvfs(filesystem.c_str(), &stat)) 
            throwFromErrno("Cannot statvfs", ErrorCodes::CANNOT_STATVFS);
        
        uint64_t total_blocks = static_cast<uint64_t>(stat.f_blocks);
        uint64_t free_blocks  = static_cast<uint64_t>(stat.f_bfree);
        uint64_t used_blocks  = total_blocks - free_blocks; 
        uint64_t block_size   = static_cast<uint64_t>(stat.f_bsize);

        data.total += total_blocks * block_size;
        data.used  += used_blocks  * block_size;
    }

    return data;
}

String DiskStatisticsOS::readNextFilesystem() 
{   
    String filesystem, unused;

    readStringUntilWhitespaceAndSkipWhitespaceIfAny(unused, mounts_in);
    readStringUntilWhitespace(filesystem, mounts_in);
    skipToNextLineOrEOF(mounts_in);

    return filesystem;
}

}

#endif
