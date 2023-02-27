#include <unistd.h>
#include <iostream>
#include <Common/Exception.h>
#include <Common/filesystemHelpers.h>
#include <filesystem>

namespace fs = std::filesystem;

namespace DB
{
    namespace ErrorCodes
    {
        extern const int SYSTEM_ERROR;
    }
}

int main(int, char **)
try
{
    fs::path dir("./test_dir/");
    fs::create_directories(dir);
    FS::createFile("./test_dir/file");

    if (0 != symlink("./test_dir", "./test_link"))
        DB::throwFromErrnoWithPath("Cannot create symlink", "./test_link", DB::ErrorCodes::SYSTEM_ERROR);

    fs::rename("./test_link", "./test_link2");
    fs::remove_all("./test_link2");
    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(false) << "\n";
    return 1;
}
