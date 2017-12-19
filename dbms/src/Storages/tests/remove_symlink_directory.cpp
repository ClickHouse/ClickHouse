#include <unistd.h>
#include <iostream>

#include <Poco/File.h>
#include <Poco/Path.h>
#include <Common/Exception.h>


int main(int, char **)
try
{
    Poco::File dir("./test_dir/");
    dir.createDirectories();

    Poco::File("./test_dir/file").createFile();

    if (0 != symlink("./test_dir", "./test_link"))
        DB::throwFromErrno("Cannot create symlink");

    Poco::File link("./test_link");
    link.renameTo("./test_link2");

    Poco::File("./test_link2").remove(true);
    return 0;
}
catch (...)
{
    std::cerr << DB::getCurrentExceptionMessage(false) << "\n";
    return 1;
}
