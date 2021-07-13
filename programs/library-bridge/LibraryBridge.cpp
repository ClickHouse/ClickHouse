#include "LibraryBridge.h"

#pragma GCC diagnostic ignored "-Wmissing-declarations"
int mainEntryClickHouseLibraryBridge(int argc, char ** argv)
{
    DB::LibraryBridge app;
    try
    {
        return app.run(argc, argv);
    }
    catch (...)
    {
        std::cerr << DB::getCurrentExceptionMessage(true) << "\n";
        auto code = DB::getCurrentExceptionCode();
        return code ? code : 1;
    }
}
