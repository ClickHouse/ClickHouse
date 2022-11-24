#include "ODBCBridge.h"

#pragma GCC diagnostic ignored "-Wmissing-declarations"
int mainEntryClickHouseODBCBridge(int argc, char ** argv)
{
    DB::ODBCBridge app;
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
