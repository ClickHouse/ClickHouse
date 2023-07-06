#include <Common/SymbolIndex.h>
#include <IO/WriteBufferFromFileDescriptor.h>

int mainEntryClickHouseDumpSymbols(int, char **)
{
    DB::WriteBufferFromFileDescriptor out(STDOUT_FILENO);
    DB::SymbolIndex::instance()->writeSymbols(out);
    return 0;
}
