#include "lld/Common/Driver.h"
#include "llvm/Support/InitLLVM.h"
#include <vector>

int mainEntryClickHouseLLD(int argc, char ** argv)
{
    llvm::InitLLVM X(argc, argv);
    std::vector<const char *> args(argv, argv + argc);
    return !lld::elf::link(args, false);
}
