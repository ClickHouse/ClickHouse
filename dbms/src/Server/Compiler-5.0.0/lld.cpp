#include "lld/Common/Driver.h"
#include "llvm/ADT/STLExtras.h"
#include "llvm/ADT/StringSwitch.h"
#include "llvm/ADT/Twine.h"
#include "llvm/Support/ManagedStatic.h"
#include "llvm/Support/Path.h"
#include "llvm/Support/PrettyStackTrace.h"
#include "llvm/Support/Signals.h"

using namespace lld;
using namespace llvm;
using namespace llvm::sys;

int mainEntryClickHouseLLD(int Argc, char **Argv)
{
  // Standard set up, so program fails gracefully.
  sys::PrintStackTraceOnErrorSignal(Argv[0]);
  PrettyStackTraceProgram StackPrinter(Argc, Argv);
  llvm_shutdown_obj Shutdown;

  std::vector<const char *> Args(Argv, Argv + Argc);
  return !elf::link(Args, true);
}
