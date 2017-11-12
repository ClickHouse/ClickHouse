//===- tools/lld/lld.cpp - Linker Driver Dispatcher -----------------------===//
//
//                             The LLVM Linker
//
// This file is distributed under the University of Illinois Open Source
// License. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//
//
// This is the entry point to the lld driver. This is a thin wrapper which
// dispatches to the given platform specific driver.
//
// If there is -flavor option, it is dispatched according to the arguments.
// If the flavor parameter is not present, then it is dispatched according
// to argv[0].
//
//===----------------------------------------------------------------------===//

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

int mainEntryClickHouseLLD(int Argc, char **Argv) {
  // Standard set up, so program fails gracefully.
  sys::PrintStackTraceOnErrorSignal(Argv[0]);
  PrettyStackTraceProgram StackPrinter(Argc, Argv);
  llvm_shutdown_obj Shutdown;

  std::vector<const char *> Args(Argv, Argv + Argc);
  return !elf::link(Args, true);
}
