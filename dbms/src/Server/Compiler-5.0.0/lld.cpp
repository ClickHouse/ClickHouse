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

#include "lld/Driver/Driver.h"
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

enum Flavor {
  Invalid,
  Gnu,     // -flavor gnu
  WinLink, // -flavor link
  Darwin,  // -flavor darwin
};

LLVM_ATTRIBUTE_NORETURN static void die(const Twine &S) {
  errs() << S << "\n";
  exit(1);
}

#if 0

static Flavor getFlavor(StringRef S) {
  return StringSwitch<Flavor>(S)
      .CasesLower("ld", "ld.lld", "gnu", Gnu)
      .CaseLower("link", WinLink)
      .CaseLower("darwin", Darwin)
      .Default(Invalid);
}

static Flavor parseProgname(StringRef Progname) {
#if __APPLE__
  // Use Darwin driver for "ld" on Darwin.
  if (Progname == "ld")
    return Darwin;
#endif

#if LLVM_ON_UNIX
  // Use GNU driver for "ld" on other Unix-like system.
  if (Progname == "ld")
    return Gnu;
#endif

  // Progname may be something like "lld-gnu". Parse it.
  SmallVector<StringRef, 3> V;
  Progname.split(V, "-");
  for (StringRef S : V)
    if (Flavor F = getFlavor(S))
      return F;
  return Invalid;
}

static Flavor parseFlavor(std::vector<const char *> &V) {
  // Parse -flavor option.
  if (V.size() > 1 && V[1] == StringRef("-flavor")) {
    if (V.size() <= 2)
      die("missing arg value for '-flavor'");
    Flavor F = getFlavor(V[2]);
    if (F == Invalid)
      die("Unknown flavor: " + StringRef(V[2]));
    V.erase(V.begin() + 1, V.begin() + 3);
    return F;
  }

  // Deduct the flavor from argv[0].
  StringRef Arg0 = path::filename(V[0]);
  if (Arg0.endswith_lower(".exe"))
    Arg0 = Arg0.drop_back(4);
  return parseProgname(Arg0);
}

#endif

/// Universal linker main(). This linker emulates the gnu, darwin, or
/// windows linker based on the argv[0] or -flavor option.
int mainEntryClickHouseLLD(int Argc, char **Argv) {
  // Standard set up, so program fails gracefully.
  sys::PrintStackTraceOnErrorSignal(Argv[0]);
  PrettyStackTraceProgram StackPrinter(Argc, Argv);
  llvm_shutdown_obj Shutdown;

  std::vector<const char *> Args(Argv, Argv + Argc);
#if 0
  switch (parseFlavor(Args)) {
  case Gnu:
#endif
    return !elf::link(Args, true);
#if 0
  case WinLink:
    return !coff::link(Args);
  case Darwin:
    return !mach_o::link(Args);
  default:
    die("lld is a generic driver.\n"
        "Invoke ld.lld (Unix), ld (macOS) or lld-link (Windows) instead.");
  }
#endif
}
