# Build change that weakens security hardening

Fires when the diff removes, downgrades, or opts code out of a hardening measure: stack protector, `-fstack-clash-protection`, `_FORTIFY_SOURCE`, PIE (`-fPIE`/`-pie`), RELRO/`-z,now`, `-z,noexecstack`, `-mbranch-protection`/`-fcf-protection`, CFI (`-fsanitize=cfi*`), `_LIBCPP_HARDENING_MODE`, and similar.

**Forms of an opt-out**
- **Global flag:** removed, downgraded (`-strong` to plain, `=none`, `=0`), or moved behind a condition that some release build does not meet.
- **Per target, file, or function:** `-fno-*` or `=none` in a contrib's CMake, `__attribute__((no_sanitize(...)))`, `target("branch-protection=none")`, `#pragma` attributes, sanitizer ignorelist entries.
- **Flag allowlist:** a CMake that rebuilds its flags from an allowlist or resets `CMAKE_*_FLAGS` silently drops an inherited hardening flag. Check every new or changed `contrib/*-cmake` for this.
- **Submodule bump:** its commit range can add any of the above. The submodule diff is not in the PR diff, so read the linked upstream PR or commits before judging the bump.

**Justification**
Removing hardening is a last resort. The PR description must state (a) the concrete failure the hardening causes (a `SIGILL` on some CPUs, a link error, a measured cost), (b) its root cause, and (c) why the root cause cannot be fixed instead. Verify each claim. Root causes that usually have a fix: code compiled for a newer ISA than the flag tolerates (lower the target features instead of disabling the flag), mismatched attributes that block inlining (make both sides agree), a hand-written assembly stub (add the protection to the stub).

**Scope**
A justified exclusion must be the narrowest possible (one function or file, not a whole library or build type) and carry a one-line comment saying why.

**Severity**
An unexplained removal or opt-out in a release build is a **Blocker**. An explained one whose root cause has an evident fix, or whose scope is wider than needed, is a **Major**.
