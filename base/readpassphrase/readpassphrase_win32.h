/* Decisions of the Windows `readpassphrase` implementation that are worth checking on their own.
 *
 * The prompt path itself needs a real console, which no CI job has: the only place a Windows
 * binary runs is the Wine step of `Build (amd_windows)`. Factoring the two decisions out keeps
 * them reachable from `clickhouse-windows-selftest` - the same approach as
 * `windowsJobObjectMemoryLimit` in `base/base/getMemoryAmount.h`. */

#ifndef READPASSPHRASE_WIN32_H
#define READPASSPHRASE_WIN32_H

#ifdef __cplusplus
extern "C" {
#endif

/* Non-zero when the prompt has to be written to a console output handle instead of `stderr`:
 * the passphrase is typed on the console, and `stderr` is not that console. */
int readpassphrase_prompt_needs_console_output(int input_is_console, int stderr_is_console);

/* The console mode to set while the passphrase is read, given the mode the console had and the
 * `RPP_*` flags. Line and processed input are always on; echo follows `RPP_ECHO_ON`. */
unsigned long readpassphrase_console_mode(unsigned long original_mode, int flags);

#ifdef __cplusplus
}
#endif

#endif
