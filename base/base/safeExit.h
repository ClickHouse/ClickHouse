#pragma once

/// _exit() with a workaround for TSan.
/// Pass run_leak_check = false when other threads are still running: LSan stops the world and
/// classifies chunks those threads still own, so it reports leaks it cannot attribute.
[[noreturn]] void safeExit(int code, bool run_leak_check = true);
