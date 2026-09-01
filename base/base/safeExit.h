#pragma once

/// What to do about the LeakSanitizer check before exiting.
enum class LeakCheck
{
    /// Run it. The default: a caller that exits with no other thread running.
    Run,
    /// Skip it, and say so on stderr. For callers that exit while other threads still run: LSan
    /// stops the world and classifies chunks those threads still own, so it reports leaks it
    /// cannot attribute.
    SkipAndReport,
    /// Skip it silently. As above, but for callers that do not redirect stderr, where a notice
    /// would land in program output and be read as a test failure.
    SkipQuietly,
};

/// _exit() with a workaround for TSan.
[[noreturn]] void safeExit(int code, LeakCheck leak_check = LeakCheck::Run);
