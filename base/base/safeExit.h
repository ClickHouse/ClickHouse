#pragma once

/// _exit() with a workaround for TSan.
[[noreturn]] void safeExit(int code);
