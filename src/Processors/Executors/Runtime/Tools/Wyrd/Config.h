#pragma once

#if !defined(EXECUTOR_SIM) && defined(DEBUG_OR_SANITIZER_BUILD)
#    define EXECUTOR_FAULT_INJECTION 1
#endif

#if !defined(EXECUTOR_FAULT_BEFORE) && !defined(EXECUTOR_FAULT_AFTER)
#    define EXECUTOR_FAULT_BEFORE 1
#endif
