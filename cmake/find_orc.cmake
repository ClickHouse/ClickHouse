##TODO replace hardcode to find procedure

set(USE_ORC 0)
set(USE_INTERNAL_ORC_LIBRARY ON)

if (ARROW_LIBRARY)
    set(USE_ORC 1)
endif()