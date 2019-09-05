##TODO replace hardcode to find procedure

set(USE_ORC 0)
set(USE_INTERNAL_ORC_LIBRARY ON)

if (ARROW_LIBRARY)
    set(USE_ORC 1)
endif()


find_path(CYRUS_SASL_INCLUDE_DIR sasl/sasl.h)
find_library(CYRUS_SASL_SHARED_LIB sasl2)
if (NOT CYRUS_SASL_INCLUDE_DIR OR NOT CYRUS_SASL_SHARED_LIB)
    set(USE_ORC 0)
endif()
