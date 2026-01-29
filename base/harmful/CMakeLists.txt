add_library(harmful harmful.c)
target_include_directories(harmful PRIVATE ..)

# Propagate fuzzing configuration
if(ENABLE_FUZZING)
    target_compile_definitions(harmful PRIVATE USE_FUZZING_MODE=1)
endif()
