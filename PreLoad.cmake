# Use Ninja instead of Unix Makefiles by default.

find_program(NINJA_PATH ninja)
if (NINJA_PATH)
    set(CMAKE_GENERATOR "Ninja")
endif ()
