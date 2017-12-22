
function(generate_function_register FUNCTION_AREA)

  foreach(FUNCTION IN LISTS ARGN)
    configure_file (registerFunction.h.in register${FUNCTION}.h)
    configure_file (registerFunction.cpp.in register${FUNCTION}.cpp)
    set(REGISTER_HEADERS "${REGISTER_HEADERS} #include \"register${FUNCTION}.h\"\n")
    set(REGISTER_FUNCTIONS "${REGISTER_FUNCTIONS} register${FUNCTION}(factory);\n")
  endforeach()

  configure_file (registerFunctions_area.cpp.in registerFunctions${FUNCTION_AREA}.cpp)

endfunction()
