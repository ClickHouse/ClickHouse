
# wget https://raw.githubusercontent.com/openssh/openssh-portable/master/openbsd-compat/readpassphrase.c
# wget https://raw.githubusercontent.com/openssh/openssh-portable/master/openbsd-compat/readpassphrase.h

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-result -Wno-reserved-id-macro")

configure_file(includes.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/includes.h)
add_library(readpassphrase ${CMAKE_CURRENT_SOURCE_DIR}/readpassphrase.c)
set_target_properties(readpassphrase
                      PROPERTIES LINKER_LANGUAGE C
                      )
# . to allow #include <readpassphrase.h>
target_include_directories(readpassphrase PUBLIC . ${CMAKE_CURRENT_BINARY_DIR}/include)
