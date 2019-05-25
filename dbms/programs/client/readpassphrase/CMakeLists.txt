
# wget https://raw.githubusercontent.com/openssh/openssh-portable/master/openbsd-compat/readpassphrase.c
# wget https://raw.githubusercontent.com/openssh/openssh-portable/master/openbsd-compat/readpassphrase.h

configure_file(includes.h.in ${CMAKE_CURRENT_BINARY_DIR}/include/includes.h)
add_library(readpassphrase ${CMAKE_CURRENT_SOURCE_DIR}/readpassphrase.c)
# . to allow #include <readpassphrase.h>
target_include_directories(readpassphrase PUBLIC . PRIVATE ${CMAKE_CURRENT_BINARY_DIR}/include)
