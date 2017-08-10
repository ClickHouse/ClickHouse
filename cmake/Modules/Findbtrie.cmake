# - Try to find btrie headers and libraries.
#
# Usage of this module as follows:
#
#     find_package(btrie)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  BTRIE_ROOT_DIR Set this variable to the root installation of
#                    btrie if the module has problems finding
#                    the proper installation path.
#
# Variables defined by this module:
#
#  BTRIE_FOUND             System has btrie libs/headers
#  BTRIE_LIBRARIES         The btrie library/libraries
#  BTRIE_INCLUDE_DIR       The location of btrie headers

find_path(BTRIE_ROOT_DIR
    NAMES include/btrie.h
)

find_library(BTRIE_LIBRARIES
    NAMES btrie
    PATHS ${BTRIE_ROOT_DIR}/lib ${BTRIE_LIBRARIES_PATHS}
)

find_path(BTRIE_INCLUDE_DIR
    NAMES btrie.h
    PATHS ${BTRIE_ROOT_DIR}/include ${BTRIE_INCLUDE_PATHS}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(btrie DEFAULT_MSG
    BTRIE_LIBRARIES
    BTRIE_INCLUDE_DIR
)

mark_as_advanced(
    BTRIE_ROOT_DIR
    BTRIE_LIBRARIES
    BTRIE_INCLUDE_DIR
)
