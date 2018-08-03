# - Try to find murmurhash2 headers and libraries.
#
# Usage of this module as follows:
#
#     find_package(murmurhash2)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  MURMURHASH2_ROOT_DIR Set this variable to the root installation of
#                    murmurhash2 if the module has problems finding
#                    the proper installation path.
#
# Variables defined by this module:
#
#  MURMURHASH2_FOUND             System has murmurhash2 libs/headers
#  MURMURHASH2_LIBRARIES         The murmurhash2 library/libraries
#  MURMURHASH2_INCLUDE_DIR       The location of murmurhash2 headers

find_path(MURMURHASH2_ROOT_DIR
    NAMES include/murmurhash2.h
)

find_library(MURMURHASH2_LIBRARIES
    NAMES murmurhash2
    PATHS ${MURMURHASH2_ROOT_DIR}/lib ${MURMURHASH2_LIBRARIES_PATHS}
)

find_path(MURMURHASH2_INCLUDE_DIR
    NAMES murmurhash2.h
    PATHS ${MURMURHASH2_ROOT_DIR}/include ${MURMURHASH2_INCLUDE_PATHS}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(murmurhash2 DEFAULT_MSG
    MURMURHASH2_LIBRARIES
    MURMURHASH2_INCLUDE_DIR
)

mark_as_advanced(
    MURMURHASH2_ROOT_DIR
    MURMURHASH2_LIBRARIES
    MURMURHASH2_INCLUDE_DIR
)
