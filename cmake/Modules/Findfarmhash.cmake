# - Try to find farmhash headers and libraries.
#
# Usage of this module as follows:
#
#     find_package(farmhash)
#
# Variables used by this module, they can change the default behaviour and need
# to be set before calling find_package:
#
#  FARMHASH_ROOT_DIR Set this variable to the root installation of
#                    farmhash if the module has problems finding
#                    the proper installation path.
#
# Variables defined by this module:
#
#  FARMHASH_FOUND             System has farmhash libs/headers
#  FARMHASH_LIBRARIES         The farmhash library/libraries
#  FARMHASH_INCLUDE_DIR       The location of farmhash headers

find_path(FARMHASH_ROOT_DIR
    NAMES include/farmhash.h
)

find_library(FARMHASH_LIBRARIES
    NAMES farmhash
    PATHS ${FARMHASH_ROOT_DIR}/lib ${FARMHASH_LIBRARIES_PATHS}
)

find_path(FARMHASH_INCLUDE_DIR
    NAMES farmhash.h
    PATHS ${FARMHASH_ROOT_DIR}/include ${FARMHASH_INCLUDE_PATHS}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(farmhash DEFAULT_MSG
    FARMHASH_LIBRARIES
    FARMHASH_INCLUDE_DIR
)

mark_as_advanced(
    FARMHASH_ROOT_DIR
    FARMHASH_LIBRARIES
    FARMHASH_INCLUDE_DIR
)
